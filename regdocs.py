#!/usr/bin/env python3
"""CER REGDOCS unified pipeline.

Single entry point for the entire document processing pipeline:
  scout    — Crawl REGDOCS and discover documents (Producer)
  download — Download files for discovered documents (Worker)
  convert  — Convert downloaded files to Markdown (Processor)
  all      — Run the full pipeline: scout -> download -> convert
  stats    — Show pipeline status and metrics

All state lives in a single SQLite database (regdocs.db), which acts as the
system bus connecting every stage. No intermediate files, no log-grepping.
"""

import asyncio
import argparse
import hashlib
import json
import logging
import mimetypes
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from regdocs_db import get_db, DocumentStatus, TokenBucketLimiter

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

DOMAIN = "https://apps.cer-rec.gc.ca"
RESULTS_URL = f"{DOMAIN}/REGDOCS/Search/SearchAdvancedResults"
ADVANCED_URL = f"{DOMAIN}/REGDOCS/Search/Advanced"

PAGE_SIZES = (20, 50, 100, 200)
SORT_OLDEST_FIRST = 21

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
}

KNOWN_FACET_FIELDS = {
    "Document Type": "document_types",
    "Application Type": "application_types",
    "File Type": "file_types",
    "Role": "roles",
    "Commodity": "commodities",
}

CONTENT_TYPE_EXTENSIONS = {
    "application/pdf": ".pdf",
    "text/html": ".html",
    "text/plain": ".txt",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-powerpoint": ".ppt",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/zip": ".zip",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/tiff": ".tif",
}

CONVERTIBLE_EXTENSIONS = {".pdf", ".doc", ".docx", ".html", ".htm"}

HTML_SKIP_NOTE = (
    "HTML documents were skipped: CER's Content Server does not serve the images they "
    "reference, so the pages are incomplete. Use --include-html to download them anyway."
)

ITEM_HREF_RE = re.compile(r"/REGDOCS/(Item/View|File/Download)/(\d+)")
TOTAL_RE = re.compile(r"Item\(s\)\s*-\s*[\d,]+\s*to\s*[\d,]+\s*out of about\s*([\d,]+)")
FILING_RE = re.compile(r"Filing:\s*(\S+)")

try:
    import lxml  # noqa: F401
    SOUP_PARSER = "lxml"
except ImportError:
    SOUP_PARSER = "html.parser"


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def slugify(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def facet_field(category: str) -> str:
    return KNOWN_FACET_FIELDS.get(category, slugify(category).replace("-", "_"))


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("transformers").setLevel(logging.ERROR)
        logging.getLogger("transformers.modeling_utils").setLevel(logging.ERROR)
        import os
        os.environ["TRANSFORMERS_VERBOSITY"] = "error"
        import warnings
        warnings.filterwarnings("ignore", message=".*tied weights.*")


def validate_date(date_str: str) -> str:
    """Validate and normalize a YYYY-MM-DD date string.

    If the day overflows the month (e.g. 2026-06-31), it is clamped to the
    last valid day of that month (2026-06-30). This lets callers use
    "end of month" patterns like --end-date 2026-02-31 without crashing.
    """
    import calendar

    parts = date_str.split("-")
    if len(parts) != 3:
        raise ValueError(f"Invalid date: '{date_str}'. Expected YYYY-MM-DD.")

    try:
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        raise ValueError(f"Invalid date: '{date_str}'. Expected YYYY-MM-DD.")

    if month < 1 or month > 12:
        raise ValueError(f"Invalid date: '{date_str}'. Month must be 1-12.")

    # Clamp day to last valid day of the month
    max_day = calendar.monthrange(year, month)[1]
    if day > max_day:
        day = max_day
        clamped = f"{year:04d}-{month:02d}-{day:02d}"
        logging.info(f"Date '{date_str}' clamped to '{clamped}' (last day of month)")
        return clamped

    if day < 1:
        raise ValueError(f"Invalid date: '{date_str}'. Day must be >= 1.")

    return f"{year:04d}-{month:02d}-{day:02d}"


def extension_from_response(response: httpx.Response, url: str) -> Optional[str]:
    """Pick a file extension from response headers, falling back to the URL path."""
    disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', disposition, re.IGNORECASE)
    if match:
        ext = Path(match.group(1).strip()).suffix
        if ext:
            return ext.lower()
    content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
    if content_type and content_type != "application/octet-stream":
        ext = CONTENT_TYPE_EXTENSIONS.get(content_type) or mimetypes.guess_extension(content_type)
        if ext:
            return ext
    ext = Path(urlparse(url).path).suffix
    return ext.lower() if ext else None



# ===========================================================================
# SCOUT (Producer) — Crawl REGDOCS, insert documents with status=NEW
# ===========================================================================

@dataclass
class ScoutConfig:
    start_date: str
    end_date: str
    db_path: Path
    facets: Optional[List[str]]
    limit: Optional[int] = None
    dry_run: bool = False
    verbose: bool = False
    page_size: int = 200
    concurrency: int = 1
    min_delay: float = 2.0
    max_delay: float = 4.0
    max_retries: int = 4
    retry_backoff: float = 2.0


@dataclass
class Fetcher:
    """Polite HTTP fetcher: shared client, semaphore, jittered delay, retries."""
    client: httpx.AsyncClient
    config: ScoutConfig
    limiter: Optional[TokenBucketLimiter] = None
    semaphore: asyncio.Semaphore = field(init=False)
    requests_made: int = 0

    def __post_init__(self):
        self.semaphore = asyncio.Semaphore(self.config.concurrency)

    async def get(self, params: Dict[str, Any]) -> Optional[str]:
        async with self.semaphore:
            for attempt in range(self.config.max_retries + 1):
                try:
                    if self.limiter:
                        await self.limiter.acquire()
                    resp = await self.client.get(RESULTS_URL, params=params, timeout=60.0)
                    self.requests_made += 1
                    if resp.status_code == 200:
                        await asyncio.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
                        return resp.text
                    if resp.status_code in (429, 500, 502, 503, 504):
                        raise httpx.HTTPStatusError(
                            f"status {resp.status_code}", request=resp.request, response=resp
                        )
                    logging.error(f"Unexpected status {resp.status_code} for {params}")
                    return None
                except (httpx.RequestError, httpx.HTTPStatusError) as e:
                    if attempt == self.config.max_retries:
                        logging.error(f"Giving up after {attempt + 1} attempts: {e}")
                        return None
                    sleep_for = self.config.retry_backoff ** (attempt + 1) + random.uniform(0, 1)
                    logging.warning(f"Retrying in {sleep_for:.1f}s ({e})")
                    await asyncio.sleep(sleep_for)
        return None


def parse_total(html: str) -> Optional[int]:
    m = TOTAL_RE.search(html)
    return int(m.group(1).replace(",", "")) if m else None


def parse_rows(html: str) -> List[Dict[str, Any]]:
    """Extracts one record per result row."""
    soup = BeautifulSoup(html, SOUP_PARSER)
    tbody = soup.find("tbody")
    if not tbody:
        return []

    results = []
    for row in tbody.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue

        summary = tds[0].find("summary")
        cell = summary if summary else tds[0]

        link_el = None
        for a in cell.find_all("a"):
            if ITEM_HREF_RE.search(a.get("href", "")):
                link_el = a
                break
        if not link_el:
            continue

        href_match = ITEM_HREF_RE.search(link_el["href"])
        record: Dict[str, Any] = {
            "id": int(href_match.group(2)),
            "name": link_el.get_text(strip=True),
            "url": f"{DOMAIN}{link_el['href']}",
            "is_file": href_match.group(1) == "File/Download",
            "date": tds[1].get_text(strip=True),
            "submitter": tds[2].get_text(strip=True) or "",
        }

        icon = cell.find("i", title=True)
        record["kind"] = icon["title"] if icon else None

        details = tds[0].find("details")
        if details:
            for a in details.find_all("a"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                m = ITEM_HREF_RE.search(href)
                if not m or a is link_el:
                    continue
                filing = FILING_RE.search(text)
                if filing:
                    record["filing_number"] = filing.group(1)
                    record["filing_id"] = int(m.group(2))
                    continue
                label_div = a.parent.find_previous_sibling("div")
                label = label_div.get_text(strip=True).rstrip(":") if label_div else ""
                if label == "Company":
                    record["company"] = text
                    record["company_id"] = int(m.group(2))
                elif label == "Project":
                    record["project"] = text
                    record["project_id"] = int(m.group(2))

            hr = details.find("hr")
            if hr:
                snippet_div = hr.find_next_sibling("div")
                if snippet_div:
                    record["snippet"] = snippet_div.get_text(strip=True)

        results.append(record)
    return results


async def fetch_facet_catalog(client: httpx.AsyncClient) -> Dict[str, Dict[str, str]]:
    """Scrapes the Advanced Search page for {category: {filter_id: label}}."""
    resp = await client.get(ADVANCED_URL, timeout=60.0)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, SOUP_PARSER)

    catalog: Dict[str, Dict[str, str]] = {}
    for label_el in soup.find_all("label", attrs={"for": re.compile(r"^selectFilter\d+$")}):
        category = label_el.get_text(strip=True)
        select = soup.find("select", id=label_el["for"])
        if not select:
            continue
        catalog[category] = {
            opt["value"]: opt.get_text(strip=True)
            for opt in select.find_all("option")
            if opt.get("value")
        }
    return catalog


async def crawl_search(
    fetcher: Fetcher,
    base_params: Dict[str, Any],
    limit: Optional[int] = None,
    progress: Optional[tqdm] = None,
) -> Tuple[Dict[int, Dict[str, Any]], int]:
    """Crawls all pages of one search. Returns ({id: record}, reported_total)."""
    cfg = fetcher.config
    params = {**base_params, "srt": SORT_OLDEST_FIRST, "sr": 1}
    first_page = await fetcher.get(params)
    if first_page is None:
        return {}, 0

    total = parse_total(first_page) or 0
    records: Dict[int, Dict[str, Any]] = {}

    def absorb(rows: List[Dict[str, Any]]) -> None:
        for rec in rows:
            if rec["id"] not in records:
                records[rec["id"]] = rec
                if progress is not None:
                    progress.update(1)

    absorb(parse_rows(first_page))

    target = min(total, limit) if limit else total
    if progress is not None and progress.total is None:
        progress.total = target
        progress.refresh()

    offsets = [sr for sr in range(1 + cfg.page_size, max(target, 1) + 1, cfg.page_size)]
    if offsets:
        pages = await asyncio.gather(*(fetcher.get({**params, "sr": sr}) for sr in offsets))
        last_full = True
        for page in pages:
            rows = parse_rows(page) if page else []
            absorb(rows)
            if page is not None:
                last_full = len(rows) == cfg.page_size
        next_sr = offsets[-1] + cfg.page_size
    else:
        last_full = len(records) == cfg.page_size
        next_sr = 1 + cfg.page_size

    while last_full and (limit is None or len(records) < limit):
        page = await fetcher.get({**params, "sr": next_sr})
        if page is None:
            break
        rows = parse_rows(page)
        if not rows:
            break
        absorb(rows)
        last_full = len(rows) == cfg.page_size
        next_sr += cfg.page_size

    if limit and len(records) > limit:
        records = dict(list(records.items())[:limit])
    return records, total


def parse_facets_arg(value: str) -> Optional[List[str]]:
    if value == "all":
        return None
    if value == "none":
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


async def run_scout(args) -> None:
    """Execute the scout stage."""
    config = ScoutConfig(
        start_date=validate_date(args.start_date),
        end_date=validate_date(args.end_date),
        db_path=Path(args.db),
        facets=parse_facets_arg(args.facets) if isinstance(args.facets, str) else args.facets,
        limit=args.limit,
        dry_run=args.dry_run,
        verbose=args.verbose,
        page_size=args.page_size,
        concurrency=max(1, args.concurrency),
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )

    started_at = datetime.now(timezone.utc)
    t0_monotonic = time.monotonic()
    date_params = {"sd": config.start_date, "ed": config.end_date}
    limiter = TokenBucketLimiter(rate=1.0 / max(config.min_delay, 0.1), burst=config.concurrency)

    logging.info(f"Crawling {config.start_date} to {config.end_date} "
                 f"(page size {config.page_size}, parser {SOUP_PARSER})")
    if config.dry_run:
        logging.info("DRY RUN: nothing will be written to the database")

    # Open DB early so stats are visible during the crawl
    db = None
    run_id = None
    if not config.dry_run:
        db = get_db(config.db_path)
        run_id = db.start_run("scout", {
            "start_date": config.start_date,
            "end_date": config.end_date,
            "page_size": config.page_size,
            "limit": config.limit,
        })

    try:
        async with httpx.AsyncClient(
            headers=HEADERS,
            cookies={"RDI-NumberOfRecords": str(config.page_size)},
            follow_redirects=True,
        ) as client:
            fetcher = Fetcher(client=client, config=config, limiter=limiter)

            with tqdm(desc="Documents", unit=" items", total=None) as pbar:
                records, reported_total = await crawl_search(
                    fetcher, date_params, limit=config.limit, progress=pbar
                )
            logging.info(f"Base crawl: {len(records)} unique items (site reported ~{reported_total})")

            # Write to DB immediately after base crawl so stats show progress
            if db:
                now_iso = datetime.now(timezone.utc).isoformat()
                for rec in records.values():
                    rec["scraped_at"] = now_iso
                    db.upsert_document(
                        doc_id=str(rec["id"]),
                        name=rec["name"],
                        url=rec["url"],
                        metadata=rec,
                    )
                logging.info(f"Inserted {len(records)} records into database (facet enrichment next...)")

            # Facet enrichment
            facet_counts: Dict[str, Dict[str, int]] = {}
            selected: List[str] = []
            if config.facets != [] and records:
                catalog = await fetch_facet_catalog(client)
                if config.facets is None:
                    selected = list(catalog)
                else:
                    by_slug = {slugify(c): c for c in catalog}
                    for wanted in config.facets:
                        match = by_slug.get(slugify(wanted))
                        if match:
                            selected.append(match)
                        else:
                            logging.warning(
                                f"Facet category '{wanted}' not found (available: {', '.join(catalog)}); skipping"
                            )

                jobs = [
                    (category, filter_id, label)
                    for category in selected
                    for filter_id, label in catalog[category].items()
                ]
                logging.info(f"Enriching with {len(jobs)} facet values across: {', '.join(selected)}")

                for rec in records.values():
                    for category in selected:
                        rec[facet_field(category)] = []

                async def tag(category: str, filter_id: str, label: str) -> None:
                    tagged, _ = await crawl_search(fetcher, {**date_params, "rds": filter_id})
                    field_name = facet_field(category)
                    hits = 0
                    for item_id in tagged:
                        if item_id in records:
                            records[item_id][field_name].append(label)
                            hits += 1
                    facet_counts.setdefault(category, {})[label] = hits

                with tqdm(total=len(jobs), desc="Facets", unit=" facet") as fbar:
                    async def tag_and_tick(job):
                        await tag(*job)
                        fbar.update(1)
                    await asyncio.gather(*(tag_and_tick(job) for job in jobs))

                # Update DB with enriched metadata
                if db:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    for rec in records.values():
                        rec["scraped_at"] = now_iso
                        db.upsert_document(
                            doc_id=str(rec["id"]),
                            name=rec["name"],
                            url=rec["url"],
                            metadata=rec,
                        )

        # Finalize
        if db:
            now_iso = datetime.now(timezone.utc).isoformat()
            t0 = time.monotonic()
            db.record_metric("scout", "crawl_and_upsert", success=True,
                             duration_ms=(time.monotonic() - t0_monotonic) * 1000,
                             detail=f"{len(records)} records")
            db.finish_run(run_id, {
                "started_at": started_at.isoformat(),
                "finished_at": now_iso,
                "records": len(records),
                "site_reported_total": reported_total,
                "http_requests": fetcher.requests_made,
                "facets_enriched": selected,
                "facet_match_counts": facet_counts,
            })
            logging.info(f"DONE: {len(records)} records -> {config.db_path} "
                         f"({fetcher.requests_made} HTTP requests)")
        else:
            logging.info(f"DONE (dry run): {len(records)} records, "
                         f"{fetcher.requests_made} HTTP requests")

    finally:
        if db:
            db.close()


# ===========================================================================
# DOWNLOAD (Worker) — Download files, update status to DOWNLOADED
# ===========================================================================

@dataclass
class DownloadConfig:
    db_path: Path
    output_dir: Path
    concurrency: int = 1
    min_delay: float = 2.0
    max_delay: float = 4.0
    max_retries: int = 3
    retry_backoff: float = 2.0
    verbose: bool = False
    force: bool = False
    include_html: bool = False


async def download_one(
    client: httpx.AsyncClient,
    record: Dict[str, Any],
    config: DownloadConfig,
    db: Any,
    limiter: TokenBucketLimiter,
    semaphore: asyncio.Semaphore,
    existing_by_stem: Dict[str, Path],
    progress: tqdm,
    counters: Dict[str, int],
) -> None:
    """Download a single document, update DB on success/failure."""
    async with semaphore:
        doc_id = record["id"]
        name = record.get("name", "unknown")
        url = record.get("url")
        metadata = json.loads(record["metadata"]) if record.get("metadata") else {}
        is_file = metadata.get("is_file", True)
        kind = metadata.get("kind")
        extension = metadata.get("extension")

        try:
            if not is_file:
                counters["skipped"] += 1
                return

            if not url or not doc_id:
                db.mark_failed(doc_id, "Missing URL or ID", "download")
                db.record_metric("download", "fetch", document_id=doc_id, success=False, error_type="missing_url")
                counters["errors"] += 1
                return

            slug_name = slugify(name)

            # HTML skip logic
            is_html = extension == ".html" or kind == "Html Document"
            if is_html and not config.include_html:
                counters["skipped_html"] += 1
                return

            # Skip-exists logic
            if not config.force:
                file_path = record.get("file_path")
                if file_path and Path(file_path).exists():
                    counters["already"] += 1
                    return
                # Also check by doc_id prefix in case the name changed since download
                existing = existing_by_stem.get(f"{doc_id}_{slug_name}")
                if not existing:
                    # Check if any file starts with this doc_id (handles name changes)
                    existing = next(
                        (p for stem, p in existing_by_stem.items() if stem.startswith(f"{doc_id}_")),
                        None,
                    )
                if existing:
                    counters["already"] += 1
                    return

            save_path: Optional[Path] = None
            t0 = time.monotonic()

            for attempt in range(config.max_retries + 1):
                try:
                    await limiter.acquire()
                    await asyncio.sleep(random.uniform(config.min_delay, config.max_delay))

                    async with client.stream("GET", url, follow_redirects=True) as response:
                        if response.status_code == 200:
                            content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
                            ext = extension_from_response(response, url)
                            if ext is None:
                                error_msg = f"Could not determine file type (Content-Type: {content_type or 'none'})"
                                db.mark_failed(doc_id, error_msg, "download")
                                db.record_metric("download", "fetch", document_id=doc_id, success=False,
                                                 duration_ms=(time.monotonic() - t0) * 1000,
                                                 error_type="unknown_type", detail=error_msg)
                                counters["errors"] += 1
                                return

                            filename = f"{doc_id}_{slug_name}{ext}"
                            save_path = config.output_dir / filename
                            final_url = str(response.url)
                            server_filename = unquote(Path(urlparse(final_url).path).name) or None

                            hasher = hashlib.sha256()
                            size = 0

                            if ext == ".html":
                                body = await response.aread()
                                if b"Content Server - Error" in body[:4096]:
                                    raise ValueError("Content Server error page")
                                if not config.include_html:
                                    counters["skipped_html"] += 1
                                    return
                                save_path.write_bytes(body)
                                hasher.update(body)
                                size = len(body)
                            else:
                                with open(save_path, "wb") as f:
                                    async for chunk in response.aiter_bytes():
                                        f.write(chunk)
                                        hasher.update(chunk)
                                        size += len(chunk)

                                if content_type == "application/pdf":
                                    with open(save_path, "rb") as f:
                                        if f.read(5) != b"%PDF-":
                                            raise ValueError("Invalid PDF header")

                            duration_ms = (time.monotonic() - t0) * 1000
                            metadata_update = {
                                "extension": ext,
                                "content_type": content_type or None,
                                "size_bytes": size,
                                "server_filename": server_filename,
                                "final_url": final_url,
                                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                            }
                            if response.headers.get("Last-Modified"):
                                metadata_update["last_modified"] = response.headers["Last-Modified"]

                            db.mark_downloaded(doc_id, str(save_path), hasher.hexdigest(), metadata_update)
                            db.record_metric("download", "fetch", document_id=doc_id, success=True,
                                             duration_ms=duration_ms, detail=filename)
                            counters["downloaded"] += 1
                            logging.info(f"Downloaded: {filename}")
                            return

                        elif response.status_code in (429, 500, 502, 503, 504):
                            raise httpx.HTTPStatusError(
                                f"status {response.status_code}", request=response.request, response=response
                            )
                        else:
                            error_msg = f"Unexpected status {response.status_code}"
                            db.mark_failed(doc_id, error_msg, "download")
                            db.record_metric("download", "fetch", document_id=doc_id, success=False,
                                             duration_ms=(time.monotonic() - t0) * 1000,
                                             error_type=f"http_{response.status_code}")
                            counters["errors"] += 1
                            # Clean up any partial file written before the status was checked
                            if save_path and save_path.exists():
                                save_path.unlink()
                            return

                except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
                    if attempt == config.max_retries:
                        error_msg = f"Giving up after {attempt + 1} attempts: {e}"
                        db.mark_failed(doc_id, error_msg, "download")
                        db.record_metric("download", "fetch", document_id=doc_id, success=False,
                                         duration_ms=(time.monotonic() - t0) * 1000,
                                         error_type=type(e).__name__, detail=str(e))
                        counters["errors"] += 1
                        if save_path and save_path.exists():
                            save_path.unlink()
                        return
                    sleep_for = config.retry_backoff ** (attempt + 1) + random.uniform(0, 1)
                    logging.warning(f"Retrying {doc_id} in {sleep_for:.1f}s ({e})")
                    await asyncio.sleep(sleep_for)

        finally:
            progress.update(1)


async def run_download(args) -> None:
    """Execute the download stage."""
    config = DownloadConfig(
        db_path=Path(args.db),
        output_dir=Path(args.output_dir),
        concurrency=max(1, args.concurrency),
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_retries=getattr(args, 'max_retries', 3),
        verbose=args.verbose,
        force=args.force,
        include_html=args.include_html,
    )

    db = get_db(config.db_path)

    records = db.get_pending_downloads(max_retries=config.max_retries, include_html=config.include_html)
    logging.info(f"Found {len(records)} documents to download")

    # Dry-run mode: just show what would be downloaded
    if getattr(args, 'dry_run', False):
        for rec in records[:20]:
            meta = json.loads(rec["metadata"]) if rec.get("metadata") else {}
            print(f"  {rec['id']:>8}  {rec.get('name', '?')[:50]:<50}  {meta.get('kind', '?')}")
        if len(records) > 20:
            print(f"  ... and {len(records) - 20} more")
        print(f"\nTotal: {len(records)} documents would be downloaded.")
        db.close()
        return

    if not records:
        logging.info("Nothing to download.")
        db.close()
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)
    run_id = db.start_run("downloader", {
        "output_dir": str(config.output_dir),
        "concurrency": config.concurrency,
        "force": config.force,
        "include_html": config.include_html,
    })

    limiter = TokenBucketLimiter(rate=1.0 / max(config.min_delay, 0.1), burst=config.concurrency)
    semaphore = asyncio.Semaphore(config.concurrency)
    existing_by_stem = {p.stem: p for p in config.output_dir.iterdir() if p.is_file()}
    counters = {"downloaded": 0, "already": 0, "skipped": 0, "skipped_html": 0, "errors": 0}

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            with tqdm(total=len(records), desc="Downloading", unit=" file") as pbar:
                tasks = [
                    download_one(client, rec, config, db, limiter, semaphore, existing_by_stem, pbar, counters)
                    for rec in records
                ]
                await asyncio.gather(*tasks)
    finally:
        db.finish_run(run_id, counters)
        db.close()

    if counters["skipped_html"]:
        logging.info(f"{counters['skipped_html']} {HTML_SKIP_NOTE}")
    logging.info(
        f"Finished. Downloaded: {counters['downloaded']}, Already existed: {counters['already']}, "
        f"Skipped HTML: {counters['skipped_html']}, Errors: {counters['errors']}"
    )


# ===========================================================================
# CONVERT (Processor) — Convert downloaded files to Markdown
# ===========================================================================

def compute_quality_score(text: str) -> float:
    """Score converted markdown quality from 0.0 (garbled OCR) to 1.0 (clean text).

    Heuristics:
    - avg_word_length: real prose has avg 4-7 chars; OCR fragments are shorter
    - long_line_ratio: proportion of lines with 40+ chars (prose vs scattered labels)
    - alpha_ratio: proportion of alphabetic chars (vs digits, symbols in drawings)
    - short_fragment_ratio: lines under 5 chars are noise
    - sentence_density: periods/question marks per line (prose has sentences)
    """
    if not text or not text.strip():
        return 0.0

    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return 0.0

    words = text.split()
    total_chars = len(text)

    # Average word length (target: 4-7 for English prose)
    avg_word_len = sum(len(w) for w in words) / max(len(words), 1)
    word_len_score = min(1.0, max(0.0, (avg_word_len - 1.5) / 4.0))

    # Ratio of lines that are "long" (40+ chars) — prose has long lines
    long_lines = sum(1 for l in lines if len(l.strip()) >= 40)
    long_line_ratio = long_lines / len(lines)

    # Alphabetic character ratio (vs numbers, symbols, coordinates)
    alpha_chars = sum(1 for c in text if c.isalpha())
    alpha_ratio = alpha_chars / max(total_chars, 1)

    # Short fragment ratio (lines under 5 chars are noise)
    short_fragments = sum(1 for l in lines if len(l.strip()) < 5)
    short_frag_ratio = short_fragments / len(lines)
    short_frag_score = 1.0 - short_frag_ratio  # fewer short fragments = better

    # Sentence density: lines containing sentence-ending punctuation
    sentence_lines = sum(1 for l in lines if any(c in l for c in '.?!'))
    sentence_density = sentence_lines / len(lines)

    # Weighted combination
    score = (
        0.20 * word_len_score +
        0.25 * long_line_ratio +
        0.20 * alpha_ratio +
        0.15 * short_frag_score +
        0.20 * sentence_density
    )

    return round(min(1.0, max(0.0, score)), 3)


async def convert_one_subprocess(
    record: Dict[str, Any],
    output_dir: Path,
    db: Any,
    progress: tqdm,
    counters: Dict[str, int],
    timeout: int = 300,
) -> None:
    """Convert a single document by spawning convert_worker.py as a subprocess.

    This isolates Docling's native code (which can segfault on certain PDFs)
    in a child process. If the child crashes, the parent logs the failure and
    moves on to the next document.
    """
    doc_id = record["id"]
    file_path = record.get("file_path")
    name = record.get("name", "unknown")
    slug_name = slugify(name)

    try:
        if not file_path or not Path(file_path).exists():
            db.mark_failed(doc_id, f"File not found: {file_path}", "convert")
            db.record_metric("convert", "convert_file", document_id=doc_id, success=False,
                             error_type="file_not_found")
            counters["errors"] += 1
            return

        doc_path = Path(file_path)
        ext = doc_path.suffix.lower()
        if ext not in CONVERTIBLE_EXTENSIONS:
            counters["skipped"] += 1
            return

        markdown_path = output_dir / f"{doc_id}_{slug_name}.md"

        # Idempotency: the markdown already exists — e.g., a previous run wrote
        # it but was interrupted before the DB update. Reconcile the DB here,
        # otherwise the document stays DOWNLOADED forever and is re-scanned on
        # every run without ever being marked CONVERTED.
        if markdown_path.exists() and markdown_path.stat().st_size > 0:
            text = markdown_path.read_text(encoding="utf-8", errors="replace")
            quality = compute_quality_score(PAGE_MARKER_RE.sub("", text))
            db.mark_converted(doc_id, str(markdown_path), quality_score=quality)
            counters["already"] += 1
            return

        t0 = time.monotonic()

        # Build subprocess command
        worker_script = Path(__file__).parent / "convert_worker.py"
        cmd = [sys.executable, str(worker_script), str(doc_path), str(markdown_path)]
        if ext in (".html", ".htm"):
            cmd.append("--html-preprocess")

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)

        except asyncio.TimeoutError:
            # Kill the subprocess if it timed out
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            duration_ms = (time.monotonic() - t0) * 1000
            db.mark_failed(doc_id, f"Timeout after {timeout}s", "convert")
            db.record_metric("convert", "convert_file", document_id=doc_id, success=False,
                             duration_ms=duration_ms, error_type="timeout")
            counters["errors"] += 1
            logging.warning(f"Timeout ({timeout}s): {doc_path.name}")
            return

        duration_ms = (time.monotonic() - t0) * 1000
        returncode = proc.returncode

        # Segfault: returncode is -11 (SIGSEGV) on Linux
        if returncode is not None and returncode < 0:
            import signal as _signal
            sig_name = "unknown signal"
            try:
                sig_name = _signal.Signals(-returncode).name
            except (ValueError, AttributeError):
                sig_name = f"signal {-returncode}"
            error_msg = f"Worker killed by {sig_name} (exit code {returncode})"
            db.mark_failed(doc_id, error_msg, "convert")
            db.record_metric("convert", "convert_file", document_id=doc_id, success=False,
                             duration_ms=duration_ms, error_type="crash", detail=error_msg)
            counters["errors"] += 1
            logging.error(f"CRASH ({sig_name}): {doc_path.name}")
            return

        # Parse JSON result from stdout
        stdout_text = stdout.decode("utf-8", errors="replace").strip()
        if returncode == 0 and stdout_text:
            try:
                result = json.loads(stdout_text)
                if result.get("success"):
                    quality_score = result.get("quality_score", 0.0)
                    db.mark_converted(doc_id, str(markdown_path), quality_score=quality_score)
                    db.record_metric("convert", "convert_file", document_id=doc_id, success=True,
                                     duration_ms=duration_ms,
                                     detail=f"{markdown_path.name} quality={quality_score:.3f}")
                    counters["converted"] += 1
                    logging.info(f"Converted: {markdown_path.name} (quality={quality_score:.2f}, {duration_ms/1000:.1f}s)")
                    return
            except json.JSONDecodeError:
                pass

        # Worker returned non-zero or unparseable output
        error_msg = "Unknown error"
        error_type = "worker_error"
        if stdout_text:
            try:
                result = json.loads(stdout_text)
                error_msg = result.get("error", "Unknown error")
                error_type = result.get("error_type", "worker_error")
            except json.JSONDecodeError:
                error_msg = f"Worker exit code {returncode}"
        else:
            # No stdout — check stderr for clues
            stderr_text = stderr.decode("utf-8", errors="replace").strip()
            last_lines = stderr_text.split("\n")[-3:] if stderr_text else []
            error_msg = f"Worker exit code {returncode}: {' | '.join(last_lines)}"[:500]

        db.mark_failed(doc_id, error_msg, "convert")
        db.record_metric("convert", "convert_file", document_id=doc_id, success=False,
                         duration_ms=duration_ms, error_type=error_type, detail=error_msg[:200])
        counters["errors"] += 1
        logging.error(f"Failed: {doc_path.name}: {error_msg[:100]}")

    finally:
        progress.update(1)


async def run_convert(args) -> None:
    """Execute the convert stage.

    Each document is converted in a separate subprocess (convert_worker.py)
    so that segfaults in Docling's native PDF code cannot crash the pipeline.
    """
    db_path = Path(args.db)
    output_dir = Path(args.output_dir)
    max_retries = getattr(args, 'max_retries', 3)

    db = get_db(db_path)

    # Filter non-convertible extensions at the SQL level
    ext_conditions = " OR ".join(f"file_path LIKE '%{ext}'" for ext in CONVERTIBLE_EXTENSIONS)
    records = db.conn.execute(
        f"""SELECT * FROM documents
            WHERE (status = 'DOWNLOADED'
               OR (status = 'FAILED' AND file_path IS NOT NULL AND markdown_path IS NULL AND retry_count < ?))
              AND ({ext_conditions})
            ORDER BY retry_count ASC, created_at ASC""",
        (max_retries,),
    ).fetchall()
    records = [dict(row) for row in records]

    logging.info(f"Found {len(records)} convertible documents")

    # Dry-run mode: just show what would be converted
    if getattr(args, 'dry_run', False):
        for rec in records[:20]:
            fp = rec.get('file_path', '')
            ext = Path(fp).suffix if fp else '?'
            print(f"  {rec['id']:>8}  {rec.get('name', '?')[:50]:<50}  {ext}")
        if len(records) > 20:
            print(f"  ... and {len(records) - 20} more")
        print(f"\nTotal: {len(records)} documents would be converted.")
        db.close()
        return

    if not records:
        logging.info("Nothing to convert.")
        db.close()
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = db.start_run("converter", {
        "output_dir": str(output_dir),
    })

    counters = {"converted": 0, "already": 0, "skipped": 0, "errors": 0}

    try:
        with tqdm(total=len(records), desc="Converting", unit=" file") as pbar:
            for rec in records:
                try:
                    await convert_one_subprocess(
                        rec, output_dir, db, pbar, counters,
                        timeout=getattr(args, 'timeout', 300),
                    )
                except KeyboardInterrupt:
                    logging.info("Interrupted by user — stopping conversion.")
                    break
                except Exception as e:
                    doc_id = rec.get("id", "?")
                    logging.error(f"Unexpected error on doc {doc_id}: {type(e).__name__}: {e}")
                    try:
                        db.mark_failed(str(doc_id), f"{type(e).__name__}: {e}", "convert")
                    except Exception:
                        pass
                    counters["errors"] += 1
                    pbar.update(1)
                    continue
    finally:
        db.finish_run(run_id, counters)
        db.close()

    logging.info(
        f"Finished. Converted: {counters['converted']}, Already existed: {counters['already']}, "
        f"Skipped (unsupported): {counters['skipped']}, Errors: {counters['errors']}"
    )


# ===========================================================================
# STATS — Show pipeline status and metrics
# ===========================================================================

def format_duration(ms: Optional[float]) -> str:
    """Format milliseconds into a human-readable duration."""
    if ms is None:
        return "n/a"
    if ms < 1000:
        return f"{ms:.0f}ms"
    elif ms < 60_000:
        return f"{ms / 1000:.1f}s"
    elif ms < 3_600_000:
        minutes = ms / 60_000
        return f"{minutes:.1f}m"
    else:
        hours = ms / 3_600_000
        return f"{hours:.1f}h"


def run_stats(args) -> None:
    """Display pipeline statistics."""
    db = get_db(Path(args.db))
    stats = db.stats_summary()

    total = stats['total']
    by_status = stats['by_status']
    stage_metrics = stats.get("stage_metrics", {})

    new_count = by_status.get('NEW', 0)
    dl_count = by_status.get('DOWNLOADED', 0)
    converted_count = by_status.get('CONVERTED', 0)
    failed_count = by_status.get('FAILED', 0)

    # Count HTML documents excluded from download (still in NEW status)
    html_new_count = db.conn.execute(
        """SELECT COUNT(*) as cnt FROM documents
           WHERE (json_extract(metadata, '$.kind') = 'Html Document'
              OR json_extract(metadata, '$.extension') = '.html')
             AND status = 'NEW'"""
    ).fetchone()["cnt"]

    # Count non-file documents (Item/View pages, not downloadable files)
    non_file_count = db.conn.execute(
        """SELECT COUNT(*) as cnt FROM documents
           WHERE json_extract(metadata, '$.is_file') = 0
             AND status = 'NEW'"""
    ).fetchone()["cnt"]

    # Count indexed docs from ChromaDB
    indexed_count = 0
    if CHROMA_DIR.exists():
        try:
            import chromadb
            client = chromadb.PersistentClient(path=str(CHROMA_DIR))
            collection = client.get_collection(name=CHROMA_COLLECTION)
            all_ids = collection.get()["ids"]
            indexed_count = len({cid.rsplit("_chunk_", 1)[0] for cid in all_ids
                                 if "_chunk_" in cid})
        except Exception:
            pass

    # --- Determine what the "effective" processable total is ---
    # HTML docs and non-file items won't be downloaded, so exclude from progress
    excluded_count = html_new_count + non_file_count
    processable = total - excluded_count
    done_downloading = dl_count + converted_count  # DOWNLOADED + CONVERTED have files

    # --- Header ---
    print(f"\n{'═' * 60}")
    print("  REGDOCS Pipeline Dashboard")
    print(f"{'═' * 60}")

    # --- Big picture: where are we? ---
    print()
    print(f"  Documents discovered:  {total:>6}")
    if excluded_count:
        exclusion_parts = []
        if non_file_count:
            exclusion_parts.append(f"{non_file_count} non-file")
        if html_new_count:
            exclusion_parts.append(f"{html_new_count} HTML*")
        print(f"  Processable:           {processable:>6}  ({', '.join(exclusion_parts)} excluded)")
    print()

    # --- Visual pipeline with counts at each stage ---
    # Show a clear left-to-right flow
    bar_width = 20

    def progress_bar(done: int, of: int) -> str:
        if of == 0:
            return f"[{'─' * bar_width}]"
        filled = int(bar_width * done / of)
        return f"[{'█' * filled}{'░' * (bar_width - filled)}]"

    def pct(done: int, of: int) -> str:
        if of == 0:
            return "  -"
        return f"{done / of * 100:>3.0f}%"

    print(f"  Stage        Done / Total    Progress")
    print(f"  {'─' * 52}")
    print(f"  Download   {done_downloading:>5} / {processable:<5}  {progress_bar(done_downloading, processable)}  {pct(done_downloading, processable)}")
    print(f"  Convert    {converted_count:>5} / {processable:<5}  {progress_bar(converted_count, processable)}  {pct(converted_count, processable)}")
    print(f"  Index      {indexed_count:>5} / {processable:<5}  {progress_bar(indexed_count, processable)}  {pct(indexed_count, processable)}")
    if failed_count:
        print(f"  Failed     {failed_count:>5}             (will auto-retry)")

    # --- What's next / what to run ---
    print()
    print(f"  {'─' * 52}")
    print("  Next Steps:")

    pending_download = new_count - excluded_count
    if pending_download > 0:
        dl_avg_ms = stage_metrics.get("download", {}).get("avg_duration_ms")
        eta = f"  ETA: ~{format_duration(pending_download * dl_avg_ms)}" if dl_avg_ms else ""
        print(f"    → {pending_download} docs ready to download{eta}")
        print(f"      Run: python regdocs.py download")
    if dl_count > 0:
        cv_avg_ms = stage_metrics.get("convert", {}).get("avg_duration_ms")
        eta = f"  ETA: ~{format_duration(dl_count * cv_avg_ms)}" if cv_avg_ms else ""
        print(f"    → {dl_count} docs ready to convert{eta}")
        print(f"      Run: python regdocs.py convert")
    await_index = converted_count - indexed_count
    if await_index > 0:
        print(f"    → {await_index} docs ready to index")
        print(f"      Run: python regdocs.py index")
    if pending_download == 0 and dl_count == 0 and await_index <= 0:
        if indexed_count > 0:
            print(f"    ✓ Pipeline complete! {indexed_count} docs ready for questions.")
            print(f"      Run: python regdocs.py ask \"your question here\"")
        elif processable == 0:
            print(f"    No documents to process. Run 'scout' to discover documents.")
        else:
            print(f"    Nothing pending.")

    # --- Throughput ---
    has_throughput = False
    for stage in ("download", "convert"):
        m = stage_metrics.get(stage, {})
        if m.get("avg_duration_ms") and m["avg_duration_ms"] > 0:
            has_throughput = True
            break

    if has_throughput:
        print()
        print(f"  {'─' * 52}")
        print("  Speed:")
        for stage in ("download", "convert"):
            m = stage_metrics.get(stage, {})
            avg_ms = m.get("avg_duration_ms")
            if avg_ms and avg_ms > 0:
                per_min = 60_000 / avg_ms
                print(f"    {stage:<12} {per_min:>5.1f} docs/min  (avg {format_duration(avg_ms)} each)")

    # --- Disk usage ---
    print()
    print(f"  {'─' * 52}")
    print("  Disk:")
    for label, path in [("Downloads", Path("downloads")), ("Markdown", Path("markdown")), ("ChromaDB", CHROMA_DIR)]:
        if path.exists():
            size_bytes = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
            if size_bytes > 0:
                print(f"    {label:<12} {_format_size(size_bytes)}")

    # --- Quality scores ---
    quality_stats = db.conn.execute(
        """SELECT
            COUNT(*) as scored,
            ROUND(AVG(json_extract(metadata, '$.quality_score')), 3) as avg_quality,
            ROUND(MIN(json_extract(metadata, '$.quality_score')), 3) as min_quality,
            ROUND(MAX(json_extract(metadata, '$.quality_score')), 3) as max_quality,
            SUM(CASE WHEN json_extract(metadata, '$.quality_score') < 0.3 THEN 1 ELSE 0 END) as low_quality
        FROM documents
        WHERE json_extract(metadata, '$.quality_score') IS NOT NULL"""
    ).fetchone()

    if quality_stats and quality_stats["scored"] > 0:
        print()
        print(f"  {'─' * 52}")
        print("  Quality Scores:")
        print(f"    Scored:     {quality_stats['scored']} documents")
        print(f"    Average:    {quality_stats['avg_quality']:.3f}")
        print(f"    Range:      {quality_stats['min_quality']:.3f} – {quality_stats['max_quality']:.3f}")
        if quality_stats["low_quality"] > 0:
            print(f"    Low (<0.3):  {quality_stats['low_quality']} docs (likely scanned drawings)")
            print(f"      Use: python regdocs.py index --min-quality 0.3")

    # --- Failures ---
    if failed_count:
        print()
        print(f"  {'─' * 52}")
        print(f"  Failures ({failed_count} total):")
        failed_docs = db.failed_documents(limit=5)
        for doc in failed_docs:
            err = (doc.get("last_error") or "unknown")[:50]
            print(f"    #{doc['id']}  {err}")
        if failed_count > 5:
            print(f"    ... and {failed_count - 5} more (see: SELECT * FROM documents WHERE status='FAILED')")

    # --- Footnotes ---
    if html_new_count:
        print()
        print(f"  * {html_new_count} HTML documents excluded: CER's server doesn't serve their")
        print(f"    images, making them incomplete. Use --include-html to download anyway.")

    print(f"\n{'═' * 60}\n")
    db.close()


def _format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


# ===========================================================================
# INDEX — Chunk converted documents and embed into ChromaDB
# ===========================================================================

# Default chunking parameters
CHUNK_SIZE = 512       # tokens (approx chars / 4)
CHUNK_OVERLAP = 64     # tokens overlap between chunks
EMBED_BATCH_SIZE = 50  # chunks per Ollama embedding call
CHROMA_COLLECTION = "regdocs"
CHROMA_DIR = Path(__file__).parent / "chroma_db"
EMBED_MODEL = "nomic-embed-text"
LLM_MODEL = "gemma4:26b"


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks by approximate token count.

    Uses a simple word-based split (1 token ≈ 0.75 words) that works well
    enough for English regulatory text without needing a tokenizer dependency.
    """
    words = text.split()
    words_per_chunk = int(chunk_size * 0.75)
    words_overlap = int(overlap * 0.75)

    if len(words) <= words_per_chunk:
        return [text] if text.strip() else []

    chunks = []
    start = 0
    while start < len(words):
        end = start + words_per_chunk
        chunk = " ".join(words[start:end])
        if chunk.strip():
            chunks.append(chunk)
        start += words_per_chunk - words_overlap

    return chunks


PAGE_MARKER_RE = re.compile(r'<!--\s*page:(\d+)\s*-->')


def chunk_text_with_pages(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> List[Dict[str, Any]]:
    """Split page-annotated markdown into overlapping chunks with page ranges.

    Parses <!-- page:N --> markers injected by convert_one to track which pages
    each chunk spans.  Returns a list of dicts:
        {"text": str, "page_start": int, "page_end": int}

    Falls back gracefully when no page markers are present (page_start=page_end=1).
    """
    # Split lines and extract page markers
    lines = text.split('\n')
    current_page = 1
    clean_lines: List[str] = []
    line_pages: List[int] = []

    for line in lines:
        m = PAGE_MARKER_RE.match(line.strip())
        if m:
            current_page = int(m.group(1))
            continue
        clean_lines.append(line)
        line_pages.append(current_page)

    full_text = '\n'.join(clean_lines)
    words = full_text.split()
    words_per_chunk = int(chunk_size * 0.75)
    words_overlap = min(int(overlap * 0.75), words_per_chunk - 1)  # overlap can't exceed chunk size

    if not words:
        return []

    # Build a word → page mapping by walking through clean_lines
    word_page: List[int] = []
    for idx, line in enumerate(clean_lines):
        line_words = line.split()
        page = line_pages[idx] if idx < len(line_pages) else 1
        word_page.extend([page] * len(line_words))

    # Ensure word_page covers all words (edge case: whitespace differences)
    while len(word_page) < len(words):
        word_page.append(word_page[-1] if word_page else 1)

    # If document fits in one chunk, return as-is
    if len(words) <= words_per_chunk:
        p_start = word_page[0] if word_page else 1
        p_end = word_page[-1] if word_page else 1
        return [{"text": full_text, "page_start": p_start, "page_end": p_end}] if full_text.strip() else []

    chunks: List[Dict[str, Any]] = []
    start = 0
    while start < len(words):
        end = min(start + words_per_chunk, len(words))
        chunk_str = " ".join(words[start:end])
        if chunk_str.strip():
            p_start = word_page[start] if start < len(word_page) else 1
            p_end = word_page[min(end - 1, len(word_page) - 1)]
            chunks.append({"text": chunk_str, "page_start": p_start, "page_end": p_end})
        start += words_per_chunk - words_overlap

    return chunks


def batch_embed(ollama_client, texts: List[str], model: str, batch_size: int = EMBED_BATCH_SIZE) -> List[List[float]]:
    """Embed texts in batches for speed. Returns list of embedding vectors."""
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        response = ollama_client.embed(model=model, input=batch)
        all_embeddings.extend(response["embeddings"])
    return all_embeddings


def run_index(args) -> None:
    """Chunk converted documents and index into ChromaDB."""
    import chromadb
    import ollama as ollama_client

    db_path = Path(args.db)
    chroma_dir = Path(args.chroma_dir)
    chunk_size = getattr(args, 'chunk_size', CHUNK_SIZE)
    overlap = getattr(args, 'overlap', CHUNK_OVERLAP)

    # Validate overlap < chunk_size to prevent chunk explosion
    if overlap >= chunk_size:
        logging.error(f"--overlap ({overlap}) must be less than --chunk-size ({chunk_size})")
        sys.exit(1)

    chroma_dir.mkdir(parents=True, exist_ok=True)

    db = get_db(db_path)
    run_id = db.start_run("index", {
        "chroma_dir": str(chroma_dir),
        "embed_model": args.embed_model,
        "chunk_size": chunk_size,
        "overlap": overlap,
    })

    # Get all CONVERTED documents with a markdown_path
    min_quality = getattr(args, 'min_quality', 0.0)
    docs = db.conn.execute(
        "SELECT id, name, markdown_path, metadata FROM documents WHERE status = 'CONVERTED' AND markdown_path IS NOT NULL"
    ).fetchall()
    docs = [dict(row) for row in docs]

    # Filter by quality score if --min-quality is set
    skipped_low_quality = 0
    if min_quality > 0.0:
        filtered = []
        for doc in docs:
            metadata = json.loads(doc["metadata"]) if doc.get("metadata") else {}
            quality = metadata.get("quality_score", 1.0)  # default high for docs scored before this feature
            if quality >= min_quality:
                filtered.append(doc)
            else:
                skipped_low_quality += 1
        docs = filtered
        if skipped_low_quality:
            logging.info(f"Skipped {skipped_low_quality} low-quality documents (below {min_quality})")

    logging.info(f"Found {len(docs)} converted documents to index")

    if not docs:
        logging.info("Nothing to index.")
        db.finish_run(run_id, {"message": "Nothing to index"})
        db.close()
        return

    # Connect to ChromaDB
    client = chromadb.PersistentClient(path=str(chroma_dir))
    collection = client.get_or_create_collection(
        name=CHROMA_COLLECTION,
        metadata={"hnsw:space": "cosine"},
    )

    # Check what's already indexed (by document ID prefix in chunk IDs)
    existing_ids = set()
    all_stored_ids = []
    if not args.force:
        try:
            all_stored = collection.get()
            all_stored_ids = all_stored["ids"]
            for chunk_id in all_stored_ids:
                doc_id = chunk_id.rsplit("_chunk_", 1)[0]
                existing_ids.add(doc_id)
        except Exception:
            pass
    else:
        # Force mode: get all IDs so we can delete before re-inserting
        try:
            all_stored = collection.get()
            all_stored_ids = all_stored["ids"]
        except Exception:
            pass

    total_chunks = 0
    indexed_docs = 0
    skipped_docs = 0

    with tqdm(total=len(docs), desc="Indexing", unit=" doc") as pbar:
        for doc in docs:
            doc_id = doc["id"]
            markdown_path = doc.get("markdown_path")
            name = doc.get("name", "unknown")
            metadata = json.loads(doc["metadata"]) if doc.get("metadata") else {}

            pbar.update(1)

            # Skip if already indexed (unless --force)
            if doc_id in existing_ids and not args.force:
                skipped_docs += 1
                continue

            # Read markdown content
            if not markdown_path or not Path(markdown_path).exists():
                logging.warning(f"Markdown file missing for {doc_id}: {markdown_path}")
                continue

            content = Path(markdown_path).read_text(encoding="utf-8", errors="replace")
            if not content.strip():
                continue

            # Delete old chunks for this doc (proper dedup on re-index)
            old_ids = [cid for cid in all_stored_ids if cid.startswith(f"{doc_id}_chunk_")]
            if old_ids:
                collection.delete(ids=old_ids)

            # Chunk the document (page-aware)
            page_chunks = chunk_text_with_pages(content, chunk_size=chunk_size, overlap=overlap)
            if not page_chunks:
                continue

            # Build metadata header for each chunk (gives LLM context)
            meta_header = ""
            if metadata.get("company"):
                meta_header += f"Company: {metadata['company']}\n"
            if metadata.get("project"):
                meta_header += f"Project: {metadata['project']}\n"
            if metadata.get("filing_number"):
                meta_header += f"Filing: {metadata['filing_number']}\n"
            if metadata.get("date"):
                meta_header += f"Date: {metadata['date']}\n"
            if metadata.get("submitter"):
                meta_header += f"Submitter: {metadata['submitter']}\n"
            if metadata.get("kind"):
                meta_header += f"Kind: {metadata['kind']}\n"
            if metadata.get("document_types"):
                types = metadata["document_types"]
                meta_header += f"Document Type: {', '.join(types) if isinstance(types, list) else types}\n"
            if metadata.get("application_types"):
                app_types = metadata["application_types"]
                meta_header += f"Application Type: {', '.join(app_types) if isinstance(app_types, list) else app_types}\n"
            if metadata.get("commodities"):
                commodities = metadata["commodities"]
                meta_header += f"Commodity: {', '.join(commodities) if isinstance(commodities, list) else commodities}\n"
            if metadata.get("roles"):
                roles = metadata["roles"]
                meta_header += f"Role: {', '.join(roles) if isinstance(roles, list) else roles}\n"
            if meta_header:
                meta_header += "\n"

            # Prepare all chunks for batch embedding
            chunk_ids = []
            chunk_texts = []
            chunk_metadatas = []

            for i, pchunk in enumerate(page_chunks):
                enriched_chunk = f"{meta_header}{pchunk['text']}" if meta_header else pchunk["text"]
                chunk_ids.append(f"{doc_id}_chunk_{i}")
                chunk_texts.append(enriched_chunk)
                chunk_metadatas.append({
                    "document_id": doc_id,
                    "document_name": name,
                    "chunk_index": i,
                    "total_chunks": len(page_chunks),
                    "page_start": pchunk["page_start"],
                    "page_end": pchunk["page_end"],
                    "company": metadata.get("company") or "",
                    "company_id": str(metadata.get("company_id") or ""),
                    "project": metadata.get("project") or "",
                    "project_id": str(metadata.get("project_id") or ""),
                    "filing_number": metadata.get("filing_number") or "",
                    "date": metadata.get("date") or "",
                    "submitter": metadata.get("submitter") or "",
                    "kind": metadata.get("kind") or "",
                    "is_file": bool(metadata.get("is_file", False)),
                    "application_types": ", ".join(metadata.get("application_types") or []) if isinstance(metadata.get("application_types"), list) else (metadata.get("application_types") or ""),
                    "document_types": ", ".join(metadata.get("document_types") or []) if isinstance(metadata.get("document_types"), list) else (metadata.get("document_types") or ""),
                    "commodities": ", ".join(metadata.get("commodities") or []) if isinstance(metadata.get("commodities"), list) else (metadata.get("commodities") or ""),
                    "roles": ", ".join(metadata.get("roles") or []) if isinstance(metadata.get("roles"), list) else (metadata.get("roles") or ""),
                    "quality_score": metadata.get("quality_score") or 1.0,
                })

            # Document-level summary chunk (chunk_index=-1) for timeline/cross-doc queries.
            # This gives the LLM a compact representation of the entire document's identity,
            # so questions like "what filings did Company X make?" or "compare timelines"
            # can match on metadata without needing to read through content chunks.
            summary_parts = []
            if metadata.get("company"):
                summary_parts.append(f"Company: {metadata['company']}")
            if metadata.get("project"):
                summary_parts.append(f"Project: {metadata['project']}")
            if metadata.get("filing_number"):
                summary_parts.append(f"Filing: {metadata['filing_number']}")
            if metadata.get("date"):
                summary_parts.append(f"Date: {metadata['date']}")
            if metadata.get("submitter"):
                summary_parts.append(f"Submitter: {metadata['submitter']}")
            if metadata.get("kind"):
                summary_parts.append(f"Kind: {metadata['kind']}")
            if metadata.get("document_types"):
                types = metadata["document_types"]
                summary_parts.append(f"Document Type: {', '.join(types) if isinstance(types, list) else types}")
            if metadata.get("application_types"):
                app_types = metadata["application_types"]
                summary_parts.append(f"Application Type: {', '.join(app_types) if isinstance(app_types, list) else app_types}")
            if metadata.get("commodities"):
                commodities = metadata["commodities"]
                summary_parts.append(f"Commodity: {', '.join(commodities) if isinstance(commodities, list) else commodities}")
            if metadata.get("roles"):
                roles = metadata["roles"]
                summary_parts.append(f"Role: {', '.join(roles) if isinstance(roles, list) else roles}")
            summary_parts.append(f"Document: {name}")
            summary_parts.append(f"Total pages: {page_chunks[-1]['page_end'] if page_chunks else 1}")
            summary_parts.append(f"Total chunks: {len(page_chunks)}")

            summary_text = "\n".join(summary_parts)
            chunk_ids.append(f"{doc_id}_chunk_summary")
            chunk_texts.append(summary_text)
            chunk_metadatas.append({
                "document_id": doc_id,
                "document_name": name,
                "chunk_index": -1,
                "total_chunks": len(page_chunks),
                "page_start": 0,
                "page_end": 0,
                "company": metadata.get("company") or "",
                "company_id": str(metadata.get("company_id") or ""),
                "project": metadata.get("project") or "",
                "project_id": str(metadata.get("project_id") or ""),
                "filing_number": metadata.get("filing_number") or "",
                "date": metadata.get("date") or "",
                "submitter": metadata.get("submitter") or "",
                "kind": metadata.get("kind") or "",
                "is_file": bool(metadata.get("is_file", False)),
                "application_types": ", ".join(metadata.get("application_types") or []) if isinstance(metadata.get("application_types"), list) else (metadata.get("application_types") or ""),
                "document_types": ", ".join(metadata.get("document_types") or []) if isinstance(metadata.get("document_types"), list) else (metadata.get("document_types") or ""),
                "commodities": ", ".join(metadata.get("commodities") or []) if isinstance(metadata.get("commodities"), list) else (metadata.get("commodities") or ""),
                "roles": ", ".join(metadata.get("roles") or []) if isinstance(metadata.get("roles"), list) else (metadata.get("roles") or ""),
                "quality_score": metadata.get("quality_score") or 1.0,
                "is_summary": True,
            })

            # Batch embed all chunks for this document
            try:
                chunk_embeddings = batch_embed(ollama_client, chunk_texts, args.embed_model)
            except Exception as e:
                logging.error(f"Embedding failed for {doc_id}: {e}")
                continue

            # Upsert to ChromaDB
            if chunk_ids:
                collection.upsert(
                    ids=chunk_ids,
                    documents=chunk_texts,
                    metadatas=chunk_metadatas,
                    embeddings=chunk_embeddings,
                )
                total_chunks += len(chunk_ids)
                indexed_docs += 1

    db.record_metric("index", "embed_and_store", success=True,
                     detail=f"{indexed_docs} docs, {total_chunks} chunks")

    # ------------------------------------------------------------------
    # Filing-level summary chunks
    # ------------------------------------------------------------------
    # Group documents by filing_number and create a summary chunk per filing.
    # This helps answer "what happened in filing X?" and timeline questions.
    filing_rows = db.conn.execute(
        """SELECT id, name, metadata FROM documents
           WHERE status = 'CONVERTED' AND metadata IS NOT NULL
             AND json_extract(metadata, '$.filing_number') IS NOT NULL
             AND json_extract(metadata, '$.filing_number') != ''"""
    ).fetchall()

    filings: Dict[str, List[Dict[str, Any]]] = {}
    for row in filing_rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        fn = meta.get("filing_number", "")
        if fn:
            filings.setdefault(fn, []).append({
                "id": row["id"],
                "name": row["name"],
                "metadata": meta,
            })

    filing_chunk_count = 0
    if filings:
        logging.info(f"Creating filing-level summary chunks for {len(filings)} filings")

        # Delete old filing summary chunks
        old_filing_ids = [cid for cid in all_stored_ids if cid.startswith("filing_") and cid.endswith("_summary")]
        if old_filing_ids:
            # Delete in batches to avoid hitting ChromaDB limits
            for i in range(0, len(old_filing_ids), 500):
                collection.delete(ids=old_filing_ids[i:i+500])

        filing_chunk_ids = []
        filing_chunk_texts = []
        filing_chunk_metadatas = []

        for filing_number, filing_docs in filings.items():
            # Collect metadata across all docs in this filing
            dates = []
            companies = set()
            document_types = set()
            application_types = set()
            commodities = set()
            roles = set()

            for fd in filing_docs:
                meta = fd["metadata"]
                if meta.get("date"):
                    dates.append(meta["date"])
                if meta.get("company"):
                    companies.add(meta["company"])
                dt = meta.get("document_types", [])
                if isinstance(dt, list):
                    document_types.update(dt)
                elif dt:
                    document_types.add(dt)
                at = meta.get("application_types", [])
                if isinstance(at, list):
                    application_types.update(at)
                elif at:
                    application_types.add(at)
                c = meta.get("commodities", [])
                if isinstance(c, list):
                    commodities.update(c)
                elif c:
                    commodities.add(c)
                r = meta.get("roles", [])
                if isinstance(r, list):
                    roles.update(r)
                elif r:
                    roles.add(r)

            dates.sort()
            date_start = dates[0] if dates else ""
            date_end = dates[-1] if dates else ""
            company = ", ".join(sorted(companies)) if companies else ""

            # Build summary text
            summary_parts = [f"Filing: {filing_number}"]
            if company:
                summary_parts.append(f"Company: {company}")
            if date_start and date_end:
                summary_parts.append(f"Date Range: {date_start} to {date_end}")
                # Compute duration in days
                try:
                    from datetime import datetime as _dt
                    _d1 = _dt.strptime(date_start, "%Y-%m-%d")
                    _d2 = _dt.strptime(date_end, "%Y-%m-%d")
                    duration_days = (_d2 - _d1).days
                    if duration_days > 0:
                        summary_parts.append(f"Duration: {duration_days} days ({duration_days / 30:.1f} months)")
                except (ValueError, TypeError):
                    duration_days = 0
            elif date_start:
                summary_parts.append(f"Date: {date_start}")
                duration_days = 0
            else:
                duration_days = 0
            doc_types_str = ", ".join(sorted(document_types)) if document_types else ""
            summary_parts.append(f"Documents: {len(filing_docs)} ({doc_types_str})" if doc_types_str else f"Documents: {len(filing_docs)}")
            if commodities:
                summary_parts.append(f"Commodity: {', '.join(sorted(commodities))}")
            if application_types:
                summary_parts.append(f"Application Type: {', '.join(sorted(application_types))}")
            if roles:
                summary_parts.append(f"Roles: {', '.join(sorted(roles))}")
            # Complexity indicators for duration estimation
            has_ir = any("Information Request" in dt for dt in document_types)
            has_compliance = any("Compliance" in dt for dt in document_types)
            has_hearing = any("Hearing" in dt for dt in document_types)
            if has_ir:
                summary_parts.append("Contains: Information Requests")
            if has_compliance:
                summary_parts.append("Contains: Compliance filings")
            if has_hearing:
                summary_parts.append("Contains: Hearing documents")

            summary_text = "\n".join(summary_parts)
            chunk_id = f"filing_{filing_number}_summary"

            filing_chunk_ids.append(chunk_id)
            filing_chunk_texts.append(summary_text)
            filing_chunk_metadatas.append({
                "filing_number": filing_number,
                "company": company,
                "date": date_start,
                "date_end": date_end,
                "duration_days": duration_days,
                "document_count": len(filing_docs),
                "document_types": doc_types_str,
                "application_types": ", ".join(sorted(application_types)) if application_types else "",
                "commodities": ", ".join(sorted(commodities)) if commodities else "",
                "roles": ", ".join(sorted(roles)) if roles else "",
                "has_ir": has_ir,
                "has_hearing": has_hearing,
                "is_summary": True,
                "chunk_index": -2,
            })

        # Batch embed and upsert filing summary chunks
        if filing_chunk_ids:
            try:
                filing_embeddings = batch_embed(ollama_client, filing_chunk_texts, args.embed_model)
                # Upsert in batches
                batch_size = 100
                for i in range(0, len(filing_chunk_ids), batch_size):
                    collection.upsert(
                        ids=filing_chunk_ids[i:i+batch_size],
                        documents=filing_chunk_texts[i:i+batch_size],
                        metadatas=filing_chunk_metadatas[i:i+batch_size],
                        embeddings=filing_embeddings[i:i+batch_size],
                    )
                filing_chunk_count = len(filing_chunk_ids)
                total_chunks += filing_chunk_count
                logging.info(f"Created {filing_chunk_count} filing-level summary chunks")
            except Exception as e:
                logging.error(f"Failed to create filing summary chunks: {e}")

    # ------------------------------------------------------------------
    # Rebuild FTS5 keyword index
    # ------------------------------------------------------------------
    try:
        db.rebuild_fts()
        logging.info("Rebuilt FTS5 keyword search index")
    except Exception as e:
        logging.error(f"Failed to rebuild FTS5 index: {e}")

    db.finish_run(run_id, {
        "indexed_docs": indexed_docs,
        "total_chunks": total_chunks,
        "skipped_docs": skipped_docs,
        "embed_model": args.embed_model,
        "chunk_size": chunk_size,
        "overlap": overlap,
    })
    db.close()

    logging.info(f"Indexed {indexed_docs} documents ({total_chunks} chunks) into {chroma_dir}")
    if skipped_docs:
        logging.info(f"Skipped {skipped_docs} already-indexed documents (use --force to re-index)")


# ===========================================================================
# ASK — Query the RAG pipeline
# ===========================================================================

def resolve_chunk_regions(
    markdown_path: Optional[str],
    chunk_text: str,
    page_start: Optional[int],
    page_end: Optional[int],
    limit: int = 3,
) -> List[Dict[str, Any]]:
    """Match a retrieved chunk back to page regions via the bbox sidecar.

    The convert stage writes <name>.bbox.json next to each Markdown file with
    per-item text snippets and bounding boxes. This finds sidecar items whose
    text appears in the chunk (within the chunk's page range) so citations can
    point at the exact region on the page, not just the page number.
    """
    if not markdown_path:
        return []
    bbox_path = Path(markdown_path).with_suffix(".bbox.json")
    if not bbox_path.exists():
        return []
    try:
        sidecar = json.loads(bbox_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    chunk_norm = " ".join(chunk_text.split())
    regions = []
    for item in sidecar.get("items", []):
        text = " ".join((item.get("text") or "").split())
        if len(text) < 25:  # too short to match reliably
            continue
        provs = item.get("prov") or []
        if not provs:
            continue
        page = provs[0].get("page")
        if page_start and page_end and page is not None and not (page_start <= page <= page_end):
            continue
        if text[:120] in chunk_norm:
            regions.append({"page": page, "bbox": provs[0].get("bbox")})
            if len(regions) >= limit:
                break
    return regions


def run_ask(args) -> None:
    """Query the indexed documents using RAG."""
    import chromadb
    import ollama as ollama_client

    chroma_dir = Path(args.chroma_dir)
    if not chroma_dir.exists():
        logging.error(f"ChromaDB not found at {chroma_dir}. Run 'regdocs.py index' first.")
        sys.exit(1)

    question = " ".join(args.question)
    if not question.strip():
        logging.error("Please provide a question.")
        sys.exit(1)

    # Connect to ChromaDB
    client = chromadb.PersistentClient(path=str(chroma_dir))
    try:
        collection = client.get_collection(name=CHROMA_COLLECTION)
    except Exception:
        logging.error(f"Collection '{CHROMA_COLLECTION}' not found. Run 'regdocs.py index' first.")
        sys.exit(1)

    # Embed the question
    try:
        response = ollama_client.embed(model=args.embed_model, input=question)
        query_embedding = response["embeddings"][0]
    except Exception as e:
        logging.error(f"Failed to embed question (is Ollama running?): {e}")
        sys.exit(1)

    # Search for relevant chunks
    n_results = args.top_k

    # Build where filter for targeted queries
    where_clauses = []
    where_document = None
    if hasattr(args, 'company') and args.company:
        where_clauses.append({"company": args.company})
    if hasattr(args, 'project') and args.project:
        where_clauses.append({"project": args.project})
    if hasattr(args, 'filing') and args.filing:
        where_clauses.append({"filing_number": args.filing})
    # Date range
    if hasattr(args, 'after') and args.after:
        where_clauses.append({"date": {"$gte": args.after}})
    if hasattr(args, 'before') and args.before:
        where_clauses.append({"date": {"$lte": args.before}})

    # Substring filters via where_document
    doc_clauses = []
    if hasattr(args, 'application_type') and args.application_type:
        doc_clauses.append({"$contains": args.application_type})
    if hasattr(args, 'commodity') and args.commodity:
        doc_clauses.append({"$contains": args.commodity})
    if hasattr(args, 'document_type') and args.document_type:
        doc_clauses.append({"$contains": args.document_type})

    # Build final filter objects
    where_filter = None
    if len(where_clauses) == 1:
        where_filter = where_clauses[0]
    elif len(where_clauses) > 1:
        where_filter = {"$and": where_clauses}

    if len(doc_clauses) == 1:
        where_document = doc_clauses[0]
    elif len(doc_clauses) > 1:
        where_document = {"$and": doc_clauses}

    query_kwargs = {
        "query_embeddings": [query_embedding],
        "n_results": n_results,
    }
    if where_filter:
        query_kwargs["where"] = where_filter
        logging.info(f"Filtering by metadata: {where_filter}")
    if where_document:
        query_kwargs["where_document"] = where_document
        logging.info(f"Filtering by document content: {where_document}")

    results = collection.query(**query_kwargs)

    # --- Hybrid search: FTS keyword lookup for filing numbers / quoted names ---
    import re as _re
    _filing_pattern = _re.compile(r'\b[A-Z]{1,3}\d{4,6}\b|[A-Z]{2}-[A-Za-z]+-[A-Za-z]+-[A-Z0-9-]+')
    _quoted_pattern = _re.compile(r'"([^"]+)"')
    _needs_hybrid = bool(_filing_pattern.search(question) or _quoted_pattern.search(question))

    if _needs_hybrid:
        logging.info("Hybrid search triggered — running FTS keyword lookup")
        try:
            db = get_db(Path(args.db))
            fts_doc_ids = db.search_fts(question, limit=20)
            db.close()

            if fts_doc_ids:
                logging.info(f"FTS returned {len(fts_doc_ids)} document IDs")
                # Retrieve chunks from ChromaDB that belong to those documents
                fts_chunks_results = collection.get(
                    where={"document_id": {"$in": fts_doc_ids}},
                    include=["documents", "metadatas", "embeddings"],
                )
                if fts_chunks_results and fts_chunks_results.get("ids"):
                    logging.info(f"Found {len(fts_chunks_results['ids'])} FTS-sourced chunks in ChromaDB")
        except Exception as e:
            logging.warning(f"FTS hybrid search failed (non-fatal): {e}")
            fts_chunks_results = None
            fts_doc_ids = []
    else:
        fts_chunks_results = None
        fts_doc_ids = []

    # Merge vector results with FTS results
    if not results["documents"] or not results["documents"][0]:
        if not fts_chunks_results or not fts_chunks_results.get("ids"):
            print("No relevant documents found.")
            return
        # Only FTS results available
        chunks = fts_chunks_results["documents"]
        metadatas = fts_chunks_results["metadatas"]
        distances = [0.5] * len(chunks)  # neutral relevance for FTS-only hits
    else:
        chunks = list(results["documents"][0])
        metadatas = list(results["metadatas"][0])
        distances = list(results["distances"][0]) if results.get("distances") else [0.5] * len(chunks)

        # Merge FTS results (deduplicate by chunk ID)
        if fts_chunks_results and fts_chunks_results.get("ids"):
            vector_ids = set(results["ids"][0]) if results.get("ids") else set()
            for i, fts_id in enumerate(fts_chunks_results["ids"]):
                if fts_id not in vector_ids:
                    chunks.append(fts_chunks_results["documents"][i])
                    metadatas.append(fts_chunks_results["metadatas"][i])
                    distances.append(0.6)  # slightly lower relevance for FTS-only
                    vector_ids.add(fts_id)

    if not chunks:
        print("No relevant documents found.")
        return

    # Optionally sort by date for timeline queries
    if getattr(args, 'sort_by_date', False):
        combined = list(zip(chunks, metadatas, distances))
        combined.sort(key=lambda x: x[1].get("date", "") or "")
        chunks, metadatas, distances = zip(*combined) if combined else ([], [], [])

    # Look up markdown paths once so citations can resolve bbox regions
    md_paths: Dict[str, str] = {}
    cited_doc_ids = {m.get("document_id") for m in metadatas if m.get("document_id")}
    if cited_doc_ids:
        try:
            _db = get_db(Path(args.db))
            qmarks = ",".join("?" * len(cited_doc_ids))
            for row in _db.conn.execute(
                f"SELECT id, markdown_path FROM documents WHERE id IN ({qmarks})",
                list(cited_doc_ids),
            ):
                if row["markdown_path"]:
                    md_paths[row["id"]] = row["markdown_path"]
            _db.close()
        except Exception:
            pass

    context_parts = []
    sources = []
    for i, (chunk, meta, dist) in enumerate(zip(chunks, metadatas, distances)):
        context_parts.append(f"[Source {i+1}] {chunk}")
        source_info = f"  - {meta.get('document_name', 'Unknown')}"
        if meta.get("date"):
            source_info += f" ({meta['date']})"
        # Page numbers
        page_start = meta.get("page_start")
        page_end = meta.get("page_end")
        if page_start and page_end:
            if page_start == page_end:
                source_info += f" p.{page_start}"
            else:
                source_info += f" pp.{page_start}-{page_end}"
        # Rich metadata
        details = []
        if meta.get("kind"):
            details.append(meta["kind"])
        if meta.get("submitter"):
            details.append(f"by {meta['submitter']}")
        if meta.get("application_types"):
            details.append(meta["application_types"])
        if meta.get("commodities"):
            details.append(meta["commodities"])
        if details:
            source_info += f" [{'; '.join(details)}]"
        if dist is not None:
            source_info += f" (relevance: {1-dist:.2f})"
        doc_id = meta.get('document_id', '')
        if doc_id:
            source_info += f"\n    https://apps.cer-rec.gc.ca/REGDOCS/Item/View/{doc_id}"
        # Pixel-level provenance from the bbox sidecar (content chunks only)
        if doc_id and meta.get("chunk_index", -1) >= 0:
            regions = resolve_chunk_regions(
                md_paths.get(doc_id), chunk, meta.get("page_start"), meta.get("page_end"))
            if regions:
                r = regions[0]
                b = r.get("bbox") or [0, 0, 0, 0]
                extra = f" (+{len(regions)-1} more)" if len(regions) > 1 else ""
                source_info += (f"\n    region: p.{r['page']} "
                                f"bbox({b[0]:.0f},{b[1]:.0f})→({b[2]:.0f},{b[3]:.0f}){extra}")
        sources.append(source_info)

    context = "\n\n".join(context_parts)

    # Build the prompt
    system_prompt = (
        "You are an expert analyst of Canada Energy Regulator (CER) regulatory documents. "
        "Use ONLY the provided context to answer. Each source includes metadata such as company, "
        "project, filing number, date, document type, application type, commodity, and role.\n\n"
        "When answering:\n"
        "- For timeline questions: organize information chronologically by date. Note patterns "
        "in filing frequency, gaps between filings, or progression of application stages.\n"
        "- For comparative questions: compare companies/projects side-by-side on dimensions like "
        "number of filings, document types filed, conditions imposed, commodities involved, "
        "or application types used.\n"
        "- For 'why faster/slower' questions: look at differences in number of documents filed, "
        "document complexity (page counts), conditions or information requests, and regulatory roles involved.\n"
        "- Always cite which source(s) you used by number.\n"
        "- If the context doesn't contain enough information to answer, say what's missing and "
        "suggest how the user could refine their query (e.g., filter by company or date range)."
    )

    # When sorted chronologically, add a timeline presentation hint
    if getattr(args, 'sort_by_date', False):
        system_prompt += (
            "\n\nIMPORTANT: The sources below are sorted in chronological order by date. "
            "Present your answer as a timeline, listing events/filings in date order. "
            "Use date headings or bullet points with dates to make the chronological "
            "progression clear. Highlight any notable gaps, accelerations, or turning points."
        )

    user_prompt = f"""Context from CER REGDOCS documents:

{context}

---

Question: {question}

Answer based on the context above:"""

    # Query Ollama
    print(f"\nSearching {len(chunks)} relevant passages...\n")

    # Show spinner while waiting for first token
    import threading
    spinner_active = True
    def spinner():
        chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        i = 0
        while spinner_active:
            sys.stdout.write(f"\r{chars[i % len(chars)]} Thinking...")
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1
        sys.stdout.write("\r" + " " * 20 + "\r")
        sys.stdout.flush()

    spin_thread = threading.Thread(target=spinner, daemon=True)
    spin_thread.start()

    try:
        response = ollama_client.chat(
            model=args.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=True,
            keep_alive="5m",
        )

        # Stream the response
        first_token = True
        for chunk in response:
            if first_token:
                spinner_active = False
                spin_thread.join()
                first_token = False
            print(chunk["message"]["content"], end="", flush=True)
        print()

    except Exception as e:
        spinner_active = False
        spin_thread.join()
        logging.error(f"LLM query failed: {e}")
        sys.exit(1)

    # Show relevant passages if requested
    if getattr(args, 'show_passages', False):
        print(f"\n{'─' * 50}")
        print("Key Passages:")
        for i, (chunk, meta, dist) in enumerate(zip(chunks[:5], metadatas[:5], distances[:5])):
            # Strip the metadata header (everything before the first double newline)
            text = chunk.split('\n\n', 1)[-1] if '\n\n' in chunk else chunk
            # Trim to ~200 chars at word boundary
            if len(text) > 200:
                text = text[:200].rsplit(' ', 1)[0] + '...'
            relevance = f"{1-dist:.0%}" if dist is not None else '?'
            page_info = ""
            ps = meta.get('page_start')
            pe = meta.get('page_end')
            if ps and pe:
                page_info = f" (p.{ps})" if ps == pe else f" (pp.{ps}-{pe})"
            print(f"  [{relevance}]{page_info} \"{text}\"")
        print()

    # Confidence assessment
    if distances:
        avg_relevance = 1 - (sum(d for d in distances if d is not None) / len([d for d in distances if d is not None]))
        low_relevance_count = sum(1 for d in distances if d is not None and (1 - d) < 0.3)
        if avg_relevance < 0.3:
            print(f"\n⚠️  Low confidence: average relevance is {avg_relevance:.2f} (below 0.30). Results may not be relevant to your question.")
        elif low_relevance_count > len(distances) * 0.5:
            print(f"\n⚠️  Mixed confidence: {low_relevance_count}/{len(distances)} sources have low relevance. Consider narrowing your query with filters.")
        if len(chunks) < 3:
            print(f"\n⚠️  Limited data: only {len(chunks)} matching passages found. Results may be incomplete.")

    # Print sources
    print(f"\n{'─' * 50}")
    print("Sources:")
    for source in sources:
        print(source)
    print()


# ===========================================================================
# SUMMARIZE — Structured extraction via LLM
# ===========================================================================

def run_summarize(args) -> None:
    """Query indexed documents and extract structured data via LLM."""
    import chromadb
    import ollama as ollama_client

    chroma_dir = Path(args.chroma_dir)
    if not chroma_dir.exists():
        logging.error(f"ChromaDB not found at {chroma_dir}. Run 'regdocs.py index' first.")
        sys.exit(1)

    topic = " ".join(args.topic)
    if not topic.strip():
        logging.error("Please provide a topic to summarize.")
        sys.exit(1)

    # Connect to ChromaDB
    client = chromadb.PersistentClient(path=str(chroma_dir))
    try:
        collection = client.get_collection(name=CHROMA_COLLECTION)
    except Exception:
        logging.error(f"Collection '{CHROMA_COLLECTION}' not found. Run 'regdocs.py index' first.")
        sys.exit(1)

    # Embed the topic
    try:
        response = ollama_client.embed(model=args.embed_model, input=topic)
        query_embedding = response["embeddings"][0]
    except Exception as e:
        logging.error(f"Failed to embed topic: {e}")
        sys.exit(1)

    # Build where filter for targeted queries
    where_clauses = []
    where_document = None
    if hasattr(args, 'company') and args.company:
        where_clauses.append({"company": args.company})
    if hasattr(args, 'project') and args.project:
        where_clauses.append({"project": args.project})
    if hasattr(args, 'filing') and args.filing:
        where_clauses.append({"filing_number": args.filing})
    if hasattr(args, 'application_type') and args.application_type:
        where_document = {"$contains": args.application_type}
    if hasattr(args, 'commodity') and args.commodity:
        doc_clause = {"$contains": args.commodity}
        if where_document:
            where_document = {"$and": [where_document, doc_clause]}
        else:
            where_document = doc_clause

    # Build final filter objects
    where_filter = None
    if len(where_clauses) == 1:
        where_filter = where_clauses[0]
    elif len(where_clauses) > 1:
        where_filter = {"$and": where_clauses}

    query_kwargs = {
        "query_embeddings": [query_embedding],
        "n_results": args.top_k,
    }
    if where_filter:
        query_kwargs["where"] = where_filter
        logging.info(f"Filtering by metadata: {where_filter}")
    if where_document:
        query_kwargs["where_document"] = where_document
        logging.info(f"Filtering by document content: {where_document}")

    results = collection.query(**query_kwargs)

    if not results["documents"] or not results["documents"][0]:
        print("No relevant documents found.")
        return

    chunks = results["documents"][0]
    metadatas = results["metadatas"][0]

    context_parts = []
    for i, (chunk, meta) in enumerate(zip(chunks, metadatas)):
        context_parts.append(f"[Source {i+1}] {chunk}")

    context = "\n\n".join(context_parts)

    # Build the structured extraction prompt
    system_prompt = (
        "You are an expert analyst of Canada Energy Regulator (CER) regulatory documents. "
        "Your task is to extract structured information from the provided context and present it "
        "as a table.\n\n"
        "For each distinct document or filing mentioned in the context, extract the following fields:\n"
        "- Filing Number: The filing/application reference number (e.g., OF-Fac-Oil-T260-2013-03)\n"
        "- Company: The company or organization that submitted or is subject to the filing\n"
        "- Date: The most relevant date (filing date, decision date, or document date)\n"
        "- Document Type: The type of document (e.g., Application, Order, Decision, Letter, Condition Compliance)\n"
        "- Application Type: The regulatory application type (e.g., Section 52, Section 58, Detailed Route Hearing)\n"
        "- Key Conditions/Decisions: Brief summary of conditions imposed or decisions made\n"
        "- Status: One of: pending, approved, denied, complied, or unknown if not determinable\n"
        "- Notable Dates: Any important dates mentioned (application date, hearing date, order date)\n\n"
        "IMPORTANT FORMATTING RULES:\n"
        "- Present the results as a Markdown table with these exact column headers:\n"
        "  | Filing Number | Company | Date | Document Type | Application Type | Key Conditions/Decisions | Status | Notable Dates |\n"
        "- If a field cannot be determined from the context, use 'N/A'\n"
        "- Each row should represent a distinct document or filing\n"
        "- Deduplicate: if the same filing appears in multiple sources, combine the information into one row\n"
        "- Sort rows chronologically by date when possible\n"
        "- Keep 'Key Conditions/Decisions' concise (one sentence max)\n"
        "- Use ONLY information from the provided context. Do not infer or fabricate data.\n"
    )

    user_prompt = f"""Context from CER REGDOCS documents:

{context}

---

Topic: {topic}

Extract structured information from the context above and present it as a table:"""

    # Query Ollama
    print(f"\nAnalyzing {len(chunks)} relevant passages...\n")

    # Show spinner while waiting for first token
    import threading
    spinner_active = True
    def spinner():
        chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        i = 0
        while spinner_active:
            sys.stdout.write(f"\r{chars[i % len(chars)]} Extracting structured data...")
            sys.stdout.flush()
            time.sleep(0.1)
            i += 1
        sys.stdout.write("\r" + " " * 40 + "\r")
        sys.stdout.flush()

    spin_thread = threading.Thread(target=spinner, daemon=True)
    spin_thread.start()

    try:
        response = ollama_client.chat(
            model=args.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=False,
            options={"num_ctx": 8192},
            keep_alive="5m",
        )
        spinner_active = False
        spin_thread.join()
        llm_output = response["message"]["content"]
    except Exception as e:
        spinner_active = False
        spin_thread.join()
        logging.error(f"LLM query failed (is Ollama running? Is the model pulled?): {e}")
        sys.exit(1)

    # Output as CSV if requested
    if args.csv:
        _summarize_to_csv(llm_output)
    else:
        print(llm_output)


def _summarize_to_csv(llm_output: str) -> None:
    """Parse the Markdown table from LLM output and print as CSV."""
    import csv
    import io

    lines = llm_output.strip().split("\n")
    # Find table lines (start with |)
    table_lines = [l for l in lines if l.strip().startswith("|")]

    if len(table_lines) < 2:
        # No table found, just print raw output
        print(llm_output)
        return

    # Parse header
    header = [cell.strip() for cell in table_lines[0].split("|") if cell.strip()]

    # Skip separator line (contains ---)
    data_lines = []
    for line in table_lines[1:]:
        cells = [cell.strip() for cell in line.split("|") if cell.strip()]
        # Skip separator rows
        if cells and all(set(c) <= {'-', ':', ' '} for c in cells):
            continue
        if cells:
            data_lines.append(cells)

    # Write CSV to stdout
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in data_lines:
        # Pad row if it has fewer cells than header
        while len(row) < len(header):
            row.append("N/A")
        writer.writerow(row[:len(header)])

    print(output.getvalue(), end="")


# ===========================================================================
# TRENDS — Analytics and duration estimation from metadata
# ===========================================================================

def run_trends(args) -> None:
    """Analyze filing trends, durations, and patterns from metadata.

    This command works purely from SQLite metadata — no LLM or ChromaDB needed.
    It computes statistics across filings to identify patterns useful for
    predicting how long a new filing might take.
    """
    db = get_db(Path(args.db))

    # Collect all filings with their metadata
    rows = db.conn.execute(
        """SELECT id, name, metadata FROM documents
           WHERE metadata IS NOT NULL
             AND json_extract(metadata, '$.filing_number') IS NOT NULL
             AND json_extract(metadata, '$.filing_number') != ''"""
    ).fetchall()

    if not rows:
        print("No documents with filing numbers found.")
        db.close()
        return

    # Group by filing
    filings: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        fn = meta.get("filing_number", "")
        if fn:
            meta["_doc_id"] = row["id"]
            meta["_doc_name"] = row["name"]
            filings.setdefault(fn, []).append(meta)

    # Compute per-filing metrics
    filing_metrics: List[Dict[str, Any]] = []
    for fn, docs in filings.items():
        dates = sorted([d["date"] for d in docs if d.get("date")])
        if not dates:
            continue

        companies = set(d.get("company", "") for d in docs if d.get("company"))
        app_types = set()
        doc_types = set()
        commodities = set()
        roles = set()
        has_order = False
        has_application = False
        has_ir = False  # information request
        has_compliance = False

        for d in docs:
            for at in (d.get("application_types") or []):
                app_types.add(at)
            for dt in (d.get("document_types") or []):
                doc_types.add(dt)
                if "Order" in dt:
                    has_order = True
                if "Application" in dt:
                    has_application = True
                if "Information Request" in dt:
                    has_ir = True
                if "Compliance" in dt:
                    has_compliance = True
            for c in (d.get("commodities") or []):
                commodities.add(c)
            for r in (d.get("roles") or []):
                roles.add(r)

        date_start = dates[0]
        date_end = dates[-1]

        # Duration in days
        try:
            from datetime import datetime as _dt
            d1 = _dt.strptime(date_start, "%Y-%m-%d")
            d2 = _dt.strptime(date_end, "%Y-%m-%d")
            duration_days = (d2 - d1).days
        except (ValueError, TypeError):
            duration_days = 0

        filing_metrics.append({
            "filing_number": fn,
            "company": ", ".join(sorted(companies)) if companies else "",
            "date_start": date_start,
            "date_end": date_end,
            "duration_days": duration_days,
            "document_count": len(docs),
            "application_types": sorted(app_types),
            "commodities": sorted(commodities),
            "document_types": sorted(doc_types),
            "roles": sorted(roles),
            "has_order": has_order,
            "has_application": has_application,
            "has_ir": has_ir,
            "has_compliance": has_compliance,
            "role_count": len(roles),
        })

    db.close()

    # Apply filters
    if args.company:
        filing_metrics = [f for f in filing_metrics if args.company.lower() in f["company"].lower()]
    if args.application_type:
        filing_metrics = [f for f in filing_metrics
                         if any(args.application_type.lower() in at.lower() for at in f["application_types"])]
    if args.commodity:
        filing_metrics = [f for f in filing_metrics
                         if any(args.commodity.lower() in c.lower() for c in f["commodities"])]
    if args.document_type:
        filing_metrics = [f for f in filing_metrics
                         if any(args.document_type.lower() in dt.lower() for dt in f["document_types"])]

    if not filing_metrics:
        print("No filings match the specified filters.")
        return

    # Only analyze filings with meaningful duration (>0 days)
    completed = [f for f in filing_metrics if f["duration_days"] > 0]
    with_orders = [f for f in completed if f["has_order"]]

    # Print report
    print(f"\n{'═' * 70}")
    print(f"  REGDOCS FILING TRENDS ANALYSIS")
    print(f"{'═' * 70}")
    print(f"\n  Total filings analyzed: {len(filing_metrics)}")
    print(f"  Filings with applications: {len(completed)}")
    print(f"  Filings with orders issued: {len(with_orders)}")

    if completed:
        durations = [f["duration_days"] for f in completed]
        doc_counts = [f["document_count"] for f in completed]
        print(f"\n{'─' * 70}")
        print(f"  DURATION STATISTICS (application → last document)")
        print(f"{'─' * 70}")
        print(f"  Mean duration:   {sum(durations) / len(durations):.0f} days ({sum(durations) / len(durations) / 30:.1f} months)")
        print(f"  Median duration: {sorted(durations)[len(durations)//2]} days")
        print(f"  Shortest:        {min(durations)} days")
        print(f"  Longest:         {max(durations)} days")
        print(f"  Mean documents:  {sum(doc_counts) / len(doc_counts):.1f} docs per filing")

    # Duration by application type
    print(f"\n{'─' * 70}")
    print(f"  DURATION BY APPLICATION TYPE")
    print(f"{'─' * 70}")

    by_app_type: Dict[str, List[int]] = {}
    for f in completed:
        for at in f["application_types"]:
            by_app_type.setdefault(at, []).append(f["duration_days"])

    app_type_stats = []
    for at, durs in sorted(by_app_type.items()):
        if len(durs) >= 3:  # need at least 3 data points
            avg = sum(durs) / len(durs)
            med = sorted(durs)[len(durs) // 2]
            app_type_stats.append((at, len(durs), avg, med))

    app_type_stats.sort(key=lambda x: -x[2])  # sort by avg duration desc
    print(f"\n  {'Application Type':<50} {'Count':>5} {'Avg Days':>9} {'Median':>7}")
    print(f"  {'─' * 50} {'─' * 5} {'─' * 9} {'─' * 7}")
    for at, cnt, avg, med in app_type_stats[:20]:
        at_display = at[:48] if len(at) > 48 else at
        print(f"  {at_display:<50} {cnt:>5} {avg:>9.0f} {med:>7}")

    # Duration by commodity
    print(f"\n{'─' * 70}")
    print(f"  DURATION BY COMMODITY")
    print(f"{'─' * 70}")

    by_commodity: Dict[str, List[int]] = {}
    for f in completed:
        for c in f["commodities"]:
            by_commodity.setdefault(c, []).append(f["duration_days"])

    print(f"\n  {'Commodity':<20} {'Count':>5} {'Avg Days':>9} {'Median':>7}")
    print(f"  {'─' * 20} {'─' * 5} {'─' * 9} {'─' * 7}")
    for c, durs in sorted(by_commodity.items()):
        if len(durs) >= 3:
            avg = sum(durs) / len(durs)
            med = sorted(durs)[len(durs) // 2]
            print(f"  {c:<20} {len(durs):>5} {avg:>9.0f} {med:>7}")

    # Complexity indicators
    print(f"\n{'─' * 70}")
    print(f"  COMPLEXITY INDICATORS (correlation with duration)")
    print(f"{'─' * 70}")

    # IRs vs no IRs
    with_ir = [f for f in completed if f["has_ir"]]
    without_ir = [f for f in completed if not f["has_ir"]]
    if with_ir and without_ir:
        avg_with = sum(f["duration_days"] for f in with_ir) / len(with_ir)
        avg_without = sum(f["duration_days"] for f in without_ir) / len(without_ir)
        print(f"\n  Information Requests:")
        print(f"    With IRs:    {avg_with:.0f} days avg ({len(with_ir)} filings)")
        print(f"    Without IRs: {avg_without:.0f} days avg ({len(without_ir)} filings)")
        if avg_without > 0:
            print(f"    IRs add:     +{((avg_with - avg_without) / avg_without * 100):.0f}% to duration")

    # Multiple roles (intervenors)
    multi_role = [f for f in completed if f["role_count"] >= 3]
    few_role = [f for f in completed if f["role_count"] < 3]
    if multi_role and few_role:
        avg_multi = sum(f["duration_days"] for f in multi_role) / len(multi_role)
        avg_few = sum(f["duration_days"] for f in few_role) / len(few_role)
        print(f"\n  Participant diversity (3+ roles vs <3):")
        print(f"    3+ roles:    {avg_multi:.0f} days avg ({len(multi_role)} filings)")
        print(f"    <3 roles:    {avg_few:.0f} days avg ({len(few_role)} filings)")

    # High doc count vs low
    if completed:
        median_docs = sorted(doc_counts)[len(doc_counts) // 2]
        high_docs = [f for f in completed if f["document_count"] > median_docs]
        low_docs = [f for f in completed if f["document_count"] <= median_docs]
        if high_docs and low_docs:
            avg_high = sum(f["duration_days"] for f in high_docs) / len(high_docs)
            avg_low = sum(f["duration_days"] for f in low_docs) / len(low_docs)
            print(f"\n  Document volume (>{median_docs} docs vs <={median_docs}):")
            print(f"    High volume: {avg_high:.0f} days avg ({len(high_docs)} filings, avg {sum(f['document_count'] for f in high_docs)/len(high_docs):.0f} docs)")
            print(f"    Low volume:  {avg_low:.0f} days avg ({len(low_docs)} filings, avg {sum(f['document_count'] for f in low_docs)/len(low_docs):.0f} docs)")

    # Top 10 longest filings
    print(f"\n{'─' * 70}")
    print(f"  LONGEST FILINGS (top 10)")
    print(f"{'─' * 70}")
    longest = sorted(completed, key=lambda f: -f["duration_days"])[:10]
    print(f"\n  {'Filing':<15} {'Company':<30} {'Days':>5} {'Docs':>4} {'Type'}")
    print(f"  {'─' * 15} {'─' * 30} {'─' * 5} {'─' * 4} {'─' * 30}")
    for f in longest:
        company_display = f["company"][:28] if len(f["company"]) > 28 else f["company"]
        at_display = f["application_types"][0][:28] if f["application_types"] else ""
        print(f"  {f['filing_number']:<15} {company_display:<30} {f['duration_days']:>5} {f['document_count']:>4} {at_display}")

    # Estimation section
    if args.estimate:
        print(f"\n{'═' * 70}")
        print(f"  DURATION ESTIMATE FOR NEW FILING")
        print(f"{'═' * 70}")

        # Find comparable filings
        comparable = completed
        est_factors = []

        if args.application_type:
            matches = [f for f in comparable
                       if any(args.application_type.lower() in at.lower() for at in f["application_types"])]
            if matches:
                comparable = matches
                est_factors.append(f"Application type: {args.application_type}")

        if args.commodity:
            matches = [f for f in comparable
                       if any(args.commodity.lower() in c.lower() for c in f["commodities"])]
            if matches:
                comparable = matches
                est_factors.append(f"Commodity: {args.commodity}")

        if args.document_type:
            matches = [f for f in comparable
                       if any(args.document_type.lower() in dt.lower() for dt in f["document_types"])]
            if matches:
                comparable = matches
                est_factors.append(f"Document type: {args.document_type}")

        if args.company:
            matches = [f for f in comparable if args.company.lower() in f["company"].lower()]
            if matches:
                comparable = matches
                est_factors.append(f"Company: {args.company}")

        if comparable:
            durs = sorted(f["duration_days"] for f in comparable)
            avg_dur = sum(durs) / len(durs)
            med_dur = durs[len(durs) // 2]
            p25 = durs[len(durs) // 4]
            p75 = durs[3 * len(durs) // 4]

            print(f"\n  Based on {len(comparable)} comparable filings:")
            if est_factors:
                for factor in est_factors:
                    print(f"    • {factor}")
            print(f"\n  Estimated duration:")
            print(f"    Optimistic (25th pctile):  {p25} days ({p25/30:.1f} months)")
            print(f"    Typical (median):          {med_dur} days ({med_dur/30:.1f} months)")
            print(f"    Average:                   {avg_dur:.0f} days ({avg_dur/30:.1f} months)")
            print(f"    Pessimistic (75th pctile): {p75} days ({p75/30:.1f} months)")
            print(f"    Worst case (max):          {max(durs)} days ({max(durs)/30:.1f} months)")

            # Adjustment factors
            print(f"\n  Adjustment factors (from historical data):")
            if with_ir and without_ir:
                ir_multiplier = avg_with / avg_without if avg_without > 0 else 1.0
                print(f"    If IRs expected:           ×{ir_multiplier:.2f}")
            if multi_role and few_role:
                role_multiplier = avg_multi / avg_few if avg_few > 0 else 1.0
                print(f"    If 3+ participant roles:   ×{role_multiplier:.2f}")
        else:
            print("\n  Not enough comparable filings to estimate.")

    print()


# ===========================================================================
# COMPLIANCE — Gap detection for filings with Orders but no Compliance docs
# ===========================================================================

def run_compliance(args) -> None:
    """Detect filings with Orders but missing Compliance documents."""
    db = get_db(Path(args.db))

    # Only load documents that could have Orders or Compliance in their doc_types
    rows = db.conn.execute(
        """SELECT id, name, metadata FROM documents
           WHERE metadata IS NOT NULL
             AND json_extract(metadata, '$.filing_number') IS NOT NULL
             AND json_extract(metadata, '$.filing_number') != ''
             AND (json_extract(metadata, '$.document_types') LIKE '%Order%'
               OR json_extract(metadata, '$.document_types') LIKE '%Compliance%')"""
    ).fetchall()

    filings: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        fn = meta.get("filing_number", "")
        if fn:
            filings.setdefault(fn, []).append(meta)

    # Find filings with Orders but no Compliance
    gaps = []
    for fn, docs in filings.items():
        doc_types = set()
        for d in docs:
            for dt in (d.get("document_types") or []):
                doc_types.add(dt)

        has_order = any("Order" in dt for dt in doc_types)
        has_compliance = any("Compliance" in dt for dt in doc_types)

        if has_order and not has_compliance:
            companies = set(d.get("company", "") for d in docs if d.get("company"))
            dates = sorted(d.get("date", "") for d in docs if d.get("date"))
            app_types = set()
            for d in docs:
                for at in (d.get("application_types") or []):
                    app_types.add(at)
            gaps.append({
                "filing_number": fn,
                "company": ", ".join(sorted(companies)),
                "date_range": f"{dates[0]} to {dates[-1]}" if dates else "",
                "doc_count": len(docs),
                "doc_types": sorted(doc_types),
                "application_types": sorted(app_types),
            })

    db.close()

    # Apply filters
    if args.company:
        gaps = [g for g in gaps if args.company.lower() in g["company"].lower()]

    # Sort by date descending
    gaps.sort(key=lambda g: g["date_range"], reverse=True)

    print(f"\n{'═' * 70}")
    print(f"  COMPLIANCE GAP REPORT")
    print(f"  Filings with Orders but NO Compliance documents filed")
    print(f"{'═' * 70}")
    print(f"\n  Found {len(gaps)} filings with potential compliance gaps\n")

    if not gaps:
        print("  No compliance gaps detected.")
        return

    print(f"  {'Filing':<15} {'Company':<30} {'Date Range':<25} {'Docs':>4}")
    print(f"  {'─' * 15} {'─' * 30} {'─' * 25} {'─' * 4}")

    display_count = min(len(gaps), 30) if not args.all else len(gaps)
    for g in gaps[:display_count]:
        company_display = g["company"][:28] if len(g["company"]) > 28 else g["company"]
        print(f"  {g['filing_number']:<15} {company_display:<30} {g['date_range']:<25} {g['doc_count']:>4}")

    if len(gaps) > display_count:
        print(f"\n  ... and {len(gaps) - display_count} more (use --all to show all)")

    print()


# ===========================================================================
# VERIFY — Cross-check converted Markdown against the PDF text layer
# ===========================================================================

def _extract_numbers(text: str) -> set:
    """Extract normalized numeric tokens (2+ significant digits) from text.

    Numbers are the highest-risk content in document extraction — a dropped or
    corrupted figure is invisible in prose but changes the meaning entirely.
    Single digits are excluded (too noisy: page numbers, list markers).
    """
    out = set()
    for n in re.findall(r"\d[\d,]*(?:\.\d+)?", text):
        n = n.replace(",", "").rstrip(".")
        if len(n.replace(".", "")) >= 2:
            out.add(n)
    return out


def run_verify(args) -> None:
    """Verify conversion fidelity by comparing each converted Markdown against a
    second, independent extraction of the same PDF's text layer (pypdfium2).

    pypdfium2 does deterministic text extraction — it cannot hallucinate or
    restructure content. If numbers present in the text layer are missing from
    the Markdown, the conversion lost data. Each document gets a fidelity score
    (numeric recall) stored in metadata; the report lists the worst offenders.

    Scanned PDFs have no text layer to compare against and are reported as
    unverifiable — for those, the OCR quality_score is the only signal.
    """
    import pypdfium2 as pdfium

    db = get_db(Path(args.db))
    rows = db.conn.execute(
        """SELECT id, name, file_path, markdown_path, metadata FROM documents
           WHERE status = 'CONVERTED'
             AND markdown_path IS NOT NULL
             AND file_path LIKE '%.pdf'"""
    ).fetchall()

    docs = [dict(r) for r in rows]
    if not docs:
        print("No converted PDF documents to verify. Run 'regdocs.py convert' first.")
        db.close()
        return

    if args.sample and args.sample < len(docs):
        docs = random.sample(docs, args.sample)

    print(f"\nVerifying {len(docs)} converted document(s) against their PDF text layers...\n")

    results = []
    unverifiable = 0
    errors = 0
    for doc in tqdm(docs, desc="Verifying", unit="doc"):
        md_path = Path(doc["markdown_path"])
        pdf_path = Path(doc["file_path"])
        if not md_path.exists() or not pdf_path.exists():
            errors += 1
            continue

        try:
            pdf = pdfium.PdfDocument(str(pdf_path))
            page_texts = []
            for page in pdf:
                textpage = page.get_textpage()
                page_texts.append(textpage.get_text_range() or "")
                textpage.close()
                page.close()
            pdf.close()
            pdf_text = "\n".join(page_texts)
        except Exception:
            errors += 1
            continue

        # No meaningful text layer = scanned document; nothing to compare against
        if len(pdf_text.strip()) < 200:
            unverifiable += 1
            continue

        md_text = re.sub(r"<!--\s*page:\d+\s*-->", "",
                         md_path.read_text(encoding="utf-8", errors="replace"))

        pdf_nums = _extract_numbers(pdf_text)
        md_nums = _extract_numbers(md_text)
        fidelity = len(pdf_nums & md_nums) / len(pdf_nums) if pdf_nums else 1.0
        length_ratio = len(md_text) / max(len(pdf_text), 1)

        missing_nums = pdf_nums - md_nums
        meta = json.loads(doc["metadata"]) if doc["metadata"] else {}
        meta["verify"] = {
            "fidelity": round(fidelity, 3),
            "length_ratio": round(length_ratio, 3),
            "pdf_numbers": len(pdf_nums),
            "missing_numbers": len(missing_nums),
            "missing_sample": sorted(missing_nums)[:8],
        }
        db.update_document(doc["id"], metadata=json.dumps(meta, ensure_ascii=False))

        results.append({
            "id": doc["id"], "name": doc["name"], "fidelity": fidelity,
            "length_ratio": length_ratio, "pdf_numbers": len(pdf_nums),
            "missing": len(missing_nums),
            "missing_sample": sorted(missing_nums)[:8],
        })

    db.close()

    if not results:
        print("Nothing could be verified (all scanned or errored).")
        return

    fidelities = sorted(r["fidelity"] for r in results)
    n = len(fidelities)
    below = [r for r in results if r["fidelity"] < args.min_fidelity]

    print(f"\n{'═' * 70}")
    print(f"  CONVERSION FIDELITY REPORT")
    print(f"  (numeric recall: Markdown vs. PDF text layer)")
    print(f"{'═' * 70}")
    print(f"\n  Verified:      {n} documents")
    print(f"  Unverifiable:  {unverifiable} (scanned — no text layer; rely on quality_score)")
    if errors:
        print(f"  Errors:        {errors} (missing/corrupt files)")
    print(f"\n  Fidelity distribution:")
    print(f"    Median:  {fidelities[n // 2]:.3f}")
    print(f"    P10:     {fidelities[n // 10]:.3f}")
    print(f"    Min:     {fidelities[0]:.3f}")
    print(f"    ≥0.99:   {sum(1 for f in fidelities if f >= 0.99)} docs "
          f"({sum(1 for f in fidelities if f >= 0.99) / n * 100:.0f}%)")

    if below:
        below.sort(key=lambda r: r["fidelity"])
        print(f"\n{'─' * 70}")
        print(f"  DOCUMENTS BELOW --min-fidelity {args.min_fidelity} ({len(below)})")
        print(f"{'─' * 70}")
        print(f"\n  {'Fidelity':>8} {'Missing':>8} {'of':>6}  Document")
        print(f"  {'─' * 8} {'─' * 8} {'─' * 6}  {'─' * 40}")
        show = below if args.all else below[:25]
        for r in show:
            name = r["name"][:55]
            print(f"  {r['fidelity']:>8.3f} {r['missing']:>8} {r['pdf_numbers']:>6}  {name}")
            if r["missing_sample"]:
                print(f"           missing e.g.: {', '.join(r['missing_sample'])}")
        if len(below) > len(show):
            print(f"\n  ... and {len(below) - len(show)} more (use --all)")
        print(f"\n  Interpreting the flags: numbers from letterheads, phone/fax lines, and page")
        print(f"  footers are excluded from Markdown BY DESIGN (Docling drops page furniture).")
        print(f"  Missing phone-number or address fragments are false alarms; missing amounts,")
        print(f"  measurements, or condition numbers are real losses worth re-converting.")
        print(f"\n  To re-convert the flagged documents:")
        print(f"    UPDATE documents SET status='DOWNLOADED', markdown_path=NULL WHERE id IN (...);")
        print(f"    python regdocs.py convert")
    else:
        print(f"\n  All verified documents meet the fidelity threshold. ✓")

    print()


# ===========================================================================
# PCMR — Structured findings extraction and trend analysis for
#        Post Construction (Environmental) Monitoring Reports
# ===========================================================================

PCMR_CATEGORIES = [
    "Erosion and Sediment Control",
    "Vegetation and Reclamation",
    "Wildlife and Wetlands",
    "Soil",
    "Drainage and Watercourse Crossings",
    "Landowner and Access",
    "Noise",
    "Other",
]

PCMR_CONTENT_CAP = 32000  # chars kept from each report before sending to the LLM


def _pcmr_extract_content(markdown_path: str) -> Optional[str]:
    """Read a markdown file, stripping page markers and capping length.

    Keeps the head and tail of long documents (methodology tends to be at the
    start, findings/conclusions at the end) rather than truncating from the top.
    """
    try:
        text = Path(markdown_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    text = re.sub(r"<!--\s*page:\d+\s*-->", "", text).strip()
    if len(text) <= PCMR_CONTENT_CAP:
        return text
    head = int(PCMR_CONTENT_CAP * 0.6)
    tail = PCMR_CONTENT_CAP - head
    return text[:head] + "\n\n...[TRUNCATED]...\n\n" + text[-tail:]


def _pcmr_analyze(ollama_client, model: str, content: str) -> Optional[Dict[str, Any]]:
    """Run an LLM extraction pass over a report, returning parsed JSON or None.

    Local models occasionally emit malformed JSON (bad escaping) even in
    format="json" mode, especially at higher temperatures. Retry once with a
    lower temperature before giving up, since a single bad token is usually
    the culprit and a fresh sample tends to fix it.
    """
    system_prompt = (
        "You are an expert reviewer of Canada Energy Regulator (CER) post-construction "
        "environmental monitoring reports. Read the report text and extract structured findings "
        "as JSON with this exact shape:\n\n"
        '{\n'
        '  "compliance_status": "Compliant" | "Non-Compliant" | "Partially Compliant" | "Unknown",\n'
        '  "issue_categories": [strings, chosen only from: ' + ", ".join(PCMR_CATEGORIES) + '],\n'
        '  "findings": [\n'
        '    {"category": one of the categories above, "description": short string, '
        '"severity": "Minor" | "Moderate" | "Major", "resolved": true | false}\n'
        '  ],\n'
        '  "summary": "1-3 sentence plain-English summary of the report outcome"\n'
        "}\n\n"
        "Use \"Other\" for issues that don't fit the listed categories. If the report describes "
        "no deficiencies, return an empty findings list and compliance_status \"Compliant\". "
        "Base everything ONLY on the provided text. Output ONLY the JSON object, no commentary. "
        "Make sure every string value is valid JSON — escape internal quotes properly and never "
        "repeat a field's key inside its own string value."
    )
    for temperature in (0.1, 0.0):
        try:
            response = ollama_client.chat(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": content},
                ],
                format="json",
                stream=False,
                options={"num_ctx": 12288, "temperature": temperature},
                keep_alive="5m",
            )
            parsed = json.loads(response["message"]["content"])
            if parsed:
                return parsed
        except Exception:
            continue
    return None


def run_pcmr(args) -> None:
    """Extract structured findings from Post Construction Monitoring Reports and
    aggregate them into trend statistics (issue categories, compliance rate over
    time, companies with recurring findings).

    Works directly from converted Markdown (doesn't require ChromaDB indexing).
    Results are cached in each document's metadata (keyed by content hash) so
    re-running this command only re-analyzes new or changed reports unless --force
    is given.
    """
    import ollama as ollama_client

    db = get_db(Path(args.db))

    rows = db.conn.execute(
        """SELECT id, name, hash, markdown_path, metadata FROM documents
           WHERE markdown_path IS NOT NULL
             AND metadata IS NOT NULL
             AND json_extract(metadata, '$.document_types') LIKE ?""",
        (f"%{args.document_type}%",),
    ).fetchall()

    if not rows:
        print(f"No converted documents found with document type matching '{args.document_type}'.")
        print("Run 'regdocs.py convert' first, or check --document-type spelling.")
        db.close()
        return

    candidates = []
    for row in rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        doc_types = meta.get("document_types") or []
        if not any(args.document_type.lower() in dt.lower() for dt in doc_types):
            continue
        if args.company and args.company.lower() not in (meta.get("company") or "").lower():
            continue
        date = meta.get("date") or ""
        if args.after and date and date < args.after:
            continue
        if args.before and date and date > args.before:
            continue
        candidates.append({"id": row["id"], "name": row["name"], "hash": row["hash"],
                            "markdown_path": row["markdown_path"], "meta": meta})

    if args.limit:
        candidates = candidates[: args.limit]

    if not candidates:
        print("No documents match the given filters.")
        db.close()
        return

    print(f"\nAnalyzing {len(candidates)} document(s) matching '{args.document_type}'...\n")

    results = []
    analyzed, cached, skipped = 0, 0, 0
    for c in tqdm(candidates, desc="Extracting findings", unit="doc"):
        meta = c["meta"]
        cache = meta.get("pcmr_analysis")
        if cache and cache.get("_hash") == c["hash"] and not args.force:
            results.append({**cache, "document_id": c["id"], "document_name": c["name"], "meta": meta})
            cached += 1
            continue

        content = _pcmr_extract_content(c["markdown_path"])
        if not content:
            skipped += 1
            continue

        analysis = _pcmr_analyze(ollama_client, args.llm_model, content)
        if analysis is None:
            skipped += 1
            continue

        analysis["_hash"] = c["hash"]
        meta["pcmr_analysis"] = analysis
        db.update_document(c["id"], metadata=json.dumps(meta, ensure_ascii=False))
        results.append({**analysis, "document_id": c["id"], "document_name": c["name"], "meta": meta})
        analyzed += 1

    db.close()

    if skipped:
        logging.warning(f"Skipped {skipped} document(s) (missing markdown or LLM extraction failed)")

    if not results:
        print("No findings could be extracted.")
        return

    if args.csv:
        import csv
        writer = csv.writer(sys.stdout)
        writer.writerow(["Company", "Project", "Filing", "Date", "Compliance Status",
                          "Issue Categories", "Summary"])
        for r in results:
            m = r["meta"]
            writer.writerow([
                m.get("company", ""), m.get("project", ""), m.get("filing_number", ""),
                m.get("date", ""), r.get("compliance_status", "Unknown"),
                "; ".join(r.get("issue_categories") or []), r.get("summary", ""),
            ])
        return

    # --- Aggregate trends ---
    status_counts: Dict[str, int] = {}
    category_counts: Dict[str, int] = {}
    year_counts: Dict[str, Dict[str, int]] = {}  # year -> {total, non_compliant}
    company_issue_counts: Dict[str, int] = {}
    unresolved_major: List[Dict[str, Any]] = []

    for r in results:
        status = r.get("compliance_status", "Unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

        year = (r["meta"].get("date") or "")[:4] or "Unknown"
        yc = year_counts.setdefault(year, {"total": 0, "non_compliant": 0})
        yc["total"] += 1
        if status in ("Non-Compliant", "Partially Compliant"):
            yc["non_compliant"] += 1

        company = r["meta"].get("company") or "Unknown"
        for cat in (r.get("issue_categories") or []):
            category_counts[cat] = category_counts.get(cat, 0) + 1
            company_issue_counts[company] = company_issue_counts.get(company, 0) + 1

        for f in (r.get("findings") or []):
            if isinstance(f, dict) and f.get("severity") == "Major" and not f.get("resolved", True):
                unresolved_major.append({
                    "company": company, "document_name": r["document_name"],
                    "document_id": r["document_id"], "category": f.get("category", ""),
                    "description": f.get("description", ""),
                })

    print(f"{'═' * 70}")
    print(f"  POST CONSTRUCTION MONITORING REPORT TRENDS")
    print(f"{'═' * 70}")
    print(f"\n  Reports analyzed: {len(results)}  ({analyzed} newly extracted, {cached} cached)")

    print(f"\n{'─' * 70}")
    print(f"  COMPLIANCE STATUS")
    print(f"{'─' * 70}")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        pct = count / len(results) * 100
        print(f"  {status:<25} {count:>5}  ({pct:.0f}%)")

    print(f"\n{'─' * 70}")
    print(f"  ISSUE CATEGORIES (most common findings)")
    print(f"{'─' * 70}")
    print(f"\n  {'Category':<35} {'Count':>5}")
    print(f"  {'─' * 35} {'─' * 5}")
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:<35} {count:>5}")

    if len(year_counts) > 1:
        print(f"\n{'─' * 70}")
        print(f"  COMPLIANCE RATE BY YEAR")
        print(f"{'─' * 70}")
        print(f"\n  {'Year':<8} {'Reports':>8} {'Non-Compliant':>14} {'Rate':>8}")
        print(f"  {'─' * 8} {'─' * 8} {'─' * 14} {'─' * 8}")
        for year in sorted(year_counts):
            yc = year_counts[year]
            rate = yc["non_compliant"] / yc["total"] * 100 if yc["total"] else 0
            print(f"  {year:<8} {yc['total']:>8} {yc['non_compliant']:>14} {rate:>7.0f}%")

    if company_issue_counts:
        print(f"\n{'─' * 70}")
        print(f"  COMPANIES WITH THE MOST FLAGGED ISSUES")
        print(f"{'─' * 70}")
        top_companies = sorted(company_issue_counts.items(), key=lambda x: -x[1])[:10]
        print(f"\n  {'Company':<45} {'Issues':>7}")
        print(f"  {'─' * 45} {'─' * 7}")
        for company, count in top_companies:
            company_display = company[:43] if len(company) > 43 else company
            print(f"  {company_display:<45} {count:>7}")

    if unresolved_major:
        print(f"\n{'─' * 70}")
        print(f"  UNRESOLVED MAJOR FINDINGS ({len(unresolved_major)})")
        print(f"{'─' * 70}\n")
        for item in unresolved_major[:20]:
            print(f"  • [{item['category']}] {item['company']}")
            print(f"    {item['description']}")
            print(f"    https://apps.cer-rec.gc.ca/REGDOCS/Item/View/{item['document_id']}\n")
        if len(unresolved_major) > 20:
            print(f"  ... and {len(unresolved_major) - 20} more")

    print()


# ===========================================================================
# DIFF — Compare two documents to identify changes
# ===========================================================================

def run_diff(args) -> None:
    """Compare two documents to identify what changed."""
    import chromadb
    import ollama as ollama_client

    db = get_db(Path(args.db))

    # Find the two documents
    doc1 = db.get_document(args.doc1)
    doc2 = db.get_document(args.doc2)

    if not doc1:
        print(f"Document not found: {args.doc1}")
        sys.exit(1)
    if not doc2:
        print(f"Document not found: {args.doc2}")
        sys.exit(1)

    # Read markdown content if available
    def get_content(doc):
        mp = doc.get("markdown_path")
        if mp and Path(mp).exists():
            return Path(mp).read_text(encoding="utf-8", errors="replace")[:8000]
        return None

    content1 = get_content(doc1)
    content2 = get_content(doc2)

    if not content1 and not content2:
        # Fall back to ChromaDB chunks
        chroma_dir = Path(args.chroma_dir)
        if not chroma_dir.exists():
            print("No markdown files and no ChromaDB. Run convert and index first.")
            sys.exit(1)
        client = chromadb.PersistentClient(path=str(chroma_dir))
        collection = client.get_collection(name=CHROMA_COLLECTION)

        def get_chunks(doc_id):
            results = collection.get(
                where={"document_id": doc_id},
                include=["documents"],
            )
            if results and results["documents"]:
                return "\n\n".join(results["documents"][:10])
            return None

        content1 = content1 or get_chunks(args.doc1)
        content2 = content2 or get_chunks(args.doc2)

    if not content1:
        print(f"No content available for document {args.doc1}")
        sys.exit(1)
    if not content2:
        print(f"No content available for document {args.doc2}")
        sys.exit(1)

    meta1 = json.loads(doc1["metadata"]) if doc1.get("metadata") else {}
    meta2 = json.loads(doc2["metadata"]) if doc2.get("metadata") else {}

    # Build diff prompt
    system_prompt = (
        "You are a regulatory document analyst. Compare the two documents below and identify:\n"
        "1. What was ADDED in Document B that wasn't in Document A\n"
        "2. What was REMOVED from Document A that's not in Document B\n"
        "3. What was CHANGED between the two versions\n"
        "4. Key implications of these changes for regulatory compliance\n\n"
        "Be specific — cite section numbers, conditions, dates, or requirements that changed."
    )

    user_prompt = f"""Document A: {doc1['name']} ({meta1.get('date', 'unknown date')})
Type: {', '.join(meta1.get('document_types', []))}

{content1[:4000]}

---

Document B: {doc2['name']} ({meta2.get('date', 'unknown date')})
Type: {', '.join(meta2.get('document_types', []))}

{content2[:4000]}

---

What are the key differences between these two documents?"""

    print(f"\nComparing:")
    print(f"  A: {doc1['name']} ({meta1.get('date', '?')})")
    print(f"  B: {doc2['name']} ({meta2.get('date', '?')})")
    print()

    try:
        response = ollama_client.chat(
            model=args.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            stream=True,
            keep_alive="5m",
        )
        for chunk in response:
            print(chunk["message"]["content"], end="", flush=True)
        print()
    except Exception as e:
        logging.error(f"LLM diff failed: {e}")
        sys.exit(1)

    db.close()


# ===========================================================================
# ALL — Run the full pipeline sequentially
# ===========================================================================

async def run_all(args) -> None:
    """Run scout -> download -> convert -> index in sequence."""
    logging.info("Running full pipeline: scout -> download -> convert -> index")

    # Resolve --force shorthand into the specific flags
    if getattr(args, 'force', False):
        args.force_download = True
        args.force_index = True

    # Map split flags for sub-stages that expect args.force
    # Download stage reads args.force
    original_force = getattr(args, 'force', False)
    args.force = getattr(args, 'force_download', False)

    logging.info("=" * 60)
    logging.info("STAGE 1: Scout")
    logging.info("=" * 60)
    await run_scout(args)

    if not getattr(args, 'dry_run', False):
        logging.info("=" * 60)
        logging.info("STAGE 2: Download")
        logging.info("=" * 60)
        await run_download(args)

        # Convert stage uses a different output directory (markdown/ not downloads/)
        download_dir = args.output_dir
        args.output_dir = "markdown"

        logging.info("=" * 60)
        logging.info("STAGE 3: Convert")
        logging.info("=" * 60)
        await run_convert(args)

        # Restore output_dir and set force for index
        args.output_dir = download_dir
        args.force = getattr(args, 'force_index', False)

        logging.info("=" * 60)
        logging.info("STAGE 4: Index")
        logging.info("=" * 60)
        run_index(args)

    # Restore for stats
    args.force = original_force

    logging.info("=" * 60)
    logging.info("Pipeline complete. Final stats:")
    logging.info("=" * 60)
    run_stats(args)


# ===========================================================================
# WATCH — Cron-friendly daily job
# ===========================================================================

async def run_watch(args) -> None:
    """Run a daily update: scout last N days, download, convert, index."""
    from datetime import timedelta

    days = args.days
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    logging.info(f"Watch mode: processing last {days} days ({start_date} to {end_date})")

    # Override args for the pipeline
    args.start_date = start_date
    args.end_date = end_date
    args.dry_run = False
    if not hasattr(args, 'output_dir'):
        args.output_dir = "downloads"
    if not hasattr(args, 'facets'):
        args.facets = "all"
    if not hasattr(args, 'limit'):
        args.limit = None
    if not hasattr(args, 'page_size'):
        args.page_size = 200
    if not hasattr(args, 'concurrency'):
        args.concurrency = 1
    if not hasattr(args, 'min_delay'):
        args.min_delay = 2.0
    if not hasattr(args, 'max_delay'):
        args.max_delay = 4.0
    if not hasattr(args, 'max_retries'):
        args.max_retries = 3
    if not hasattr(args, 'force'):
        args.force = False
    if not hasattr(args, 'include_html'):
        args.include_html = False
    if not hasattr(args, 'chroma_dir'):
        args.chroma_dir = str(CHROMA_DIR)
    if not hasattr(args, 'embed_model'):
        args.embed_model = EMBED_MODEL
    if not hasattr(args, 'chunk_size'):
        args.chunk_size = CHUNK_SIZE
    if not hasattr(args, 'overlap'):
        args.overlap = CHUNK_OVERLAP
    if not hasattr(args, 'min_quality'):
        args.min_quality = 0.0
    if not hasattr(args, 'timeout'):
        args.timeout = 300

    # run_all passes args through to each stage. Download and convert both
    # read args.output_dir but expect different directories. We stash the
    # download dir, then swap in the convert dir before that stage runs.
    # Alternatively, run stages directly here for clarity.
    await run_scout(args)

    # Download stage uses args.output_dir = downloads/
    args.force = False
    await run_download(args)

    # Convert stage uses args.output_dir = markdown/
    args.output_dir = "markdown"
    await run_convert(args)

    # Index stage
    run_index(args)

    run_stats(args)


# ===========================================================================
# EXPORT — CSV dump of documents table
# ===========================================================================

def run_export(args) -> None:
    """Export the documents table to CSV."""
    import csv

    db = get_db(Path(args.db))
    rows = db.conn.execute(
        "SELECT id, name, url, status, file_path, markdown_path, hash, last_error, retry_count, metadata, created_at, updated_at FROM documents ORDER BY created_at"
    ).fetchall()
    db.close()

    output_path = Path(args.output)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "url", "status", "file_path", "markdown_path",
                         "hash", "last_error", "retry_count", "metadata", "created_at", "updated_at"])
        for row in rows:
            writer.writerow(list(row))

    logging.info(f"Exported {len(rows)} documents to {output_path}")


# ===========================================================================
# CLI — Argument parsing and entry point
# ===========================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="regdocs",
        description="CER REGDOCS unified pipeline — scout, download, convert, all in one tool.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default=str(Path(__file__).parent / "regdocs.db"),
                        help="Path to the SQLite database")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")

    subparsers = parser.add_subparsers(dest="command", help="Pipeline stage to run")

    # --- scout ---
    scout_p = subparsers.add_parser("scout", help="Crawl REGDOCS and discover documents",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    scout_p.add_argument("--start-date", default="2026-01-01", help="Start date (YYYY-MM-DD)")
    scout_p.add_argument("--end-date", default="2026-12-31", help="End date (YYYY-MM-DD)")
    scout_p.add_argument("--facets", default="all",
                         help="Facet categories: all, none, or comma-separated list")
    scout_p.add_argument("--limit", type=int, help="Stop after N items")
    scout_p.add_argument("--page-size", type=int, default=200, choices=PAGE_SIZES,
                         help="Results per request")
    scout_p.add_argument("--concurrency", type=int, default=1,
                         help="Max parallel requests")
    scout_p.add_argument("--min-delay", type=float, default=2.0,
                         help="Min politeness delay between requests (seconds)")
    scout_p.add_argument("--max-delay", type=float, default=4.0,
                         help="Max politeness delay between requests (seconds)")
    scout_p.add_argument("--dry-run", action="store_true", help="Crawl but write nothing")

    # --- download ---
    dl_p = subparsers.add_parser("download", help="Download files for discovered documents",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    dl_p.add_argument("--output-dir", default="downloads", help="Directory to save files")
    dl_p.add_argument("--concurrency", type=int, default=1,
                      help="Max parallel downloads")
    dl_p.add_argument("--min-delay", type=float, default=2.0,
                      help="Min politeness delay between requests (seconds)")
    dl_p.add_argument("--max-delay", type=float, default=4.0,
                      help="Max politeness delay between requests (seconds)")
    dl_p.add_argument("--max-retries", type=int, default=3,
                      help="Max retry attempts for failed downloads")
    dl_p.add_argument("--force", action="store_true", help="Re-download existing files")
    dl_p.add_argument("--include-html", action="store_true", help="Also download HTML documents")
    dl_p.add_argument("--dry-run", action="store_true", help="Show what would be downloaded without downloading")

    # --- convert ---
    conv_p = subparsers.add_parser("convert", help="Convert downloaded files to Markdown",
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    conv_p.add_argument("--output-dir", default="markdown", help="Directory to save Markdown files")
    conv_p.add_argument("--max-retries", type=int, default=3,
                        help="Max retry attempts for failed conversions")
    conv_p.add_argument("--timeout", type=int, default=600,
                        help="Timeout per document in seconds (default: 600s = 10 min)")
    conv_p.add_argument("--dry-run", action="store_true", help="Show what would be converted without converting")

    # --- all ---
    all_p = subparsers.add_parser("all", help="Run full pipeline: scout -> download -> convert",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    all_p.add_argument("--start-date", default="2026-01-01", help="Start date (YYYY-MM-DD)")
    all_p.add_argument("--end-date", default="2026-12-31", help="End date (YYYY-MM-DD)")
    all_p.add_argument("--facets", default="all",
                       help="Facet categories: all, none, or comma-separated list")
    all_p.add_argument("--limit", type=int, help="Stop after N items")
    all_p.add_argument("--page-size", type=int, default=200, choices=PAGE_SIZES,
                       help="Results per request")
    all_p.add_argument("--concurrency", type=int, default=1,
                       help="Max parallel operations")
    all_p.add_argument("--min-delay", type=float, default=2.0,
                       help="Min politeness delay between requests (seconds)")
    all_p.add_argument("--max-delay", type=float, default=4.0,
                       help="Max politeness delay between requests (seconds)")
    all_p.add_argument("--max-retries", type=int, default=3,
                       help="Max retry attempts for failures")
    all_p.add_argument("--output-dir", default="downloads", help="Directory for downloaded files")
    all_p.add_argument("--force-download", action="store_true", help="Re-download existing files")
    all_p.add_argument("--force-index", action="store_true", help="Re-index already indexed documents")
    all_p.add_argument("--force", action="store_true", help="Re-download AND re-index (shorthand for both --force-download --force-index)")
    all_p.add_argument("--include-html", action="store_true", help="Also download HTML documents")
    all_p.add_argument("--dry-run", action="store_true", help="Crawl but write nothing")
    all_p.add_argument("--chroma-dir", default=str(CHROMA_DIR),
                       help="Path to ChromaDB storage directory")
    all_p.add_argument("--embed-model", default=EMBED_MODEL,
                       help="Ollama model for embeddings")
    all_p.add_argument("--chunk-size", type=int, default=CHUNK_SIZE,
                       help="Chunk size in approximate tokens")
    all_p.add_argument("--overlap", type=int, default=CHUNK_OVERLAP,
                       help="Overlap between chunks in approximate tokens")
    all_p.add_argument("--min-quality", type=float, default=0.0,
                       help="Minimum quality score (0.0-1.0) to index. Documents below this are skipped.")

    # --- stats ---
    subparsers.add_parser("stats", help="Show pipeline status and metrics")

    # --- index ---
    idx_p = subparsers.add_parser("index", help="Chunk and embed documents into ChromaDB",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    idx_p.add_argument("--chroma-dir", default=str(CHROMA_DIR),
                       help="Path to ChromaDB storage directory")
    idx_p.add_argument("--embed-model", default=EMBED_MODEL,
                       help="Ollama model for embeddings")
    idx_p.add_argument("--chunk-size", type=int, default=CHUNK_SIZE,
                       help="Chunk size in approximate tokens")
    idx_p.add_argument("--overlap", type=int, default=CHUNK_OVERLAP,
                       help="Overlap between chunks in approximate tokens")
    idx_p.add_argument("--force", action="store_true",
                       help="Re-index all documents (even already indexed ones)")
    idx_p.add_argument("--min-quality", type=float, default=0.0,
                       help="Minimum quality score (0.0-1.0) to index. Documents below this are skipped.")

    # --- ask ---
    ask_p = subparsers.add_parser("ask", help="Ask a question about indexed documents",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ask_p.add_argument("question", nargs="+", help="Your question")
    ask_p.add_argument("--chroma-dir", default=str(CHROMA_DIR),
                       help="Path to ChromaDB storage directory")
    ask_p.add_argument("--embed-model", default=EMBED_MODEL,
                       help="Ollama model for embeddings")
    ask_p.add_argument("--llm-model", "--model", default=LLM_MODEL,
                       help="Ollama model for answering")
    ask_p.add_argument("--top-k", type=int, default=15,
                       help="Number of chunks to retrieve (use higher values for timeline/comparative queries)")
    ask_p.add_argument("--company", type=str, default=None,
                       help="Filter results to a specific company name")
    ask_p.add_argument("--project", type=str, default=None,
                       help="Filter results to a specific project name")
    ask_p.add_argument("--application-type", type=str, default=None,
                       help="Filter results by application type (e.g., 'Section 52')")
    ask_p.add_argument("--filing", type=str, default=None,
                       help="Filter results by filing number")
    ask_p.add_argument("--commodity", type=str, default=None,
                       help="Filter results by commodity (e.g., 'Oil', 'Natural Gas')")
    ask_p.add_argument("--document-type", type=str, default=None,
                       help="Filter results by document type (e.g., 'Application', 'Order')")
    ask_p.add_argument("--sort-by-date", action="store_true",
                       help="Sort retrieved chunks chronologically (useful for timeline queries)")
    ask_p.add_argument("--after", type=str, default=None,
                       help="Only include chunks with date >= this value (YYYY-MM-DD)")
    ask_p.add_argument("--before", type=str, default=None,
                       help="Only include chunks with date <= this value (YYYY-MM-DD)")
    ask_p.add_argument("--show-passages", action="store_true",
                       help="Show the most relevant text passages from matched documents")

    # --- summarize ---
    sum_p = subparsers.add_parser("summarize", help="Extract structured data from documents via LLM",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sum_p.add_argument("topic", nargs="+", help="What to summarize (e.g., 'Trans Mountain conditions')")
    sum_p.add_argument("--chroma-dir", default=str(CHROMA_DIR),
                       help="Path to ChromaDB storage directory")
    sum_p.add_argument("--embed-model", default=EMBED_MODEL,
                       help="Ollama model for embeddings")
    sum_p.add_argument("--llm-model", "--model", default=LLM_MODEL,
                       help="Ollama model for structured extraction")
    sum_p.add_argument("--top-k", type=int, default=30,
                       help="Number of chunks to retrieve (higher = broader context)")
    sum_p.add_argument("--company", type=str, default=None,
                       help="Filter results to a specific company name")
    sum_p.add_argument("--project", type=str, default=None,
                       help="Filter results to a specific project name")
    sum_p.add_argument("--application-type", type=str, default=None,
                       help="Filter results by application type (e.g., 'Section 52')")
    sum_p.add_argument("--filing", type=str, default=None,
                       help="Filter results by filing number")
    sum_p.add_argument("--commodity", type=str, default=None,
                       help="Filter results by commodity (e.g., 'Oil', 'Natural Gas')")
    sum_p.add_argument("--csv", action="store_true",
                       help="Output results as CSV instead of a formatted table")

    # --- trends ---
    trends_p = subparsers.add_parser("trends", help="Analyze filing duration patterns and estimate timelines",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    trends_p.add_argument("--company", type=str, default=None,
                          help="Filter to filings by a specific company")
    trends_p.add_argument("--application-type", type=str, default=None,
                          help="Filter by application type")
    trends_p.add_argument("--commodity", type=str, default=None,
                          help="Filter by commodity")
    trends_p.add_argument("--document-type", type=str, default=None,
                          help="Filter by document type (e.g., 'Post Construction Monitoring Report')")
    trends_p.add_argument("--estimate", action="store_true",
                          help="Show duration estimate for a new filing matching these filters")

    # --- compliance ---
    comp_p = subparsers.add_parser("compliance", help="Detect filings with Orders but missing Compliance documents",
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    comp_p.add_argument("--company", type=str, default=None,
                        help="Filter to a specific company")
    comp_p.add_argument("--all", action="store_true",
                        help="Show all results (default shows top 30)")

    # --- verify ---
    ver_p = subparsers.add_parser("verify", help="Cross-check converted Markdown against the "
                                  "PDF text layer to catch extraction data loss",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ver_p.add_argument("--sample", type=int, default=None,
                       help="Verify a random sample of N documents (default: all converted)")
    ver_p.add_argument("--min-fidelity", type=float, default=0.95,
                       help="Flag documents whose numeric recall falls below this")
    ver_p.add_argument("--all", action="store_true",
                       help="List every flagged document (default shows worst 25)")

    # --- pcmr ---
    pcmr_p = subparsers.add_parser("pcmr", help="Extract findings from Post Construction Monitoring "
                                   "Reports and analyze trends",
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    pcmr_p.add_argument("--document-type", type=str, default="Post Construction Monitoring Report",
                        help="Document type to match (substring)")
    pcmr_p.add_argument("--llm-model", "--model", default=LLM_MODEL,
                        help="Ollama model for structured extraction")
    pcmr_p.add_argument("--company", type=str, default=None,
                        help="Filter to a specific company")
    pcmr_p.add_argument("--after", type=str, default=None,
                        help="Only include reports on or after this date (YYYY-MM-DD)")
    pcmr_p.add_argument("--before", type=str, default=None,
                        help="Only include reports on or before this date (YYYY-MM-DD)")
    pcmr_p.add_argument("--limit", type=int, default=None,
                        help="Analyze at most N reports (useful for a quick test run)")
    pcmr_p.add_argument("--force", action="store_true",
                        help="Re-extract findings even if a cached analysis exists")
    pcmr_p.add_argument("--csv", action="store_true",
                        help="Output per-report findings as CSV instead of the trend report")

    # --- diff ---
    diff_p = subparsers.add_parser("diff", help="Compare two documents to identify changes",
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    diff_p.add_argument("doc1", help="First document ID (from REGDOCS)")
    diff_p.add_argument("doc2", help="Second document ID (from REGDOCS)")
    diff_p.add_argument("--chroma-dir", default=str(CHROMA_DIR),
                        help="Path to ChromaDB storage directory")
    diff_p.add_argument("--llm-model", "--model", default=LLM_MODEL,
                        help="Ollama model for analysis")

    # --- watch ---
    watch_p = subparsers.add_parser("watch", help="Cron-friendly: scout + download + convert + index for last N days",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    watch_p.add_argument("--days", type=int, default=7,
                         help="Number of days to look back")
    watch_p.add_argument("--output-dir", default="downloads", help="Directory for downloaded files")
    watch_p.add_argument("--chroma-dir", default=str(CHROMA_DIR),
                         help="Path to ChromaDB storage directory")
    watch_p.add_argument("--embed-model", default=EMBED_MODEL,
                         help="Ollama model for embeddings")

    # --- export ---
    export_p = subparsers.add_parser("export", help="Export documents table to CSV",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    export_p.add_argument("--output", default="regdocs_export.csv",
                          help="Output CSV file path")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging(args.verbose)

    # Graceful Ctrl+C: let the current operation finish cleanly
    import signal
    shutdown_requested = False

    def handle_sigint(signum, frame):
        nonlocal shutdown_requested
        if shutdown_requested:
            # Second Ctrl+C = force quit
            logging.warning("Force quit.")
            sys.exit(1)
        shutdown_requested = True
        logging.info("Shutdown requested — finishing current operation (Ctrl+C again to force quit)...")

    signal.signal(signal.SIGINT, handle_sigint)

    try:
        if args.command == "scout":
            asyncio.run(run_scout(args))
        elif args.command == "download":
            asyncio.run(run_download(args))
        elif args.command == "convert":
            asyncio.run(run_convert(args))
        elif args.command == "all":
            asyncio.run(run_all(args))
        elif args.command == "stats":
            run_stats(args)
        elif args.command == "index":
            run_index(args)
        elif args.command == "ask":
            run_ask(args)
        elif args.command == "summarize":
            run_summarize(args)
        elif args.command == "trends":
            run_trends(args)
        elif args.command == "compliance":
            run_compliance(args)
        elif args.command == "pcmr":
            run_pcmr(args)
        elif args.command == "verify":
            run_verify(args)
        elif args.command == "diff":
            run_diff(args)
        elif args.command == "watch":
            asyncio.run(run_watch(args))
        elif args.command == "export":
            run_export(args)
        else:
            parser.print_help()
            sys.exit(1)
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        sys.exit(130)


if __name__ == "__main__":
    main()
