#!/usr/bin/env python3
"""Subprocess worker for converting a single PDF to Markdown via Docling.

This script is invoked by regdocs.py's convert stage as a separate process.
If Docling segfaults on a problematic PDF, only this subprocess dies — the
parent continues processing the next document.

Usage:
    python convert_worker.py <input_pdf> <output_md> [--html-preprocess]

Exit codes:
    0  — success (writes JSON result to stdout)
    1  — conversion error (writes JSON error to stdout)
    2  — invalid arguments
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

# Suppress noisy warnings before importing heavy libraries
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
import warnings
warnings.filterwarnings("ignore", message=".*tied weights.*")
warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("transformers.modeling_utils").setLevel(logging.ERROR)


def preprocess_html(source_path: Path, target_path: Path) -> None:
    """Replace image references in HTML with text equivalents."""
    try:
        import lxml  # noqa: F401
        parser = "lxml"
    except ImportError:
        parser = "html.parser"

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(source_path.read_text(encoding="utf-8", errors="replace"), parser)
    for img in soup.find_all("img"):
        src_name = Path(str(img.get("src", ""))).name.lower()
        if src_name in ("yes.png", "checked.png"):
            img.replace_with("☑")
        elif src_name in ("no.png", "unchecked.png"):
            img.replace_with("☐")
        elif img.get("alt"):
            img.replace_with(str(img["alt"]))
        else:
            img.decompose()
    target_path.write_text(str(soup), encoding="utf-8")


def get_converter():
    """Initialize the Docling document converter with GPU support if available."""
    from docling.datamodel.base_models import InputFormat
    from docling.document_converter import DocumentConverter, PdfFormatOption

    # Monkeypatch: fix docling-ibm-models bug where LOG_LEVEL becomes a closure cell
    import docling_ibm_models.tableformer.settings as _tfs
    _original_get_custom_logger = _tfs.get_custom_logger

    def _safe_get_custom_logger(logger_name, level, stream=None):
        if stream is None:
            stream = sys.stdout
        if not isinstance(level, (int, str)):
            level = logging.INFO
        return _original_get_custom_logger(logger_name, level, stream)

    _tfs.get_custom_logger = _safe_get_custom_logger

    from docling.datamodel.pipeline_options import (
        PdfPipelineOptions,
        EasyOcrOptions,
        AcceleratorOptions,
    )

    # Auto-detect GPU
    device = "cpu"
    try:
        import torch
        if torch.cuda.is_available():
            device = "cuda"
            logging.info("CUDA GPU detected — using GPU acceleration")
        else:
            logging.info("No CUDA GPU — using CPU")
    except ImportError:
        logging.info("PyTorch not available — using CPU")

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_table_structure = True
    pipeline_options.do_formula_enrichment = True
    pipeline_options.do_ocr = True
    pipeline_options.accelerator_options = AcceleratorOptions(
        device=device, num_threads=4 if device == "cpu" else 1
    )
    pipeline_options.ocr_options = EasyOcrOptions(lang=["en", "fr"])

    return DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
    )


def compute_quality_score(text: str) -> float:
    """Score converted markdown quality from 0.0 (garbled) to 1.0 (clean)."""
    if not text or not text.strip():
        return 0.0

    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return 0.0

    words = text.split()
    total_chars = len(text)

    avg_word_len = sum(len(w) for w in words) / max(len(words), 1)
    word_len_score = min(1.0, max(0.0, (avg_word_len - 1.5) / 4.0))

    long_lines = sum(1 for l in lines if len(l.strip()) >= 40)
    long_line_ratio = long_lines / len(lines)

    alpha_chars = sum(1 for c in text if c.isalpha())
    alpha_ratio = alpha_chars / max(total_chars, 1)

    short_fragments = sum(1 for l in lines if len(l.strip()) < 5)
    short_frag_score = 1.0 - (short_fragments / len(lines))

    sentence_lines = sum(1 for l in lines if any(c in l for c in '.?!'))
    sentence_density = sentence_lines / len(lines)

    score = (
        0.20 * word_len_score +
        0.25 * long_line_ratio +
        0.20 * alpha_ratio +
        0.15 * short_frag_score +
        0.20 * sentence_density
    )
    return round(min(1.0, max(0.0, score)), 3)


def build_bbox_sidecar(doc_obj) -> dict:
    """Collect per-item page/bounding-box provenance for click-to-highlight UI.

    Coordinates are PDF points as reported by Docling (origin noted per bbox,
    typically bottom-left). Page dimensions are included so a viewer can
    normalize. Item text (truncated) is stored so chunks can later be matched
    back to their page regions by text search.
    """
    items = []
    for item, _level in doc_obj.iterate_items():
        prov = getattr(item, "prov", None)
        if not prov:
            continue
        items.append({
            "label": str(getattr(item, "label", "")),
            "text": (getattr(item, "text", "") or "")[:300],
            "prov": [
                {
                    "page": p.page_no,
                    "bbox": [round(p.bbox.l, 2), round(p.bbox.t, 2),
                             round(p.bbox.r, 2), round(p.bbox.b, 2)],
                    "origin": str(getattr(p.bbox, "coord_origin", "")),
                }
                for p in prov
            ],
        })
    pages = {}
    for page_no, page in getattr(doc_obj, "pages", {}).items():
        size = getattr(page, "size", None)
        if size is not None:
            pages[str(page_no)] = {"width": round(size.width, 2), "height": round(size.height, 2)}
    return {"pages": pages, "items": items}


def convert_document(input_path: Path, output_path: Path, html_preprocess: bool = False) -> dict:
    """Convert a single document, return result dict."""
    t0 = time.monotonic()

    preprocessed_path = None
    convert_path = input_path

    try:
        ext = input_path.suffix.lower()

        if ext in (".html", ".htm") and html_preprocess:
            preprocessed_path = output_path.with_suffix(".preprocessed.html")
            preprocess_html(input_path, preprocessed_path)
            convert_path = preprocessed_path

        logging.info(f"Initializing converter...")
        converter = get_converter()

        logging.info(f"Converting: {input_path.name}")
        result = converter.convert(convert_path)

        # Build page-annotated markdown: export each page separately and
        # prefix it with an invisible <!-- page:N --> marker that the index
        # stage uses for page citations. HTML inputs have no pages dict and
        # fall through to the whole-document export.
        doc_obj = result.document
        try:
            parts = []
            for page_no in sorted(doc_obj.pages.keys()):
                page_md = doc_obj.export_to_markdown(page_no=page_no)
                if page_md and page_md.strip():
                    parts.append(f"<!-- page:{page_no} -->\n\n{page_md}")
            markdown_content = "\n\n".join(parts)
            if not markdown_content.strip():
                markdown_content = doc_obj.export_to_markdown()
        except Exception:
            logging.warning("Per-page export failed; falling back to flat markdown", exc_info=True)
            markdown_content = doc_obj.export_to_markdown()

        import re as _re
        quality_score = compute_quality_score(
            _re.sub(r"<!--\s*page:\d+\s*-->", "", markdown_content)
        )

        # Bounding-box sidecar for click-to-highlight PDF viewing (PDFs only —
        # HTML documents have no page geometry). Failure here is non-fatal:
        # the markdown is still valid without it.
        bbox_path = None
        try:
            sidecar = build_bbox_sidecar(doc_obj)
            if sidecar["items"]:
                bbox_path = output_path.with_suffix(".bbox.json")
                bbox_tmp = bbox_path.with_suffix(".tmp")
                with open(bbox_tmp, "w", encoding="utf-8") as f:
                    json.dump(sidecar, f, ensure_ascii=False)
                bbox_tmp.replace(bbox_path)
        except Exception as e:
            logging.warning(f"Bbox sidecar failed (non-fatal): {e}")
            bbox_path = None

        # Atomic write
        tmp_path = output_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        tmp_path.replace(output_path)

        duration_ms = (time.monotonic() - t0) * 1000
        logging.info(f"Done: {output_path.name} (quality={quality_score:.3f}, {duration_ms/1000:.1f}s)")

        return {
            "success": True,
            "output_path": str(output_path),
            "bbox_path": str(bbox_path) if bbox_path else None,
            "quality_score": quality_score,
            "duration_ms": duration_ms,
        }

    except Exception as e:
        duration_ms = (time.monotonic() - t0) * 1000
        logging.error(f"Failed: {type(e).__name__}: {e}")
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "duration_ms": duration_ms,
        }

    finally:
        if preprocessed_path and preprocessed_path.exists():
            preprocessed_path.unlink()


def main():
    if len(sys.argv) < 3:
        print("Usage: python convert_worker.py <input_file> <output_md> [--html-preprocess]",
              file=sys.stderr)
        sys.exit(2)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    html_preprocess = "--html-preprocess" in sys.argv

    if not input_path.exists():
        result = {"success": False, "error": f"File not found: {input_path}", "error_type": "FileNotFoundError", "duration_ms": 0}
        print(json.dumps(result))
        sys.exit(1)

    result = convert_document(input_path, output_path, html_preprocess)
    print(json.dumps(result))
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
