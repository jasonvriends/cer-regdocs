# REGDOCS API Reference

The CER REGDOCS website has no official API documentation. This is reverse-engineered from the site's JavaScript and observed HTTP behavior.

---

## Endpoints

| URL | Purpose | Method |
|-----|---------|--------|
| `https://apps.cer-rec.gc.ca/REGDOCS/Search/Advanced` | Advanced Search page (HTML) | GET |
| `https://apps.cer-rec.gc.ca/REGDOCS/Search/SearchAdvancedResults` | Search results (AJAX partial) | GET |
| `https://apps.cer-rec.gc.ca/REGDOCS/Item/View/{id}` | Document detail page | GET |
| `https://apps.cer-rec.gc.ca/REGDOCS/File/Download/{id}` | File download | GET |

---

## Query Parameters (SearchAdvancedResults)

| Parameter | Purpose | Values | Notes |
|-----------|---------|--------|-------|
| `sd` | Start date | `YYYY-MM-DD` | |
| `ed` | End date | `YYYY-MM-DD` | |
| `sr` | Start record (pagination) | 1-based integer | First page = `1`, second page = `201` (with page size 200) |
| `srt` | Sort order | `21` = oldest first, `22` = newest first | 21 keeps pagination stable during crawls |
| `rds` | Facet filter IDs | Comma-separated numeric IDs | Multiple IDs are ANDed |
| `txthl` | Text search highlight | Free text | |
| `limit` | (**IGNORED**) | — | Page size comes from cookie, not this param |

---

## Pagination

Page size is **NOT** controlled by a query parameter. It comes from the `RDI-NumberOfRecords` cookie.

Valid cookie values: `20`, `50`, `100`, `200`

Pagination uses `sr` (start record):
```
Page 1: sr=1      → records 1-200
Page 2: sr=201    → records 201-400
Page 3: sr=401    → records 401-600
...
```

**Important:** Use `srt=21` (oldest first) to keep pagination stable during crawling. With newest-first, new documents arriving mid-crawl shift offsets and cause duplicates or missed records.

### Detecting the End

The results page includes a string like:
```
Item(s) - 1 to 200 out of about 8,500
```

The word "about" is important — totals are approximate. The crawler continues paginating until it gets an empty page.

---

## Facet Filtering

### How It Works

1. The Advanced Search page (`/REGDOCS/Search/Advanced`) has `<select>` dropdowns for each facet category
2. Each `<option>` has a numeric `value` attribute — this is the filter ID
3. Pass filter IDs via `rds=<id>` to filter results to that category
4. Multiple IDs in `rds` are **ANDed** (narrows the result set)
5. The scout discovers these IDs dynamically — they can change if CER adds/removes categories

### Facet Categories

| Category | JSON field | How many values |
|----------|-----------|-----------------|
| Document Type | `document_types` | 76 |
| Application Type | `application_types` | 63 |
| File Type | `file_types` | 4 |
| Role | `roles` | 7 |
| Commodity | `commodities` | 4 |

### How the Scout Uses Facets

To tag documents with their categories, the scout:
1. Runs the date-range search (gets all documents)
2. For each facet value (e.g., "Application", "Order", "Letter"), runs the same date-range search with `rds=<filter_id>`
3. Matches returned IDs against the base results
4. Tags each document with all matching facet values

This requires `N` extra HTTP requests (one per facet value across all categories, ~150+ total), but produces complete tagging.

### Enrichment Efficiency

```
Base crawl:     1 request per 200 documents
Facet queries:  ~150 requests total (regardless of document count)
Total for 10k docs: ~50 (pagination) + 150 (facets) = ~200 requests
```

---

## Request Requirements

### Headers

```http
GET /REGDOCS/Search/SearchAdvancedResults?sd=2026-01-01&ed=2026-06-30&sr=1&srt=21 HTTP/1.1
Host: apps.cer-rec.gc.ca
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
X-Requested-With: XMLHttpRequest
Cookie: RDI-NumberOfRecords=200
```

**Critical headers:**
- `X-Requested-With: XMLHttpRequest` — required for the AJAX endpoint to return partial HTML (without it, you get the full page)
- `Cookie: RDI-NumberOfRecords=200` — controls page size
- `User-Agent` — must look like a real browser

### Rate Limiting

CER doesn't publish rate limits. Observed behavior:
- No evidence of IP banning at polite rates (2-4s between requests)
- 429 responses rare but possible under high load
- 500/502/503 responses during maintenance windows
- No robots.txt restrictions on `/REGDOCS/Search/`

Recommended approach:
- Jittered delay between requests (2-4 seconds)
- Exponential backoff on 429/5xx (2s, 4s, 8s, 16s)
- Concurrency limit of 1 (parallel requests increase 429 risk)

---

## Result HTML Structure

The AJAX endpoint returns a partial HTML document (no `<html>`/`<head>`) containing:

```html
<div>Item(s) - 1 to 200 out of about 8,500</div>
<table>
  <tbody>
    <tr>
      <td>
        <summary>
          <i title="PDF Document"></i>
          <a href="/REGDOCS/File/Download/4642847">C37815-4 Application...</a>
        </summary>
        <details>
          <div>Company:</div>
          <a href="/REGDOCS/Item/View/12345">Bryan Business Centre Ltd.</a>
          <div>Filing:</div>
          <a href="/REGDOCS/Item/View/9012">Filing: C37815</a>
          <hr/>
          <div>First few hundred characters of content...</div>
        </details>
      </td>
      <td>2026-01-02</td>
      <td>Bryan Business Centre Ltd</td>
    </tr>
  </tbody>
</table>
```

### Parsing Guide

| Data | Location | Pattern |
|------|----------|---------|
| Document ID | `<a href="/REGDOCS/(Item/View|File/Download)/(\d+)">` | Regex on href |
| Document name | Link text | |
| Kind (type icon) | `<i title="...">` | Title attribute |
| Is file vs item | `/File/Download/` vs `/Item/View/` | URL path |
| Date | Second `<td>` | Plain text |
| Submitter | Third `<td>` | Plain text |
| Company | `<details>` → sibling of "Company:" div | Link text + ID from href |
| Project | `<details>` → sibling of "Project:" div | Link text + ID from href |
| Filing number | `<details>` → "Filing: XXXXX" | Regex `Filing:\s*(\S+)` |
| Snippet | `<details>` → div after `<hr>` | Plain text |

---

## Document Item Page

`/REGDOCS/Item/View/{id}` shows:
- Full document metadata
- Download links (if applicable)
- Filing tree (parent/child relationships)
- Activity history

This page is **not** currently crawled by the scout (too slow for bulk discovery), but could be used for:
- Extracting filing tree structure
- Getting full metadata for individual documents
- Finding related documents

---

## File Download

`/REGDOCS/File/Download/{id}` returns the actual file with:
- `Content-Type` header (e.g., `application/pdf`)
- `Content-Disposition` header with filename
- Possible redirect (follow redirects)

Some files are on a different host (`docs2.cer-rec.gc.ca`) — the downloader follows redirects automatically.

---

## Known Quirks

1. **"About" totals** — the total count shown in results is approximate. Don't rely on it for exact pagination bounds.
2. **Facet IDs change** — CER can add/remove/renumber facet values. Always discover them fresh from the Advanced Search page.
3. **Date clamping** — if you pass an end date like `2026-02-31`, the server silently clamps it to a valid date.
4. **Empty facets** — some facet values exist in the dropdown but return zero results (e.g., "Certificate", "Comprehensive Study").
5. **HTML documents** — some items are HTML pages referencing images stored in CER's Content Server (not publicly accessible). See [`html-documents.md`](html-documents.md).
6. **Compound documents** — these are containers (like ZIP files in OpenText); they show as folders in search results and cannot be downloaded directly.
