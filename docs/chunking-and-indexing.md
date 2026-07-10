# Chunking & Indexing

Technical details on how documents are converted, chunked, embedded, and stored for RAG retrieval.

---

## Pipeline Flow

```
PDF/HTML file
    │
    ▼ (convert stage)
Page-annotated Markdown        ← Docling with iterate_items() provenance
    │
    ▼ (index stage)
┌───────────────────────────────────────────────────┐
│ ChromaDB collection "regdocs"                      │
│                                                    │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────┐  │
│  │Content chunk│  │Doc summary  │  │Filing    │  │
│  │chunk_idx: 0 │  │chunk_idx:-1 │  │summary   │  │
│  │chunk_idx: 1 │  │             │  │chunk_idx:│  │
│  │chunk_idx: 2 │  │(metadata    │  │-2        │  │
│  │...          │  │ only, no    │  │          │  │
│  │(text +      │  │ content)    │  │(per      │  │
│  │ metadata    │  │             │  │ filing)  │  │
│  │ header)     │  │             │  │          │  │
│  └─────────────┘  └─────────────┘  └──────────┘  │
└───────────────────────────────────────────────────┘
         +
┌───────────────────────────────────────────────────┐
│ SQLite FTS5 table "documents_fts"                  │
│ (keyword search: name, company, filing, snippet)   │
└───────────────────────────────────────────────────┘
```

---

## Convert Stage: Page-Annotated Markdown

The convert stage uses [Docling](https://github.com/DS4SD/docling) to convert PDFs to Markdown with page provenance.

### How It Works

Instead of `doc.export_to_markdown()` (which produces flat text), we use:

```python
for item, _level in doc.iterate_items():
    if hasattr(item, 'prov') and item.prov:
        page = item.prov[0].page_no
        if page != current_page:
            parts.append(f"\n<!-- page:{page} -->")
            current_page = page
    parts.append(item.export_to_markdown())
```

This inserts invisible page markers into the Markdown:

```markdown
<!-- page:1 -->

## Application for Approval

Trans Mountain Pipeline ULC hereby applies under section 52...

<!-- page:2 -->

### Terms and Conditions

The following conditions are imposed...
```

### Why Whole-Document Conversion

Docling processes the entire document at once because:
- Layout analysis needs full-page context (columns, headers, footers)
- Tables that span pages need both pages for correct structure
- Reading order depends on the document's overall layout
- OCR confidence improves with more context

### Quality Score

After conversion, a quality score (0.0–1.0) is computed based on:
- Average word length (prose = 4-7 chars; OCR noise = shorter)
- Long line ratio (prose has long lines)
- Alphabetic character ratio (vs symbols/numbers)
- Short fragment ratio (noise = many short lines)
- Sentence density (prose has sentence-ending punctuation)

Use `--min-quality 0.3` during indexing to skip engineering drawings, scanned maps, or badly-OCR'd documents that would add noise to search results.

---

## Index Stage: Three Chunk Types

### 1. Content Chunks (chunk_index ≥ 0)

Text is split into overlapping windows:

| Parameter | Default | What it controls |
|-----------|---------|-----------------|
| `--chunk-size` | 512 | Tokens per chunk (approx: 1 token ≈ 0.75 words) |
| `--overlap` | 64 | Token overlap between consecutive chunks |

With defaults: each chunk is ~384 words, overlapping by ~48 words.

**Page tracking:** The `chunk_text_with_pages()` function:
1. Strips `<!-- page:N -->` markers from the text
2. Builds a word→page mapping
3. Splits into overlapping chunks
4. Records which pages each chunk spans (`page_start`, `page_end`)

**Metadata header:** Each chunk gets a header prepended before embedding:

```
Company: Trans Mountain Pipeline ULC
Project: Trans Mountain Expansion
Filing: OF-Fac-Oil-T260-2013-03 02
Date: 2025-03-15
Submitter: Trans Mountain Pipeline ULC
Kind: PDF Document
Document Type: Application
Application Type: CERA 183 Pipeline Facilities Certificate
Commodity: Oil
Role: Applicant

[actual document text...]
```

This means the embedding vector captures both the content semantics AND the metadata context. A search for "Trans Mountain application" will score highly even if the text chunk doesn't mention "Trans Mountain" explicitly.

### 2. Document Summary Chunks (chunk_index = -1)

One per document. Contains all metadata fields as text, but no document content:

```
Company: Trans Mountain Pipeline ULC
Project: Trans Mountain Expansion
Filing: OF-Fac-Oil-T260-2013-03 02
Date: 2025-03-15
Submitter: Trans Mountain Pipeline ULC
Kind: PDF Document
Document Type: Application
Application Type: CERA 183 Pipeline Facilities Certificate
Commodity: Oil
Role: Applicant
Document: C37815-4 Application for Short-Term Export
Total pages: 24
Total chunks: 12
```

**Purpose:** Questions like "what did Trans Mountain file?" need to find documents as units, not individual text passages. Without summary chunks, you'd only find Trans Mountain documents if one of their text chunks happened to mention the company name.

### 3. Filing Summary Chunks (chunk_index = -2)

One per filing number. Aggregates metadata across all documents in a filing:

```
Filing: C37813
Company: Whitecap Resources Inc.
Date Range: 2025-12-31 to 2026-01-28
Duration: 28 days (0.9 months)
Documents: 18 (Application, Letter, Order, Receipt, Supplemental Information)
Commodity: Oil
Application Type: CERA 181 Purchase/Sell/Name Change of Pipeline Facilities
Roles: Applicant, Commission
Contains: Information Requests
Contains: Hearing documents
```

**Purpose:** Timeline and comparison queries need to reason about entire proceedings, not individual documents. Filing summaries include duration, document count, and complexity indicators.

---

## ChromaDB Metadata Fields

Every chunk (all three types) carries these filterable metadata fields:

| Field | Type | Used For |
|-------|------|----------|
| `document_id` | string | Linking back to SQLite |
| `document_name` | string | Display in citations |
| `chunk_index` | int | -2=filing summary, -1=doc summary, ≥0=content |
| `total_chunks` | int | Document size indicator |
| `page_start` / `page_end` | int | Page citations |
| `company` / `company_id` | string | `--company` filter |
| `project` / `project_id` | string | `--project` filter |
| `filing_number` | string | `--filing` filter, filing grouping |
| `date` | string | `--after`/`--before` filter, chronological sort |
| `submitter` | string | Display |
| `kind` | string | PDF/HTML/Compound |
| `is_file` | bool | Downloadable vs container |
| `application_types` | string | `--application-type` filter |
| `document_types` | string | `--document-type` filter |
| `commodities` | string | `--commodity` filter |
| `roles` | string | Regulatory role |
| `quality_score` | float | `--min-quality` filter |
| `is_summary` | bool | Distinguish summary from content chunks |

Filing summaries additionally have:
| `date_end` | string | End of filing date range |
| `duration_days` | int | Filing duration |
| `document_count` | int | Number of docs in filing |
| `has_ir` | bool | Information requests present |
| `has_hearing` | bool | Hearing documents present |

---

## FTS5 Keyword Index

The `documents_fts` table provides keyword search for exact matching:

```sql
CREATE VIRTUAL TABLE documents_fts USING fts5(
    doc_id,
    name,
    company,
    project,
    filing_number,
    submitter,
    snippet,
    document_types,
    application_types,
    commodities,
    roles,
    tokenize='porter'
);
```

**Porter stemming** means "applications" matches "application", "filing" matches "filed", etc.

**When it's used:** The `ask` command detects filing numbers (`C37815`, `OF-Fac-Oil-T260-2013-03`) and quoted terms in your question and automatically runs a keyword search alongside vector search. Results are merged and deduplicated.

**Rebuild:** The FTS index is rebuilt automatically at the end of every `index` run.

---

## Hybrid Search Flow

```
User question
    │
    ├──→ Embed question (Ollama nomic-embed-text)
    │         │
    │         ▼
    │    ChromaDB vector search (top-k results)
    │         │
    ├──→ Detect filing numbers / quoted terms?
    │         │ (yes)
    │         ▼
    │    SQLite FTS5 keyword search
    │         │
    │         ▼
    │    ChromaDB get(where={document_id in FTS results})
    │         │
    ▼         ▼
    Merge + deduplicate
         │
         ▼
    Apply filters (--company, --after, --before, etc.)
         │
         ▼
    Optionally sort by date (--sort-by-date)
         │
         ▼
    Build context + send to LLM
```

---

## Tuning Recommendations

### Chunk Size

| Use Case | Recommended `--chunk-size` | Why |
|----------|---------------------------|-----|
| General Q&A | 512 (default) | Good balance of context and precision |
| Conditions extraction | 256 | Conditions are short, precise passages |
| Timeline analysis | 768 | More context per chunk = fewer chunks needed |
| Full-document summarization | 1024 | Fewer, larger chunks for broad questions |

### Overlap

| Use Case | Recommended `--overlap` | Why |
|----------|------------------------|-----|
| General | 64 (default) | Prevents splitting sentences at boundaries |
| High-precision extraction | 128 | More overlap = less risk of missing context |
| Large corpus (save space) | 32 | Less overlap = fewer total chunks |

### Top-K

| Question Type | Recommended `--top-k` |
|--------------|----------------------|
| Single document focused | 5 |
| General Q&A | 15 (default) |
| Timeline (multiple docs) | 25-30 |
| Broad comparison | 30-50 |

### When to Re-Index

Run `index --force` when:
- You've upgraded to a new version with changed metadata fields
- You've changed `--chunk-size` or `--overlap`
- You want to switch embedding models
- Filing summaries are stale (many new documents added)
