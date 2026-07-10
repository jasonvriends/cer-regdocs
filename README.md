# CER REGDOCS Pipeline

**A unified tool to collect, download, and convert public documents from [REGDOCS](https://apps.cer-rec.gc.ca/REGDOCS/) into a RAG-ready knowledge base.**

REGDOCS is the Canada Energy Regulator's public library of regulatory documents. This tool automates the full lifecycle — discovery, download, conversion to Markdown, and indexing for question-answering — backed by a single SQLite database as the system bus.

---

## Quick Start

```bash
# one-time setup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# run the full pipeline for one week
python regdocs.py all --start-date 2026-06-01 --end-date 2026-06-07 --limit 10

# or run stages individually
python regdocs.py scout --start-date 2026-06-01 --end-date 2026-06-07
python regdocs.py download
python regdocs.py convert
python regdocs.py index

# ask questions about the documents
python regdocs.py ask "What conditions did CER impose on Trans Mountain?"

# check pipeline status at any time
python regdocs.py stats

# cron-friendly: process last 7 days in one command
python regdocs.py watch --days 7
```

### Prerequisites for RAG

```bash
# Install Ollama (https://ollama.com)
curl -fsSL https://ollama.com/install.sh | sh

# Pull the required models
ollama pull nomic-embed-text   # embeddings (274 MB)
ollama pull gemma4:26b         # LLM for answering (17 GB) — or use a smaller model
```

### Web UI

```bash
python regdocs_ui.py                    # http://localhost:7860
python regdocs_ui.py --share            # public Gradio link (temporary)
python regdocs_ui.py --host 0.0.0.0     # accessible on your LAN
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          regdocs.py                               │
├────────┬──────────┬─────────┬───────┬───────┬───────┬─────┬─────┤
│ scout  │ download │ convert │ index │  ask  │ watch │ all │stats│
└───┬────┴─────┬────┴────┬────┴───┬───┴───┬───┴───────┴──┬──┴──┬──┘
    │          │         │        │       │               │     │
    ▼          ▼         ▼        ▼       ▼               ▼     ▼
┌──────────────────────────────────────────────────────────────────┐
│                      regdocs.db (SQLite)                          │
│  documents | history | metrics | runs                            │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  chroma_db/ (vectors)│
                    └─────────────────────┘
```

---

## Commands

| Command | What it does |
|---------|-------------|
| `scout` | Crawls REGDOCS for a date range, inserts documents with `status=NEW` |
| `download` | Downloads files for all `NEW` documents, marks them `DOWNLOADED` |
| `convert` | Converts `DOWNLOADED` files to Markdown via Docling (GPU/OCR), marks `CONVERTED` |
| `index` | Chunks Markdown and embeds into ChromaDB using Ollama |
| `ask` | Retrieves relevant chunks and answers via Ollama LLM |
| `summarize` | Extracts structured data (conditions, dates, status) into a table |
| `trends` | Analyzes filing duration patterns and estimates timelines from metadata |
| `compliance` | Detects filings with Orders but no Compliance documents |
| `diff` | Compares two documents to identify changes (LLM-powered) |
| `all` | Runs scout → download → convert → index in sequence |
| `watch` | Cron-friendly: processes the last N days end-to-end |
| `stats` | Pipeline dashboard: progress, queues, ETA, throughput, disk usage |
| `export` | Dumps the documents table to CSV |

---

## Enterprise Features

| Feature | How it works |
|---------|-------------|
| **Idempotency** | SHA-256 hash of every download. Rescans detect changes without reprocessing. |
| **Resilience** | Exponential backoff retries. `retry_count` tracked per document. Restart anytime — the script asks the DB "what's missing?" |
| **Throttling** | Token-bucket rate limiter prevents 429s. Configurable via `--min-delay`. |
| **Observability** | Every operation inserts a record into the `metrics` table with timing and outcome. |
| **Auditing** | The `history` table logs every state transition with timestamps. |
| **Reporting** | `python regdocs.py stats` — ETA, throughput, disk usage, error breakdown. |

---

## Document Lifecycle

```
NEW  ──→  DOWNLOADED  ──→  CONVERTED  ──→  INDEXED (in ChromaDB)
  │            │                │
  └──→ FAILED ←┘────────────────┘
        (retry_count incremented, last_error preserved)
        next run auto-retries if retry_count < max_retries
```

---

## Database Schema

Four tables in `regdocs.db`:

**documents** — the state machine:
```sql
id TEXT PRIMARY KEY, name TEXT, url TEXT, status TEXT, file_path TEXT,
markdown_path TEXT, hash TEXT, last_error TEXT, retry_count INTEGER,
metadata JSON, created_at TIMESTAMP, updated_at TIMESTAMP
```

**history** — audit trail (every state transition logged)

**metrics** — observability (every operation with timing, success/failure, error type)

**runs** — tracks every invocation with parameters and summary

The `metadata` JSON column stores everything the scout discovers: `kind`, `date`, `submitter`, `company`, `project`, `document_types`, `application_types`, `commodities`, etc.

---

## Useful Queries

```sql
-- Pipeline overview
SELECT status, COUNT(*) FROM documents GROUP BY status;

-- What failed and why?
SELECT id, name, last_error, retry_count FROM documents WHERE status = 'FAILED';

-- Average download time
SELECT AVG(duration_ms) FROM metrics WHERE stage = 'download' AND success = 1;

-- Documents by company
SELECT json_extract(metadata, '$.company') as company, COUNT(*) as cnt
FROM documents GROUP BY company ORDER BY cnt DESC LIMIT 10;

-- Reset a stuck document
UPDATE documents SET status = 'NEW', retry_count = 0, last_error = NULL WHERE id = '12345';

-- Throughput over time
SELECT strftime('%Y-%m-%d %H:00', created_at) as hour, COUNT(*) as ops
FROM metrics GROUP BY hour ORDER BY hour;
```

---

## Common Patterns

The database accumulates state across runs — no shell loops needed.

```bash
# Collect a full year
python regdocs.py all --start-date 2026-01-01 --end-date 2026-12-31

# Keep data fresh (last 30 days) — safe to run daily
python regdocs.py watch --days 30

# Resume after a crash — picks up where it left off
python regdocs.py download

# Dry-run to see what would happen
python regdocs.py download --dry-run
python regdocs.py convert --dry-run

# Export for spreadsheet analysis
python regdocs.py export --output regdocs.csv

# Use a faster/smaller model for quick answers
python regdocs.py ask --model qwen2.5-coder:1.5b "What companies filed this week?"
```

### RAG Queries

Filter by company, project, or application type for better results:

```bash
# Timeline for one company
python regdocs.py ask --company "Trans Mountain Pipeline ULC" \
  "Show me the chronological sequence of filings"

# Compare companies
python regdocs.py ask "Compare Trans Mountain and Coastal GasLink timelines"

# Conditions analysis
python regdocs.py ask --application-type "Section 52" \
  "What conditions are most commonly imposed?"

# Scope to a filing
python regdocs.py ask --filing "OF-Fac-Oil-T260-2013-03 02" "Summarize this filing"

# Broad analysis with more context
python regdocs.py ask --top-k 30 "Overview of all applications filed in 2025"

# Structured extraction as a table
python regdocs.py summarize --company "Westcoast Energy Inc." \
  "all filings, dates, and application types"

# Duration estimation (no LLM needed)
python regdocs.py trends --application-type "CERA 183" --estimate

# Compliance gaps: filings with orders but no compliance docs
python regdocs.py compliance --company "Trans Mountain"

# Compare two documents
python regdocs.py diff 4642847 4642848
```

---

## Documentation

| Document | What it covers |
|----------|---------------|
| [`docs/rag-and-search.md`](docs/rag-and-search.md) | Search guide: filters, timeline queries, summarize, tips |
| [`docs/chunking-and-indexing.md`](docs/chunking-and-indexing.md) | Technical: how PDFs become searchable chunks, page tracking, FTS5 |
| [`docs/trends-and-estimation.md`](docs/trends-and-estimation.md) | Filing duration analysis, complexity indicators, estimation |
| [`docs/regdocs-api.md`](docs/regdocs-api.md) | REGDOCS endpoint reference: query params, pagination, facets, HTML parsing |
| [`docs/html-documents.md`](docs/html-documents.md) | Why HTML documents are skipped and how to handle them |

---

## Options Reference

### Global

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | `regdocs.db` | Path to the SQLite database |
| `--verbose` | off | Debug logging |

### scout

| Option | Default | Description |
|--------|---------|-------------|
| `--start-date` | `2026-01-01` | Start of date range (YYYY-MM-DD) |
| `--end-date` | `2026-12-31` | End of date range (auto-clamps invalid days) |
| `--facets` | `all` | Categories to enrich: `all`, `none`, or comma list |
| `--limit` | no limit | Stop after N documents |
| `--page-size` | `200` | Results per request (20/50/100/200) |
| `--concurrency` | `1` | Parallel requests |
| `--min-delay` / `--max-delay` | `2.0` / `4.0` | Politeness delay (seconds) |
| `--dry-run` | off | Don't write to database |

### download

| Option | Default | Description |
|--------|---------|-------------|
| `--output-dir` | `downloads` | Where to save files |
| `--concurrency` | `1` | Parallel downloads |
| `--min-delay` / `--max-delay` | `2.0` / `4.0` | Politeness delay |
| `--max-retries` | `3` | Max retry attempts |
| `--force` | off | Re-download existing files |
| `--include-html` | off | Also download HTML documents (see `docs/html-documents.md`) |
| `--dry-run` | off | Show what would be downloaded |

### convert

| Option | Default | Description |
|--------|---------|-------------|
| `--output-dir` | `markdown` | Where to save Markdown files |
| `--concurrency` | `1` | Parallel conversions |
| `--max-retries` | `3` | Max retry attempts |
| `--dry-run` | off | Show what would be converted |

### index

| Option | Default | Description |
|--------|---------|-------------|
| `--chroma-dir` | `chroma_db` | ChromaDB storage directory |
| `--embed-model` | `nomic-embed-text` | Ollama embedding model |
| `--chunk-size` | `512` | Chunk size in approximate tokens |
| `--overlap` | `64` | Overlap between chunks in tokens |
| `--force` | off | Re-index all documents (rebuild from scratch) |
| `--min-quality` | `0.0` | Skip documents with quality score below this |

### ask

| Option | Default | Description |
|--------|---------|-------------|
| `--model` | `gemma4:26b` | Ollama LLM for answering |
| `--top-k` | `15` | Number of chunks to retrieve |
| `--embed-model` | `nomic-embed-text` | Ollama embedding model |
| `--company` | none | Filter results to a specific company |
| `--project` | none | Filter results to a specific project |
| `--application-type` | none | Filter by application type (substring match) |
| `--filing` | none | Filter by filing number |
| `--commodity` | none | Filter by commodity (e.g., "Oil", "Natural Gas") |
| `--document-type` | none | Filter by document type (e.g., "Application", "Order") |
| `--after` | none | Only include documents on or after this date (YYYY-MM-DD) |
| `--before` | none | Only include documents on or before this date (YYYY-MM-DD) |
| `--sort-by-date` | off | Sort results chronologically for timeline queries |
| `--show-passages` | off | Show the most relevant text passages from matched documents |

### summarize

| Option | Default | Description |
|--------|---------|-------------|
| `--model` | `gemma4:26b` | Ollama LLM for extraction |
| `--top-k` | `30` | Number of chunks (higher = broader context) |
| `--company` | none | Filter to a specific company |
| `--project` | none | Filter to a specific project |
| `--application-type` | none | Filter by application type |
| `--filing` | none | Filter by filing number |
| `--commodity` | none | Filter by commodity |
| `--csv` | off | Output as CSV instead of table |

### trends

| Option | Default | Description |
|--------|---------|-------------|
| `--company` | none | Filter to filings by a specific company |
| `--application-type` | none | Filter by application type |
| `--commodity` | none | Filter by commodity |
| `--estimate` | off | Show duration estimate for a new filing matching these filters |

### watch

| Option | Default | Description |
|--------|---------|-------------|
| `--days` | `7` | Number of days to look back |

### export

| Option | Default | Description |
|--------|---------|-------------|
| `--output` | `regdocs_export.csv` | Output CSV file path |

---

## About

This tool collects publicly available information from the [Canada Energy Regulator's REGDOCS](https://apps.cer-rec.gc.ca/REGDOCS/) website for non-commercial purposes. It is a personal project — not made by, affiliated with, or endorsed by the Canada Energy Regulator.
