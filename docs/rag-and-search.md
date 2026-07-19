# Search & Analysis Guide

How to use `ask` and `summarize` to query the knowledge base. For technical details on how indexing works, see [`chunking-and-indexing.md`](chunking-and-indexing.md).

---

## What You Can Ask

| Question Type | How | Example |
|---|---|---|
| **Single document** | Low top-k, filing filter | `ask --filing C37815 "Summarize"` |
| **Company overview** | Company filter | `ask --company "Trans Mountain" "What have they filed?"` |
| **Timeline** | Company + sort-by-date | `ask --company "Enbridge" --sort-by-date "Filing history"` |
| **Comparison** | No filter, high top-k | `ask --top-k 30 "Compare Trans Mountain and NGTL"` |
| **Conditions** | App type filter | `ask --application-type "Section 52" "What conditions?"` |
| **Status** | Filing filter | `ask --filing C37815 "What's the current status?"` |
| **Patterns** | Commodity + top-k | `ask --commodity "Gas" --top-k 30 "Common application types"` |
| **Precedent** | App type + date range | `ask --application-type "CERA 183" --before 2024-01-01 "Past conditions"` |

---

## Filters

### Exact-Match Filters (ChromaDB metadata)

These filter on chunk metadata before vector similarity is computed:

```bash
--company "Trans Mountain Pipeline ULC"   # exact company name
--project "Coastal GasLink Pipeline"      # exact project name
--filing "OF-Fac-Oil-T260-2013-03 02"    # exact filing number
```

### Substring Filters (document text)

These search the embedded text (which includes the metadata header):

```bash
--application-type "Section 52"    # matches "s.52 Facilities" in header
--commodity "Natural Gas"          # matches "Commodity: Natural Gas"
--document-type "Order"            # matches "Document Type: Order"
```

### Date Filters

```bash
--after 2025-01-01                 # documents on or after this date
--before 2025-12-31                # documents on or before this date
```

### Combining

All filters are ANDed:

```bash
python regdocs.py ask \
  --company "Trans Mountain Pipeline ULC" \
  --application-type "Section 52" \
  --after 2020-01-01 \
  --sort-by-date \
  "What conditions were imposed and when?"
```

---

## Timeline Queries

For questions that span time, use `--sort-by-date`:

```bash
# Company filing history
python regdocs.py ask --company "Enbridge Pipelines Inc." --sort-by-date \
  "Show me the chronological sequence of filings, dates, and document types"

# Filing lifecycle
python regdocs.py ask --filing "C37813" --sort-by-date \
  "Walk me through this filing from application to decision"

# Compare timelines
python regdocs.py ask --sort-by-date \
  "Compare the Trans Mountain and Coastal GasLink application timelines"
```

When `--sort-by-date` is active:
- Retrieved chunks are sorted chronologically before being sent to the LLM
- The system prompt hints that results are in chronological order
- The LLM is instructed to present information as a timeline

---

## Structured Extraction (summarize)

The `summarize` command asks the LLM to extract structured data into a table:

```bash
# Conditions table
python regdocs.py summarize --company "Trans Mountain Pipeline ULC" \
  "conditions imposed and their compliance status"

# Filing inventory
python regdocs.py summarize --company "Westcoast Energy Inc." \
  "all filings with dates, types, and outcomes"

# Export to CSV for Excel
python regdocs.py summarize --application-type "CERA 183" \
  --csv "filing number, company, date, commodity, decision"
```

The LLM extracts: Filing Number, Company, Date, Document Type, Application Type, Key Conditions/Decisions, Status, Notable Dates.

---

## Hybrid Search (Automatic)

The system automatically detects when keyword search would help:

| Your question contains | What happens |
|---|---|
| Filing number like `C37815` | Also searches FTS5 by filing number |
| Pattern like `OF-Fac-Oil-T260` | Also searches FTS5 by that pattern |
| Quoted text like `"Trans Mountain"` | Also searches FTS5 for exact match |

Results from both vector search and keyword search are merged and deduplicated. This ensures you find documents even when embeddings don't capture exact identifiers well.

---

## Model Selection

```bash
# Quick lookups (fast, basic)
python regdocs.py ask --model qwen2.5-coder:1.5b "List companies that filed this week"

# Focused questions (good balance)
python regdocs.py ask --model qwen2.5-coder:7b --filing C37815 "Summarize"

# Complex analysis (best quality, slower)
python regdocs.py ask --model gemma4:26b \
  "Compare Trans Mountain and NGTL applications and explain why one took longer"
```

---

## Tips

1. **Start broad, then narrow.** Try without filters first to see what comes back, then add `--company` or `--filing` to focus.

2. **Filing numbers are the best filter.** They uniquely identify a regulatory proceeding and group all related documents together.

3. **Use `--top-k 30+` for comparison questions.** Default 15 may not have enough documents from both companies being compared.

4. **Use `summarize` for tables.** If you want structured output (CSV, table), use `summarize` instead of `ask`.

5. **Check `trends` first for duration questions.** `trends --estimate` gives you numbers directly from metadata — faster and more precise than asking the LLM.

6. **Combine `ask` with `trends`.** Use `trends` for the numbers, then `ask` for the "why":
   ```bash
   python regdocs.py trends --company "NGTL" --estimate
   python regdocs.py ask --company "NGTL" "What caused delays in their applications?"
   ```

7. **Use `pcmr` for monitoring-report questions.** For "what issues keep showing up in
   post-construction monitoring?", `pcmr` reads the reports' content and aggregates structured
   findings (compliance status, issue categories, unresolved items) — more reliable than
   free-form `ask` for this document type. See `docs/trends-and-estimation.md`.

---

## Citation Format

Sources in `ask` output show:

```
Sources:
  - Application for Pipeline Certificate (2025-03-15) pp.12-14
    [PDF; by Trans Mountain; CERA 183; Oil] (relevance: 0.87)
    https://apps.cer-rec.gc.ca/REGDOCS/Item/View/4510256
    region: p.12 bbox(72,494)→(542,456) (+2 more)
  - Order MO-001-2025 (2025-09-20) p.3
    [PDF; Commission; CERA 183; Oil] (relevance: 0.82)
    https://apps.cer-rec.gc.ca/REGDOCS/Item/View/4510301
```

Every source is cross-verifiable at three levels:
- **REGDOCS URL** — opens the original document on the CER's site
- **Page numbers** (`pp.12-14`) — where in the PDF the passage lives
- **Region** (`bbox(l,t)→(r,b)`, PDF points, bottom-left origin) — the exact rectangle on the
  page, resolved from the `.bbox.json` sidecar written at convert time. Only shown when the
  chunk's text can be matched back to sidecar items (tables and heavily reflowed text may not
  resolve).

Includes: document name, date, page numbers, kind, submitter, application type, commodity, and relevance score.
