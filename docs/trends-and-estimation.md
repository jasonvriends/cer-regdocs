# Trends & Duration Estimation

Analyze filing patterns and predict how long a new application might take, based on historical data.

---

## Overview

The `trends` command works purely from SQLite metadata — no LLM or ChromaDB needed. It computes statistics across filings to identify patterns and estimate timelines.

```bash
python regdocs.py trends
python regdocs.py trends --application-type "CERA 183" --estimate
```

---

## What It Measures

**Filing duration** = date of first document → date of last document in that filing.

This captures the full lifecycle: application → information requests → responses → supplemental info → order/decision.

---

## Command Usage

### Basic Analysis

```bash
# Full overview
python regdocs.py trends

# By application type
python regdocs.py trends --application-type "CERA 183"
python regdocs.py trends --application-type "Tolls"
python regdocs.py trends --application-type "s.52"

# By commodity
python regdocs.py trends --commodity "Gas"
python regdocs.py trends --commodity "Oil"

# By company
python regdocs.py trends --company "Trans Mountain"
python regdocs.py trends --company "Enbridge"

# By document type — only filings that contain at least one document of this type
python regdocs.py trends --document-type "Post Construction Monitoring Report"
python regdocs.py trends --document-type "Compliance"
```

### Duration Estimation

```bash
# How long for a new CERA 183 pipeline certificate?
python regdocs.py trends --application-type "CERA 183" --estimate

# How long for a gas tolls & tariffs filing?
python regdocs.py trends --application-type "Tolls" --commodity "Gas" --estimate

# How long for this company historically?
python regdocs.py trends --company "Westcoast Energy" --estimate

# Combine multiple filters for best estimate
python regdocs.py trends --application-type "CERA 214" --commodity "Oil" --company "Trans Mountain" --estimate
```

---

## Report Sections

### Duration Statistics

```
Mean duration:   45 days (1.5 months)
Median duration: 28 days
Shortest:        3 days
Longest:         540 days
Mean documents:  12.3 docs per filing
```

### Duration by Application Type

Shows which application types take longest, with count and median:

```
Application Type                                   Count  Avg Days  Median
s.52 Facilities                                       23       380     320
CERA 183 Pipeline Facilities Certificate             156        45      28
CERA 214 Pipeline Facilities Order                    89        14       8
CERA Tolls & Tariffs                                 201        22      12
```

### Duration by Commodity

```
Commodity            Count  Avg Days  Median
Gas                     87        34      18
Oil                    142        28      14
Electricity             34        45      30
```

### Complexity Indicators

Factors that correlate with longer proceedings:

```
Information Requests:
  With IRs:    85 days avg (47 filings)
  Without IRs: 22 days avg (312 filings)
  IRs add:     +286% to duration

Participant diversity (3+ roles vs <3):
  3+ roles:    94 days avg (28 filings)
  <3 roles:    24 days avg (331 filings)

Document volume (>12 docs vs <=12):
  High volume: 78 days avg (89 filings)
  Low volume:  18 days avg (270 filings)
```

### Duration Estimate (with `--estimate`)

```
Based on 23 comparable filings:
  • Application type: s.52
  • Commodity: Gas

Estimated duration:
  Optimistic (25th pctile):  180 days (6.0 months)
  Typical (median):          320 days (10.7 months)
  Average:                   380 days (12.7 months)
  Pessimistic (75th pctile): 520 days (17.3 months)
  Worst case (max):          1460 days (48.7 months)

Adjustment factors (from historical data):
  If IRs expected:           ×3.86
  If 3+ participant roles:   ×3.92
```

---

## Interpreting the Results

### What Drives Duration

Based on regulatory practice, filings take longer when:

| Factor | Why | Impact |
|--------|-----|--------|
| **Information Requests** | CER needs more info from applicant; back-and-forth cycle | 2-4× longer |
| **Intervenors** | Additional parties file evidence, cross-examine | 2-4× longer |
| **Hearing** | Oral or written hearing ordered | 3-6× longer |
| **Facilities applications** | Environmental assessment, routing, conditions | Longest category |
| **Multiple commodities** | Cross-commodity applications are rarer, more complex | 1.5-2× longer |
| **Compliance cycle** | Post-decision condition compliance adds calendar time | +3-12 months |

### What Doesn't Affect Duration Much

- Filing month (no strong seasonal pattern)
- PDF vs HTML format
- File size (more a function of type than complexity)

### Limitations

1. **Incomplete filings** — active proceedings show shorter durations than they'll eventually have
2. **Data depth** — need 5+ years of history for good estimates on multi-year proceedings
3. **External factors** — court challenges, policy changes, and government priorities aren't in the data
4. **Measurement point** — we measure first-to-last document, not specifically application-to-decision

---

## How It Works in ChromaDB

Filing summary chunks include duration data so the LLM can reason about patterns:

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
```

ChromaDB metadata includes:
- `duration_days` (filterable integer)
- `has_ir` (boolean)
- `has_hearing` (boolean)
- `document_count` (integer)

This means you can also ask the LLM about patterns:

```bash
python regdocs.py ask "Based on filing summaries, which application types \
  typically take longest? What's the average duration for CERA 183 vs s.52?"

python regdocs.py ask --application-type "Section 52" \
  "What factors seem to correlate with longer proceedings?"
```

---

## PCMR: Post Construction Monitoring Report Trends

`trends` only looks at *metadata* (dates, counts, types). The `pcmr` command goes one level
deeper: it reads the *content* of Post Construction Monitoring Reports and extracts what the
reports actually say.

### What it does, step by step

1. **Finds reports** — queries SQLite for converted documents whose `document_types`
   includes "Post Construction Monitoring Report" (configurable via `--document-type`).
2. **Reads each one** — loads the Markdown that the `convert` stage already produced.
   (No OCR or PDF work happens here — it reuses the existing conversion output.)
3. **Extracts findings via LLM** — sends the text to the Ollama model with a fixed
   extraction schema. The model returns JSON:
   - `compliance_status` — Compliant / Non-Compliant / Partially Compliant / Unknown
   - `issue_categories` — from a fixed list (Erosion and Sediment Control, Vegetation and
     Reclamation, Wildlife and Wetlands, Soil, Drainage and Watercourse Crossings,
     Landowner and Access, Noise, Other)
   - `findings` — individual issues with severity (Minor/Moderate/Major) and a
     resolved/unresolved flag
   - `summary` — 1-3 sentence plain-English outcome
4. **Caches the result** — stored in the document's `metadata.pcmr_analysis`, keyed by the
   file's content hash. Re-running `pcmr` is instant for already-analyzed reports; only new
   or changed reports hit the LLM. Use `--force` to re-extract everything.
5. **Aggregates trends** — counts across all analyzed reports:
   - Compliance status breakdown
   - Most common issue categories
   - Compliance rate by year
   - Companies with the most flagged issues
   - Unresolved **Major** findings, each with a REGDOCS link

### Usage

```bash
# Analyze all PCMRs and show the trend report
python regdocs.py pcmr

# Scope it
python regdocs.py pcmr --company "Trans Mountain" --after 2024-01-01

# Quick test on a few reports before committing to a long run
python regdocs.py pcmr --limit 5

# Per-report spreadsheet instead of the aggregate report
python regdocs.py pcmr --csv > pcmr_findings.csv

# Re-analyze everything (e.g., after switching models)
python regdocs.py pcmr --force --model gemma4:26b
```

### Caveats

- Reports must be **converted first** (`regdocs.py convert`) — `pcmr` reads Markdown, not PDFs.
- Extraction quality depends on the LLM. Very long reports are truncated to ~32K characters
  (head + tail kept, middle dropped) before extraction.
- Occasionally the local model emits malformed JSON and the report is skipped (a warning shows
  the skip count). Re-running usually succeeds — cached successes are never re-attempted.
- Trend counts are only as good as your corpus: if you've only scouted one year, "compliance
  rate by year" will have one row. Backfill more history for real trends (see below).

---

## Building Better Estimates

### Backfill Historical Data

For meaningful multi-year estimates, collect historical filings:

```bash
# 10 years of data (will take several hours with polite delays)
python regdocs.py scout --start-date 2015-01-01 --end-date 2025-12-31

# Then download, convert, and index
python regdocs.py download
python regdocs.py convert
python regdocs.py index --force
```

### What You'll Get

With 10 years of data:
- Section 52 facilities applications (3-5 year proceedings)
- Major hearings (Trans Mountain, Coastal GasLink, Energy East)
- Hundreds of completed CERA 183/214 applications
- Complete tolls & tariffs cycles
- Abandonment proceedings
- International power line applications

### Recommended Data Strategy

1. **Start recent** — collect 2024-2026 for quick results
2. **Backfill by year** — add 2023, 2022, ... as time allows
3. **Focus on major types** — s.52, CERA 183, CERA 214 for pipeline work
4. **Re-run trends** after each backfill to see estimates improve
