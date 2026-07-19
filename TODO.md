# TODO — Feature Roadmap

Future enhancements for the CER REGDOCS pipeline, roughly in priority order.

---

## Web UI Enhancements

- [ ] **User authentication** — Add OAuth or basic auth so the UI can be shared securely. Gradio supports OAuth natively, or put behind nginx/Caddy with auth.
- [ ] **Saved queries** — Let users save and name frequently-used filter combinations. Store in a `saved_queries` SQLite table.
- [ ] **Bulk export from UI** — Add download buttons for CSV/PDF export of any analysis (compliance gaps, trends, search results).
- [ ] **Filing timeline visualization** — Gantt-chart or timeline view showing filing progress over time. Use Plotly in Gradio.
- [ ] **PDF viewer integration** — When citations reference page numbers, link to an embedded PDF viewer (PDF.js) showing the exact page — and highlight the source region. The data for this already exists: `convert` writes a `<name>.bbox.json` sidecar next to each Markdown file with per-item page numbers, bounding boxes (PDF points, origin noted), page dimensions, and text snippets for matching chunks back to page regions.
- [ ] **Dark mode / theming** — Respect system preference.

---

## Analysis Features

- [ ] **Condition tracking table** — Extract conditions from Orders at index time into a structured `conditions` table (condition_id, filing, text, status, due_date). Show compliance status in the UI.
- [ ] **Notification / alerting** — Alert when new filings match saved criteria (e.g., new docs for a specific company or filing). Requires a daemon or cron hook.
- [ ] **Scheduled reports** — Email weekly compliance gap reports or filing summaries. Cron + sendmail/SES integration.
- [ ] **Filing timeline Gantt chart** — Visual representation of filing lifecycles (application → IRs → hearing → order → compliance).
- [ ] **Cross-filing comparison** — Side-by-side comparison of two filings' metadata, conditions, and timelines.
- [ ] **Auto-categorization** — Use LLM at index time to tag documents with higher-level labels (approval, denial, amendment, routine) beyond what REGDOCS provides.
- [ ] **Anomaly detection** — Flag filings that are taking unusually long compared to similar historical filings.

---

## Search & RAG Improvements

- [ ] **Multi-hop retrieval** — For complex questions, retrieve once → identify entities → retrieve again filtered to those entities for deeper answers.
- [ ] **Graph relationships** — Track parent/child document relationships (application → conditions → compliance) for true timeline traversal.
- [ ] **Re-ranking** — After initial retrieval, use a cross-encoder model to re-rank chunks by relevance before sending to the LLM.
- [ ] **Conversation memory** — Allow follow-up questions in the web UI without repeating context (session-based chat).
- [ ] **Chunk quality filtering** — Automatically skip chunks from low-quality OCR documents in search results.
- [ ] **Embedding model fine-tuning** — Fine-tune nomic-embed-text on regulatory terminology for better retrieval precision.

---

## Data & Pipeline

- [ ] **Historical backfill** — Crawl REGDOCS back to 2010+ for better duration estimates on multi-year proceedings (Section 52, major hearings).
- [ ] **Incremental FTS rebuild** — Currently rebuilds from scratch; switch to incremental inserts when new documents are indexed.
- [ ] **Document change detection** — On rescan, detect when a previously-downloaded document has been updated (hash comparison) and re-process.
- [ ] **Parallel convert** — GPU-aware batching for Docling conversion (currently sequential per document).
- [ ] **Cloud deployment** — Dockerize the pipeline + UI for deployment on AWS/GCP. Consider S3 for downloads, RDS for SQLite replacement at scale.

---

## API & Integration

- [ ] **REST API** — FastAPI endpoint alongside Gradio for programmatic access to search, ask, trends, compliance.
- [ ] **Webhook support** — POST notifications to Slack/Teams when new filings match criteria.
- [ ] **CSV/JSON API output** — Structured API responses for integration with other tools (Power BI, Tableau, Excel).
- [ ] **OpenAI-compatible endpoint** — Wrap the RAG pipeline in an OpenAI-compatible chat API for use with other clients.

---

## Documentation

- [ ] **Video walkthrough** — Screen recording of the pipeline and UI in action.
- [ ] **Deployment guide** — How to run on a server, set up systemd services, configure nginx.
- [ ] **Data dictionary** — Complete reference of all metadata fields, their sources, and valid values.
- [ ] **Contributing guide** — How to add new commands, new facet categories, or support new document formats.
