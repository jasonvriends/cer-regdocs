"""CER REGDOCS Web UI — Gradio-based interface to the RAG pipeline.

Launch with:
    python regdocs_ui.py

Then open http://localhost:7860 in your browser (or share over your network).
"""

import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "regdocs.db"
CHROMA_DIR = Path(__file__).parent / "chroma_db"
CHROMA_COLLECTION = "regdocs"
EMBED_MODEL = "nomic-embed-text"
LLM_MODEL = "gemma4:26b"


# ---------------------------------------------------------------------------
# Backend functions (thin wrappers around the pipeline logic)
# ---------------------------------------------------------------------------

def get_db_connection():
    """Get a read-only SQLite connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_stats():
    """Get pipeline status summary."""
    conn = get_db_connection()
    rows = conn.execute("SELECT status, COUNT(*) as cnt FROM documents GROUP BY status").fetchall()
    status_counts = {r["status"]: r["cnt"] for r in rows}
    total = sum(status_counts.values())

    # Recent metrics
    recent_ops = conn.execute(
        "SELECT stage, COUNT(*) as cnt, AVG(duration_ms) as avg_ms, "
        "SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) as successes "
        "FROM metrics WHERE created_at > datetime('now', '-24 hours') "
        "GROUP BY stage"
    ).fetchall()

    # Top companies
    top_companies = conn.execute(
        """SELECT json_extract(metadata, '$.company') as company, COUNT(*) as cnt
           FROM documents WHERE metadata IS NOT NULL
           GROUP BY company HAVING company IS NOT NULL AND company != ''
           ORDER BY cnt DESC LIMIT 15"""
    ).fetchall()

    # Recent runs
    recent_runs = conn.execute(
        "SELECT stage, started_at, finished_at, summary FROM runs ORDER BY started_at DESC LIMIT 10"
    ).fetchall()

    conn.close()

    # Format output
    lines = ["# Pipeline Status\n"]
    lines.append("## Document Counts\n")
    lines.append("| Status | Count | % |")
    lines.append("|--------|------:|---:|")
    for status in ["NEW", "DOWNLOADED", "CONVERTED", "FAILED"]:
        cnt = status_counts.get(status, 0)
        pct = (cnt / total * 100) if total > 0 else 0
        lines.append(f"| {status} | {cnt:,} | {pct:.1f}% |")
    lines.append(f"| **Total** | **{total:,}** | |")

    if recent_ops:
        lines.append("\n## Last 24 Hours\n")
        lines.append("| Stage | Operations | Avg Time | Success Rate |")
        lines.append("|-------|----------:|----------:|-------------:|")
        for op in recent_ops:
            avg_ms = op["avg_ms"] or 0
            rate = (op["successes"] / op["cnt"] * 100) if op["cnt"] > 0 else 0
            lines.append(f"| {op['stage']} | {op['cnt']} | {avg_ms:.0f}ms | {rate:.0f}% |")

    if top_companies:
        lines.append("\n## Top Companies\n")
        lines.append("| Company | Documents |")
        lines.append("|---------|----------:|")
        for c in top_companies:
            lines.append(f"| {c['company']} | {c['cnt']:,} |")

    if recent_runs:
        lines.append("\n## Recent Runs\n")
        lines.append("| Stage | Started | Duration |")
        lines.append("|-------|---------|----------|")
        for run in recent_runs:
            started = run["started_at"][:16] if run["started_at"] else "?"
            if run["started_at"] and run["finished_at"]:
                try:
                    t1 = datetime.fromisoformat(run["started_at"].replace("Z", "+00:00"))
                    t2 = datetime.fromisoformat(run["finished_at"].replace("Z", "+00:00"))
                    dur = str(t2 - t1).split(".")[0]
                except Exception:
                    dur = "?"
            else:
                dur = "running..."
            lines.append(f"| {run['stage']} | {started} | {dur} |")

    return "\n".join(lines)


def ask_question(
    question: str,
    company: str = "",
    project: str = "",
    filing: str = "",
    application_type: str = "",
    commodity: str = "",
    document_type: str = "",
    after: str = "",
    before: str = "",
    sort_by_date: bool = False,
    top_k: int = 15,
    model: str = LLM_MODEL,
):
    """Run the ask pipeline and return answer + sources."""
    import chromadb
    import ollama as ollama_client

    if not question.strip():
        return "Please enter a question.", ""

    if not CHROMA_DIR.exists():
        return "ChromaDB not found. Run `python regdocs.py index` first.", ""

    # Connect
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    try:
        collection = client.get_collection(name=CHROMA_COLLECTION)
    except Exception:
        return "Collection not found. Run `python regdocs.py index` first.", ""

    # Embed question
    try:
        response = ollama_client.embed(model=EMBED_MODEL, input=question)
        query_embedding = response["embeddings"][0]
    except Exception as e:
        return f"Failed to embed question (is Ollama running?): {e}", ""

    # Build filters
    where_clauses = []
    if company:
        where_clauses.append({"company": company})
    if project:
        where_clauses.append({"project": project})
    if filing:
        where_clauses.append({"filing_number": filing})
    if after:
        where_clauses.append({"date": {"$gte": after}})
    if before:
        where_clauses.append({"date": {"$lte": before}})

    doc_clauses = []
    if application_type:
        doc_clauses.append({"$contains": application_type})
    if commodity:
        doc_clauses.append({"$contains": commodity})
    if document_type:
        doc_clauses.append({"$contains": document_type})

    where_filter = None
    if len(where_clauses) == 1:
        where_filter = where_clauses[0]
    elif len(where_clauses) > 1:
        where_filter = {"$and": where_clauses}

    where_document = None
    if len(doc_clauses) == 1:
        where_document = doc_clauses[0]
    elif len(doc_clauses) > 1:
        where_document = {"$and": doc_clauses}

    query_kwargs = {"query_embeddings": [query_embedding], "n_results": top_k}
    if where_filter:
        query_kwargs["where"] = where_filter
    if where_document:
        query_kwargs["where_document"] = where_document

    results = collection.query(**query_kwargs)

    if not results["documents"] or not results["documents"][0]:
        return "No relevant documents found. Try broadening your filters.", ""

    chunks = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0] if results.get("distances") else [0.5] * len(chunks)

    # Sort by date if requested
    if sort_by_date:
        combined = list(zip(chunks, metadatas, distances))
        combined.sort(key=lambda x: x[1].get("date", "") or "")
        if combined:
            chunks, metadatas, distances = zip(*combined)

    # Build context
    context_parts = []
    for i, (chunk, meta, dist) in enumerate(zip(chunks, metadatas, distances)):
        context_parts.append(f"[Source {i+1}] {chunk}")

    context = "\n\n".join(context_parts)

    # System prompt
    system_prompt = (
        "You are an expert analyst of Canada Energy Regulator (CER) regulatory documents. "
        "Use ONLY the provided context to answer. Each source includes metadata such as company, "
        "project, filing number, date, document type, application type, commodity, and role.\n\n"
        "When answering:\n"
        "- For timeline questions: organize information chronologically.\n"
        "- For comparative questions: compare side-by-side.\n"
        "- Always cite which source(s) you used by number.\n"
        "- If the context doesn't contain enough information, say what's missing."
    )

    if sort_by_date:
        system_prompt += "\n\nResults are sorted chronologically. Present information as a timeline."

    user_prompt = f"Context from CER REGDOCS documents:\n\n{context}\n\n---\n\nQuestion: {question}\n\nAnswer based on the context above:"

    # Query LLM
    try:
        response = ollama_client.chat(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            keep_alive="5m",
        )
        answer = response["message"]["content"]
    except Exception as e:
        return f"LLM query failed (is Ollama running? Is `{model}` pulled?): {e}", ""

    # Build sources
    source_lines = []
    for i, (meta, dist) in enumerate(zip(metadatas, distances)):
        relevance = f"{1-dist:.0%}" if dist is not None else "?"
        name = meta.get("document_name", "Unknown")
        date = meta.get("date", "")
        doc_id = meta.get("document_id", "")
        page_start = meta.get("page_start")
        page_end = meta.get("page_end")

        line = f"**[{relevance}]** {name}"
        if date:
            line += f" ({date})"
        if page_start and page_end:
            if page_start == page_end:
                line += f" p.{page_start}"
            else:
                line += f" pp.{page_start}-{page_end}"

        details = []
        if meta.get("kind"):
            details.append(meta["kind"])
        if meta.get("application_types"):
            details.append(meta["application_types"])
        if details:
            line += f" — {'; '.join(details)}"

        if doc_id:
            line += f"  \n[View on REGDOCS](https://apps.cer-rec.gc.ca/REGDOCS/Item/View/{doc_id})"

        source_lines.append(f"{i+1}. {line}")

    # Confidence warning
    confidence_note = ""
    avg_relevance = 1 - (sum(d for d in distances if d is not None) / max(len(distances), 1))
    if avg_relevance < 0.3:
        confidence_note = "⚠️ **Low confidence** — results may not be relevant to your question.\n\n"
    elif len(chunks) < 3:
        confidence_note = "⚠️ **Limited data** — only a few matching passages found.\n\n"

    sources_md = "\n".join(source_lines)
    return confidence_note + answer, sources_md


def get_compliance_gaps(company_filter: str = ""):
    """Run compliance gap detection."""
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT id, name, metadata FROM documents WHERE metadata IS NOT NULL"
    ).fetchall()

    filings = {}
    for row in rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        fn = meta.get("filing_number", "")
        if fn:
            filings.setdefault(fn, []).append(meta)

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
            gaps.append({
                "filing": fn,
                "company": ", ".join(sorted(companies)),
                "dates": f"{dates[0]} to {dates[-1]}" if dates else "",
                "docs": len(docs),
            })

    conn.close()

    if company_filter:
        gaps = [g for g in gaps if company_filter.lower() in g["company"].lower()]

    gaps.sort(key=lambda g: g["dates"], reverse=True)

    lines = [f"Found **{len(gaps)}** filings with Orders but no Compliance documents.\n"]
    lines.append("| Filing | Company | Date Range | Docs |")
    lines.append("|--------|---------|------------|-----:|")
    for g in gaps[:50]:
        lines.append(f"| {g['filing']} | {g['company'][:40]} | {g['dates']} | {g['docs']} |")
    if len(gaps) > 50:
        lines.append(f"\n*...and {len(gaps)-50} more*")

    return "\n".join(lines)


def get_trends(application_type: str = "", commodity: str = "", company: str = ""):
    """Run trends analysis."""
    conn = get_db_connection()
    rows = conn.execute(
        """SELECT id, name, metadata FROM documents
           WHERE metadata IS NOT NULL
             AND json_extract(metadata, '$.filing_number') IS NOT NULL"""
    ).fetchall()

    filings = {}
    for row in rows:
        meta = json.loads(row["metadata"]) if row["metadata"] else {}
        fn = meta.get("filing_number", "")
        if fn:
            filings.setdefault(fn, []).append(meta)

    conn.close()

    # Compute per-filing metrics
    filing_metrics = []
    for fn, docs in filings.items():
        dates = sorted([d["date"] for d in docs if d.get("date")])
        if len(dates) < 2:
            continue

        try:
            d1 = datetime.strptime(dates[0], "%Y-%m-%d")
            d2 = datetime.strptime(dates[-1], "%Y-%m-%d")
            duration = (d2 - d1).days
        except (ValueError, TypeError):
            continue

        if duration <= 0:
            continue

        companies = set(d.get("company", "") for d in docs if d.get("company"))
        app_types = set()
        commodities_set = set()
        for d in docs:
            for at in (d.get("application_types") or []):
                app_types.add(at)
            for c in (d.get("commodities") or []):
                commodities_set.add(c)

        filing_metrics.append({
            "filing": fn,
            "company": ", ".join(sorted(companies)),
            "duration": duration,
            "docs": len(docs),
            "app_types": sorted(app_types),
            "commodities": sorted(commodities_set),
        })

    # Apply filters
    if application_type:
        filing_metrics = [f for f in filing_metrics
                         if any(application_type.lower() in at.lower() for at in f["app_types"])]
    if commodity:
        filing_metrics = [f for f in filing_metrics
                         if any(commodity.lower() in c.lower() for c in f["commodities"])]
    if company:
        filing_metrics = [f for f in filing_metrics if company.lower() in f["company"].lower()]

    if not filing_metrics:
        return "No filings match the specified filters (or no filings with multi-day spans found)."

    durations = [f["duration"] for f in filing_metrics]
    durations.sort()

    lines = [f"## Duration Analysis ({len(filing_metrics)} filings)\n"]
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Mean | {sum(durations)/len(durations):.0f} days |")
    lines.append(f"| Median | {durations[len(durations)//2]} days |")
    lines.append(f"| 25th percentile | {durations[len(durations)//4]} days |")
    lines.append(f"| 75th percentile | {durations[3*len(durations)//4]} days |")
    lines.append(f"| Min | {min(durations)} days |")
    lines.append(f"| Max | {max(durations)} days |")

    lines.append("\n## Longest Filings\n")
    lines.append("| Filing | Company | Duration | Docs |")
    lines.append("|--------|---------|----------|-----:|")
    for f in sorted(filing_metrics, key=lambda x: -x["duration"])[:15]:
        lines.append(f"| {f['filing']} | {f['company'][:30]} | {f['duration']} days | {f['docs']} |")

    return "\n".join(lines)


def search_documents(query: str, limit: int = 20):
    """Search documents using FTS5."""
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """SELECT doc_id, name, company, filing_number, snippet,
                      rank FROM documents_fts
               WHERE documents_fts MATCH ?
               ORDER BY rank LIMIT ?""",
            (query, limit),
        ).fetchall()
    except Exception:
        rows = []

    conn.close()

    if not rows:
        return "No results found."

    lines = [f"Found **{len(rows)}** matching documents.\n"]
    lines.append("| # | Document | Company | Filing |")
    lines.append("|---|----------|---------|--------|")
    for i, r in enumerate(rows, 1):
        name = (r["name"] or "")[:50]
        company = (r["company"] or "")[:25]
        filing = r["filing_number"] or ""
        doc_id = r["doc_id"]
        link = f"[{name}](https://apps.cer-rec.gc.ca/REGDOCS/Item/View/{doc_id})"
        lines.append(f"| {i} | {link} | {company} | {filing} |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Gradio UI
# ---------------------------------------------------------------------------

def build_ui():
    """Build the Gradio interface."""
    import gradio as gr

    with gr.Blocks(
        title="CER REGDOCS",
        theme=gr.themes.Soft(),
    ) as app:
        gr.Markdown("# 🇨🇦 CER REGDOCS Knowledge Base")
        gr.Markdown("Search and analyze Canada Energy Regulator regulatory documents.")

        with gr.Tabs():
            # --- Ask Tab ---
            with gr.Tab("💬 Ask"):
                with gr.Row():
                    with gr.Column(scale=3):
                        question_input = gr.Textbox(
                            label="Question",
                            placeholder="What conditions did CER impose on Trans Mountain?",
                            lines=2,
                        )
                        ask_btn = gr.Button("Ask", variant="primary")
                    with gr.Column(scale=1):
                        company_input = gr.Textbox(label="Company filter", placeholder="e.g., Trans Mountain Pipeline ULC")
                        filing_input = gr.Textbox(label="Filing filter", placeholder="e.g., C37815")
                        app_type_input = gr.Textbox(label="Application type", placeholder="e.g., Section 52")
                        commodity_input = gr.Dropdown(
                            label="Commodity", choices=["", "Oil", "Gas", "Electricity"], value=""
                        )
                        with gr.Row():
                            after_input = gr.Textbox(label="After", placeholder="YYYY-MM-DD", scale=1)
                            before_input = gr.Textbox(label="Before", placeholder="YYYY-MM-DD", scale=1)
                        sort_date = gr.Checkbox(label="Sort by date (timeline)")
                        top_k_slider = gr.Slider(minimum=5, maximum=50, value=15, step=5, label="Sources (top-k)")

                answer_output = gr.Markdown(label="Answer")
                sources_output = gr.Markdown(label="Sources")

                ask_btn.click(
                    fn=ask_question,
                    inputs=[question_input, company_input, gr.Textbox(visible=False, value=""),
                            filing_input, app_type_input, commodity_input,
                            gr.Textbox(visible=False, value=""), after_input, before_input,
                            sort_date, top_k_slider],
                    outputs=[answer_output, sources_output],
                )

            # --- Search Tab ---
            with gr.Tab("🔍 Search"):
                search_input = gr.Textbox(
                    label="Keyword Search",
                    placeholder="Search by filing number, company name, or keywords...",
                )
                search_btn = gr.Button("Search", variant="primary")
                search_output = gr.Markdown()

                search_btn.click(fn=search_documents, inputs=[search_input], outputs=[search_output])

            # --- Compliance Tab ---
            with gr.Tab("⚠️ Compliance"):
                gr.Markdown("### Compliance Gap Detection\nFilings with Orders issued but no Compliance documents filed.")
                compliance_company = gr.Textbox(label="Company filter (optional)", placeholder="e.g., Trans Mountain")
                compliance_btn = gr.Button("Check Compliance Gaps", variant="primary")
                compliance_output = gr.Markdown()

                compliance_btn.click(
                    fn=get_compliance_gaps, inputs=[compliance_company], outputs=[compliance_output]
                )

            # --- Trends Tab ---
            with gr.Tab("📈 Trends"):
                gr.Markdown("### Filing Duration Analysis\nHow long do filings take, by type and commodity?")
                with gr.Row():
                    trends_app_type = gr.Textbox(label="Application type", placeholder="e.g., CERA 183")
                    trends_commodity = gr.Dropdown(
                        label="Commodity", choices=["", "Oil", "Gas", "Electricity"], value=""
                    )
                    trends_company = gr.Textbox(label="Company", placeholder="e.g., Enbridge")
                trends_btn = gr.Button("Analyze Trends", variant="primary")
                trends_output = gr.Markdown()

                trends_btn.click(
                    fn=get_trends,
                    inputs=[trends_app_type, trends_commodity, trends_company],
                    outputs=[trends_output],
                )

            # --- Stats Tab ---
            with gr.Tab("📊 Dashboard"):
                stats_btn = gr.Button("Refresh Stats", variant="primary")
                stats_output = gr.Markdown()

                stats_btn.click(fn=get_stats, outputs=[stats_output])
                # Auto-load on tab view
                app.load(fn=get_stats, outputs=[stats_output])

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CER REGDOCS Web UI")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=7860, help="Port to listen on")
    parser.add_argument("--share", action="store_true", help="Create a public Gradio link")
    args = parser.parse_args()

    # Check dependencies
    try:
        import gradio
    except ImportError:
        print("Gradio not installed. Run: pip install gradio")
        sys.exit(1)

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run the pipeline first.")
        sys.exit(1)

    print(f"Starting CER REGDOCS UI on http://{args.host}:{args.port}")
    app = build_ui()
    app.launch(server_name=args.host, server_port=args.port, share=args.share)
