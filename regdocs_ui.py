"""CER REGDOCS Web UI — Gradio-based interface to the RAG pipeline.

Enhanced with:
  - Example queries you can click to explore
  - Timeline visualization (filing Gantt charts)
  - Auto-discovery of interesting patterns (Explore tab)
  - Richer trends with Plotly charts

Launch with:
    python regdocs_ui.py

Then open http://localhost:7860 in your browser.
"""

import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "regdocs.db"
CHROMA_DIR = Path(__file__).parent / "chroma_db"
CHROMA_COLLECTION = "regdocs"
EMBED_MODEL = "nomic-embed-text"
LLM_MODEL = "gemma4:26b"



# ---------------------------------------------------------------------------
# Backend: Database helpers
# ---------------------------------------------------------------------------

def get_db_connection():
    """Get a read-only SQLite connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _query_filings_with_durations(
    conn,
    company: str = "",
    application_type: str = "",
    commodity: str = "",
    min_docs: int = 2,
) -> List[Dict[str, Any]]:
    """Query filing metrics from metadata. Returns list of filing dicts with durations."""
    rows = conn.execute(
        """SELECT json_extract(metadata, '$.filing_number') as fn,
                  json_extract(metadata, '$.company') as company,
                  json_extract(metadata, '$.date') as date,
                  json_extract(metadata, '$.application_types') as app_types,
                  json_extract(metadata, '$.commodities') as commodities,
                  json_extract(metadata, '$.document_types') as doc_types
           FROM documents
           WHERE metadata IS NOT NULL
             AND json_extract(metadata, '$.filing_number') IS NOT NULL
             AND json_extract(metadata, '$.filing_number') != ''
             AND json_extract(metadata, '$.date') IS NOT NULL"""
    ).fetchall()

    # Group by filing
    filings: Dict[str, List[Dict]] = {}
    for row in rows:
        fn = row["fn"]
        filings.setdefault(fn, []).append(dict(row))

    results = []
    for fn, docs in filings.items():
        dates = sorted(d["date"] for d in docs if d.get("date"))
        if len(dates) < min_docs:
            continue

        try:
            d1 = datetime.strptime(dates[0], "%Y-%m-%d")
            d2 = datetime.strptime(dates[-1], "%Y-%m-%d")
            duration = (d2 - d1).days
        except (ValueError, TypeError):
            continue

        companies = set()
        app_types_set = set()
        commodities_set = set()
        doc_types_set = set()

        for d in docs:
            if d.get("company"):
                companies.add(d["company"])
            try:
                for at in json.loads(d.get("app_types") or "[]"):
                    app_types_set.add(at)
            except (json.JSONDecodeError, TypeError):
                pass
            try:
                for c in json.loads(d.get("commodities") or "[]"):
                    commodities_set.add(c)
            except (json.JSONDecodeError, TypeError):
                pass
            try:
                for dt in json.loads(d.get("doc_types") or "[]"):
                    doc_types_set.add(dt)
            except (json.JSONDecodeError, TypeError):
                pass

        company_str = ", ".join(sorted(companies))

        # Apply filters
        if company and company.lower() not in company_str.lower():
            continue
        if application_type and not any(
            application_type.lower() in at.lower() for at in app_types_set
        ):
            continue
        if commodity and not any(
            commodity.lower() in c.lower() for c in commodities_set
        ):
            continue

        results.append({
            "filing": fn,
            "company": company_str,
            "date_start": dates[0],
            "date_end": dates[-1],
            "duration_days": duration,
            "doc_count": len(docs),
            "app_types": sorted(app_types_set),
            "commodities": sorted(commodities_set),
            "doc_types": sorted(doc_types_set),
        })

    return results



# ---------------------------------------------------------------------------
# Backend: Stats
# ---------------------------------------------------------------------------

def get_stats():
    """Get pipeline status summary."""
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM documents GROUP BY status"
    ).fetchall()
    status_counts = {r["status"]: r["cnt"] for r in rows}
    total = sum(status_counts.values())

    # Top companies
    top_companies = conn.execute(
        """SELECT json_extract(metadata, '$.company') as company, COUNT(*) as cnt
           FROM documents WHERE metadata IS NOT NULL
           GROUP BY company HAVING company IS NOT NULL AND company != ''
           ORDER BY cnt DESC LIMIT 15"""
    ).fetchall()

    # Date range
    date_range = conn.execute(
        """SELECT MIN(json_extract(metadata, '$.date')) as min_date,
                  MAX(json_extract(metadata, '$.date')) as max_date
           FROM documents WHERE metadata IS NOT NULL"""
    ).fetchone()

    # Filing count
    filing_count = conn.execute(
        """SELECT COUNT(DISTINCT json_extract(metadata, '$.filing_number')) as cnt
           FROM documents WHERE json_extract(metadata, '$.filing_number') IS NOT NULL
             AND json_extract(metadata, '$.filing_number') != ''"""
    ).fetchone()["cnt"]

    # Recent runs
    recent_runs = conn.execute(
        "SELECT stage, started_at, finished_at, summary FROM runs ORDER BY started_at DESC LIMIT 10"
    ).fetchall()

    conn.close()

    lines = ["# Pipeline Status\n"]
    lines.append(f"**{total:,} documents** across **{filing_count:,} filings** "
                 f"({date_range['min_date']} to {date_range['max_date']})\n")

    lines.append("## Document Counts\n")
    lines.append("| Status | Count | % |")
    lines.append("|--------|------:|---:|")
    for status in ["NEW", "DOWNLOADED", "CONVERTED", "FAILED"]:
        cnt = status_counts.get(status, 0)
        pct = (cnt / total * 100) if total > 0 else 0
        lines.append(f"| {status} | {cnt:,} | {pct:.1f}% |")
    lines.append(f"| **Total** | **{total:,}** | |")

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



# ---------------------------------------------------------------------------
# Backend: Ask (RAG query)
# ---------------------------------------------------------------------------

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
        return ("ChromaDB not found. Run `python regdocs.py index` first.\n\n"
                "The RAG pipeline requires indexed documents to answer questions."), ""

    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    try:
        collection = client.get_collection(name=CHROMA_COLLECTION)
    except Exception:
        return "Collection not found. Run `python regdocs.py index` first.", ""

    try:
        response = ollama_client.embed(model=EMBED_MODEL, input=question)
        query_embedding = response["embeddings"][0]
    except Exception as e:
        return f"Failed to embed question (is Ollama running with `{EMBED_MODEL}`?): {e}", ""

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

    if sort_by_date:
        combined = list(zip(chunks, metadatas, distances))
        combined.sort(key=lambda x: x[1].get("date", "") or "")
        if combined:
            chunks, metadatas, distances = zip(*combined)

    context_parts = []
    for i, (chunk, meta, dist) in enumerate(zip(chunks, metadatas, distances)):
        context_parts.append(f"[Source {i+1}] {chunk}")
    context = "\n\n".join(context_parts)

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

    user_prompt = (
        f"Context from CER REGDOCS documents:\n\n{context}\n\n---\n\n"
        f"Question: {question}\n\nAnswer based on the context above:"
    )

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

    # Build sources markdown
    source_lines = []
    for i, (meta, dist) in enumerate(zip(metadatas, distances)):
        relevance = f"{1-dist:.0%}" if dist is not None else "?"
        name = meta.get("document_name", "Unknown")
        date = meta.get("date", "")
        doc_id = meta.get("document_id", "")

        line = f"**[{relevance}]** {name}"
        if date:
            line += f" ({date})"
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

    confidence_note = ""
    avg_relevance = 1 - (sum(d for d in distances if d is not None) / max(len(distances), 1))
    if avg_relevance < 0.3:
        confidence_note = "⚠️ **Low confidence** — results may not be relevant.\n\n"

    return confidence_note + answer, "\n".join(source_lines)



# ---------------------------------------------------------------------------
# Backend: Timeline (Gantt chart)
# ---------------------------------------------------------------------------

def get_timeline_chart(company: str = "", application_type: str = "", commodity: str = "", top_n: int = 30):
    """Generate a Plotly Gantt chart of filing timelines."""
    import plotly.graph_objects as go
    from datetime import timedelta

    conn = get_db_connection()
    filings = _query_filings_with_durations(conn, company, application_type, commodity, min_docs=2)
    conn.close()

    if not filings:
        fig = go.Figure()
        fig.add_annotation(text="No filings match the filters (need 2+ documents with dates)",
                           xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    # Sort by start date descending, take top N
    filings.sort(key=lambda f: f["date_start"], reverse=True)
    filings = filings[:top_n]
    filings.reverse()  # oldest at top for Gantt

    colors = {
        "CERA 183": "#2196F3",
        "CERA 214": "#4CAF50",
        "CERA Tolls": "#FF9800",
        "s.52": "#F44336",
        "CERA 356": "#9C27B0",
        "CERA 241": "#795548",
        "CERA 181": "#607D8B",
        "CERA 327": "#009688",
    }

    # Build data for a horizontal bar chart using date start/end
    labels = []
    starts = []
    ends = []
    bar_colors = []
    hover_texts = []

    for f in filings:
        # Determine color by primary app type
        color = "#90A4AE"  # default grey
        for at in f["app_types"]:
            for key, c in colors.items():
                if key in at:
                    color = c
                    break
            if color != "#90A4AE":
                break

        label = f"{f['filing']} ({f['company'][:25]})" if f["company"] else f["filing"]
        hover = (
            f"<b>{f['filing']}</b><br>"
            f"Company: {f['company']}<br>"
            f"Duration: {f['duration_days']} days<br>"
            f"Documents: {f['doc_count']}<br>"
            f"Type: {', '.join(f['app_types'][:2])}<br>"
            f"Commodity: {', '.join(f['commodities'][:2])}"
        )

        labels.append(label)
        starts.append(f["date_start"])
        # Ensure at least 1 day width for same-day filings
        end_date = f["date_end"] if f["duration_days"] > 0 else (
            (datetime.strptime(f["date_start"], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        )
        ends.append(end_date)
        bar_colors.append(color)
        hover_texts.append(hover)

    fig = go.Figure()

    # Use one trace per bar (for individual colors)
    for i in range(len(labels)):
        start_dt = datetime.strptime(starts[i], "%Y-%m-%d")
        end_dt = datetime.strptime(ends[i], "%Y-%m-%d")
        duration_ms = (end_dt - start_dt).total_seconds() * 1000

        fig.add_trace(go.Bar(
            x=[duration_ms],
            y=[labels[i]],
            base=[starts[i]],
            orientation='h',
            marker=dict(color=bar_colors[i], opacity=0.8),
            hovertext=hover_texts[i],
            hoverinfo="text",
            showlegend=False,
            width=0.7,
        ))

    fig.update_layout(
        title="Filing Timelines (first document → last document)",
        xaxis_title="Date",
        yaxis_title="",
        height=max(400, len(filings) * 28 + 100),
        margin=dict(l=280, r=50, t=60, b=50),
        barmode='overlay',
        xaxis=dict(type='date'),
    )

    return fig



def get_timeline_table(company: str = "", application_type: str = "", commodity: str = "", top_n: int = 30):
    """Generate a markdown table of filing timelines sorted by duration."""
    conn = get_db_connection()
    filings = _query_filings_with_durations(conn, company, application_type, commodity, min_docs=2)
    conn.close()

    if not filings:
        return "No filings match the filters."

    # Sort by duration descending
    filings.sort(key=lambda f: -f["duration_days"])
    filings = filings[:top_n]

    lines = [f"**{len(filings)} filings** (sorted by duration, longest first)\n"]
    lines.append("| Filing | Company | Start | End | Days | Docs | Type |")
    lines.append("|--------|---------|-------|-----|-----:|-----:|------|")
    for f in filings:
        company_display = f["company"][:30] if f["company"] else ""
        app_type = f["app_types"][0][:25] if f["app_types"] else ""
        lines.append(
            f"| {f['filing']} | {company_display} | {f['date_start']} | {f['date_end']} "
            f"| {f['duration_days']} | {f['doc_count']} | {app_type} |"
        )

    return "\n".join(lines)



# ---------------------------------------------------------------------------
# Backend: Explore / Auto-discover patterns
# ---------------------------------------------------------------------------

def get_explore_data():
    """Auto-discover interesting patterns from the data without user input."""
    conn = get_db_connection()

    output_sections = []

    # --- Section 1: Filing volume over time (monthly) ---
    rows = conn.execute(
        """SELECT substr(json_extract(metadata, '$.date'), 1, 7) as month,
                  COUNT(*) as cnt
           FROM documents
           WHERE json_extract(metadata, '$.date') IS NOT NULL
           GROUP BY month ORDER BY month"""
    ).fetchall()

    if rows:
        output_sections.append("## 📅 Filing Volume by Month\n")
        output_sections.append("| Month | Documents | Trend |")
        output_sections.append("|-------|----------:|-------|")
        max_cnt = max(r["cnt"] for r in rows)
        for r in rows:
            bar_len = int(r["cnt"] / max_cnt * 20)
            bar = "█" * bar_len
            output_sections.append(f"| {r['month']} | {r['cnt']:,} | {bar} |")

    # --- Section 2: Fastest completed filings (with >5 docs) ---
    filings = _query_filings_with_durations(conn, min_docs=5)
    completed = [f for f in filings if f["duration_days"] > 0]

    if completed:
        fastest = sorted(completed, key=lambda f: f["duration_days"])[:10]
        output_sections.append("\n## ⚡ Fastest Filings (5+ documents, resolved quickly)\n")
        output_sections.append("| Filing | Company | Days | Docs | Type |")
        output_sections.append("|--------|---------|-----:|-----:|------|")
        for f in fastest:
            at = f["app_types"][0][:30] if f["app_types"] else ""
            output_sections.append(
                f"| {f['filing']} | {f['company'][:30]} | {f['duration_days']} "
                f"| {f['doc_count']} | {at} |"
            )

    # --- Section 3: Slowest filings (likely complex proceedings) ---
    if completed:
        slowest = sorted(completed, key=lambda f: -f["duration_days"])[:10]
        output_sections.append("\n## 🐢 Longest Filings (potentially complex proceedings)\n")
        output_sections.append("| Filing | Company | Days | Months | Docs | Type |")
        output_sections.append("|--------|---------|-----:|-------:|-----:|------|")
        for f in slowest:
            at = f["app_types"][0][:30] if f["app_types"] else ""
            months = f["duration_days"] / 30.0
            output_sections.append(
                f"| {f['filing']} | {f['company'][:30]} | {f['duration_days']} "
                f"| {months:.1f} | {f['doc_count']} | {at} |"
            )

    # --- Section 4: Most active submitters (who files the most?) ---
    top_submitters = conn.execute(
        """SELECT json_extract(metadata, '$.submitter') as submitter, COUNT(*) as cnt
           FROM documents WHERE metadata IS NOT NULL
           GROUP BY submitter HAVING submitter IS NOT NULL AND submitter != ''
           ORDER BY cnt DESC LIMIT 15"""
    ).fetchall()

    if top_submitters:
        output_sections.append("\n## 👤 Most Active Submitters\n")
        output_sections.append("| Submitter | Documents |")
        output_sections.append("|-----------|----------:|")
        for s in top_submitters:
            output_sections.append(f"| {s['submitter'][:50]} | {s['cnt']:,} |")

    # --- Section 5: Application type breakdown ---
    app_types = conn.execute(
        """SELECT json_extract(metadata, '$.application_types') as at, COUNT(*) as cnt
           FROM documents WHERE metadata IS NOT NULL AND at IS NOT NULL AND at != '[]'
           GROUP BY at ORDER BY cnt DESC LIMIT 12"""
    ).fetchall()

    if app_types:
        output_sections.append("\n## 📋 Application Types (document count)\n")
        output_sections.append("| Application Type | Documents | Share |")
        output_sections.append("|------------------|----------:|------:|")
        total_with_at = sum(r["cnt"] for r in app_types)
        for r in app_types:
            # Parse the JSON array to get a clean name
            try:
                names = json.loads(r["at"])
                name = ", ".join(names) if isinstance(names, list) else str(names)
            except (json.JSONDecodeError, TypeError):
                name = r["at"]
            pct = r["cnt"] / total_with_at * 100
            output_sections.append(f"| {name[:50]} | {r['cnt']:,} | {pct:.1f}% |")

    # --- Section 6: Duration comparison by company ---
    if completed:
        company_durations: Dict[str, List[int]] = {}
        for f in completed:
            if f["company"]:
                company_durations.setdefault(f["company"], []).append(f["duration_days"])

        # Companies with 3+ filings
        company_stats = []
        for company, durations in company_durations.items():
            if len(durations) >= 3:
                avg = sum(durations) / len(durations)
                med = sorted(durations)[len(durations) // 2]
                company_stats.append({
                    "company": company,
                    "count": len(durations),
                    "avg": avg,
                    "median": med,
                    "min": min(durations),
                    "max": max(durations),
                })

        if company_stats:
            company_stats.sort(key=lambda x: -x["avg"])
            output_sections.append("\n## 🏢 Filing Duration by Company (3+ filings)\n")
            output_sections.append(
                "| Company | Filings | Avg Days | Median | Fastest | Slowest |"
            )
            output_sections.append(
                "|---------|--------:|---------:|-------:|--------:|--------:|"
            )
            for cs in company_stats[:15]:
                output_sections.append(
                    f"| {cs['company'][:35]} | {cs['count']} | {cs['avg']:.0f} "
                    f"| {cs['median']} | {cs['min']} | {cs['max']} |"
                )

    conn.close()
    return "\n".join(output_sections)



def get_explore_volume_chart():
    """Generate a Plotly chart of filing volume over time."""
    import plotly.graph_objects as go

    conn = get_db_connection()
    rows = conn.execute(
        """SELECT substr(json_extract(metadata, '$.date'), 1, 7) as month,
                  COUNT(*) as cnt
           FROM documents
           WHERE json_extract(metadata, '$.date') IS NOT NULL
           GROUP BY month ORDER BY month"""
    ).fetchall()
    conn.close()

    if not rows:
        fig = go.Figure()
        fig.add_annotation(text="No data available", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False)
        return fig

    months = [r["month"] for r in rows]
    counts = [r["cnt"] for r in rows]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=months,
        y=counts,
        marker_color="#2196F3",
        opacity=0.8,
    ))
    fig.update_layout(
        title="Document Filing Volume by Month",
        xaxis_title="Month",
        yaxis_title="Documents Filed",
        height=350,
        margin=dict(l=50, r=30, t=60, b=50),
    )
    return fig


def get_explore_duration_chart():
    """Generate a box plot of filing durations by application type."""
    import plotly.graph_objects as go

    conn = get_db_connection()
    filings = _query_filings_with_durations(conn, min_docs=2)
    conn.close()

    completed = [f for f in filings if f["duration_days"] > 0]
    if not completed:
        fig = go.Figure()
        fig.add_annotation(text="No completed filings found", xref="paper", yref="paper",
                           x=0.5, y=0.5, showarrow=False)
        return fig

    # Group by primary app type
    by_type: Dict[str, List[int]] = {}
    for f in completed:
        if f["app_types"]:
            # Use shortened name
            full_name = f["app_types"][0]
            short = full_name[:30]
            by_type.setdefault(short, []).append(f["duration_days"])
        else:
            by_type.setdefault("Unknown", []).append(f["duration_days"])

    # Only show types with 3+ filings
    by_type = {k: v for k, v in by_type.items() if len(v) >= 3}

    if not by_type:
        fig = go.Figure()
        fig.add_annotation(text="Not enough data (need 3+ filings per type)",
                           xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    # Sort by median duration
    sorted_types = sorted(by_type.items(), key=lambda x: sorted(x[1])[len(x[1])//2], reverse=True)

    fig = go.Figure()
    for type_name, durations in sorted_types[:12]:
        fig.add_trace(go.Box(
            y=durations,
            name=type_name,
            boxmean=True,
        ))

    fig.update_layout(
        title="Filing Duration Distribution by Application Type",
        yaxis_title="Duration (days)",
        height=450,
        margin=dict(l=50, r=30, t=60, b=100),
        showlegend=False,
    )
    return fig



# ---------------------------------------------------------------------------
# Backend: Trends with estimation
# ---------------------------------------------------------------------------

def get_trends(application_type: str = "", commodity: str = "", company: str = ""):
    """Detailed trends analysis with duration statistics."""
    conn = get_db_connection()
    filings = _query_filings_with_durations(conn, company, application_type, commodity, min_docs=2)
    conn.close()

    completed = [f for f in filings if f["duration_days"] > 0]

    if not completed:
        return "No filings match the specified filters (or no filings with multi-day spans found)."

    durations = sorted(f["duration_days"] for f in completed)

    lines = [f"## Duration Analysis ({len(completed)} filings)\n"]
    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Mean | {sum(durations)/len(durations):.0f} days ({sum(durations)/len(durations)/30:.1f} months) |")
    lines.append(f"| Median | {durations[len(durations)//2]} days |")
    lines.append(f"| 25th percentile | {durations[len(durations)//4]} days |")
    lines.append(f"| 75th percentile | {durations[3*len(durations)//4]} days |")
    lines.append(f"| Min | {min(durations)} days |")
    lines.append(f"| Max | {max(durations)} days ({max(durations)/30:.1f} months) |")
    lines.append(f"| Avg docs/filing | {sum(f['doc_count'] for f in completed)/len(completed):.1f} |")

    # Duration by application type
    by_app_type: Dict[str, List[int]] = {}
    for f in completed:
        for at in f["app_types"]:
            by_app_type.setdefault(at, []).append(f["duration_days"])

    app_stats = [(at, durs) for at, durs in by_app_type.items() if len(durs) >= 3]
    if app_stats:
        app_stats.sort(key=lambda x: -(sum(x[1]) / len(x[1])))
        lines.append("\n## Duration by Application Type\n")
        lines.append("| Type | Count | Avg Days | Median | Max |")
        lines.append("|------|------:|---------:|-------:|----:|")
        for at, durs in app_stats[:15]:
            avg = sum(durs) / len(durs)
            med = sorted(durs)[len(durs) // 2]
            lines.append(f"| {at[:45]} | {len(durs)} | {avg:.0f} | {med} | {max(durs)} |")

    # Estimation
    if len(completed) >= 5:
        p25 = durations[len(durations) // 4]
        p50 = durations[len(durations) // 2]
        p75 = durations[3 * len(durations) // 4]
        avg = sum(durations) / len(durations)

        lines.append("\n## Duration Estimate (for new filing matching these filters)\n")
        lines.append("| Scenario | Estimate |")
        lines.append("|----------|---------|")
        lines.append(f"| Optimistic (25th pctile) | {p25} days ({p25/30:.1f} months) |")
        lines.append(f"| Typical (median) | {p50} days ({p50/30:.1f} months) |")
        lines.append(f"| Average | {avg:.0f} days ({avg/30:.1f} months) |")
        lines.append(f"| Pessimistic (75th pctile) | {p75} days ({p75/30:.1f} months) |")
        lines.append(f"| Worst case | {max(durations)} days ({max(durations)/30:.1f} months) |")

    # Longest filings table
    longest = sorted(completed, key=lambda f: -f["duration_days"])[:15]
    lines.append("\n## Longest Filings\n")
    lines.append("| Filing | Company | Duration | Docs | Type |")
    lines.append("|--------|---------|----------|-----:|------|")
    for f in longest:
        at = f["app_types"][0][:25] if f["app_types"] else ""
        lines.append(
            f"| {f['filing']} | {f['company'][:30]} | {f['duration_days']} days | {f['doc_count']} | {at} |"
        )

    return "\n".join(lines)



# ---------------------------------------------------------------------------
# Backend: Compliance gaps
# ---------------------------------------------------------------------------

def get_compliance_gaps(company_filter: str = ""):
    """Detect filings with Orders but no Compliance documents."""
    conn = get_db_connection()

    # Only load documents that have document_types containing "Order" or "Compliance"
    # This avoids loading all 30k documents when only a fraction are relevant
    rows = conn.execute(
        """SELECT id, name, metadata FROM documents
           WHERE metadata IS NOT NULL
             AND json_extract(metadata, '$.filing_number') IS NOT NULL
             AND json_extract(metadata, '$.filing_number') != ''
             AND (json_extract(metadata, '$.document_types') LIKE '%Order%'
               OR json_extract(metadata, '$.document_types') LIKE '%Compliance%')"""
    ).fetchall()

    filings: Dict[str, List] = {}
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


# ---------------------------------------------------------------------------
# Backend: Search
# ---------------------------------------------------------------------------

def search_documents(query: str, limit: int = 20):
    """Search documents using FTS5."""
    if not query.strip():
        return "Enter a search query (filing number, company name, or keywords)."

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
        # Fall back to LIKE search if FTS fails
        rows = conn.execute(
            """SELECT id as doc_id, name,
                      json_extract(metadata, '$.company') as company,
                      json_extract(metadata, '$.filing_number') as filing_number,
                      '' as snippet, 0 as rank
               FROM documents
               WHERE name LIKE ? OR json_extract(metadata, '$.company') LIKE ?
               LIMIT ?""",
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()

    conn.close()

    if not rows:
        return "No results found. Try different keywords."

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

# Example queries users can click to explore
EXAMPLE_QUERIES = [
    ["What conditions were imposed on Trans Mountain?", "Trans Mountain Pipeline ULC", "", "", "", "", True, 20],
    ["Compare NGTL and Enbridge filing timelines", "", "", "", "", "", True, 30],
    ["What commodities does Westcoast Energy transport?", "Westcoast Energy Inc.", "", "", "", "", False, 15],
    ["Show me recent pipeline certificate applications", "", "", "CERA 183", "", "", True, 20],
    ["What abandonment proceedings are underway?", "", "", "CERA 241", "", "", True, 15],
    ["Who filed tolls and tariffs applications for gas?", "", "", "CERA Tolls", "Gas", "", False, 20],
    ["What are the most common conditions in s.52 applications?", "", "", "s.52", "", "", False, 25],
    ["Show the history of Pouce Coupe filings", "Pouce Coupé Pipe Line Ltd", "", "", "", "", True, 20],
]


def build_ui():
    """Build the Gradio interface with enhanced discovery features."""
    import gradio as gr

    with gr.Blocks(
        title="CER REGDOCS",
        theme=gr.themes.Soft(),
        css="""
        .example-btn { margin: 2px !important; font-size: 0.85em !important; }
        .tab-content { min-height: 400px; }
        """
    ) as app:
        gr.Markdown("# 🇨🇦 CER REGDOCS Knowledge Base")
        gr.Markdown(
            "Search, analyze, and explore Canada Energy Regulator regulatory documents. "
            "**30,000+ documents** across **5,000+ filings** from major pipeline and energy companies."
        )

        with gr.Tabs():
            # =================================================================
            # EXPLORE TAB (first! — shows interesting stuff without asking)
            # =================================================================
            with gr.Tab("🔭 Explore"):
                gr.Markdown(
                    "### Auto-discovered patterns\n"
                    "No query needed — these patterns are computed from the full dataset."
                )
                with gr.Row():
                    explore_btn = gr.Button("🔄 Refresh Analysis", variant="primary")

                with gr.Row():
                    volume_chart = gr.Plot(label="Filing Volume Over Time")
                    duration_chart = gr.Plot(label="Duration by Application Type")

                explore_output = gr.Markdown(elem_classes=["tab-content"])

                def load_explore():
                    return (
                        get_explore_volume_chart(),
                        get_explore_duration_chart(),
                        get_explore_data(),
                    )

                explore_btn.click(
                    fn=load_explore,
                    outputs=[volume_chart, duration_chart, explore_output],
                )
                app.load(fn=load_explore, outputs=[volume_chart, duration_chart, explore_output])

            # =================================================================
            # TIMELINE TAB
            # =================================================================
            with gr.Tab("📊 Timeline"):
                gr.Markdown(
                    "### Filing Timelines\n"
                    "Visual Gantt chart showing how long filings take from first to last document. "
                    "Filter by company, application type, or commodity to compare."
                )
                with gr.Row():
                    tl_company = gr.Textbox(label="Company", placeholder="e.g., Trans Mountain")
                    tl_app_type = gr.Textbox(label="Application type", placeholder="e.g., CERA 183")
                    tl_commodity = gr.Dropdown(
                        label="Commodity",
                        choices=["", "Oil", "Gas", "Natural Gas", "Electricity"],
                        value="",
                    )
                    tl_top_n = gr.Slider(minimum=10, maximum=60, value=30, step=5, label="Show top N")
                with gr.Row():
                    tl_btn = gr.Button("Generate Timeline", variant="primary")

                tl_chart = gr.Plot(label="Filing Timeline (Gantt)")
                tl_table = gr.Markdown(label="Details")

                tl_btn.click(
                    fn=lambda c, a, co, n: (
                        get_timeline_chart(c, a, co, int(n)),
                        get_timeline_table(c, a, co, int(n)),
                    ),
                    inputs=[tl_company, tl_app_type, tl_commodity, tl_top_n],
                    outputs=[tl_chart, tl_table],
                )

            # =================================================================
            # ASK TAB (with examples)
            # =================================================================
            with gr.Tab("💬 Ask"):
                gr.Markdown(
                    "### Ask questions about regulatory documents\n"
                    "Uses RAG (retrieval-augmented generation) to answer from indexed documents. "
                    "**Requires `python regdocs.py index` to have been run.**"
                )

                # Example queries
                gr.Markdown("**Try these examples** (click to fill):")
                with gr.Row(elem_classes=["example-row"]):
                    example_btns = []
                    for i, ex in enumerate(EXAMPLE_QUERIES[:4]):
                        btn = gr.Button(ex[0][:50], size="sm", elem_classes=["example-btn"])
                        example_btns.append((btn, ex))
                with gr.Row(elem_classes=["example-row"]):
                    for i, ex in enumerate(EXAMPLE_QUERIES[4:8]):
                        btn = gr.Button(ex[0][:50], size="sm", elem_classes=["example-btn"])
                        example_btns.append((btn, ex))

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
                            label="Commodity",
                            choices=["", "Oil", "Gas", "Natural Gas", "Electricity"],
                            value="",
                        )
                        with gr.Row():
                            after_input = gr.Textbox(label="After", placeholder="YYYY-MM-DD", scale=1)
                            before_input = gr.Textbox(label="Before", placeholder="YYYY-MM-DD", scale=1)
                        sort_date = gr.Checkbox(label="Sort by date (timeline)", value=False)
                        top_k_slider = gr.Slider(minimum=5, maximum=50, value=15, step=5, label="Sources (top-k)")

                answer_output = gr.Markdown(label="Answer")
                sources_output = gr.Markdown(label="Sources")

                # Wire up Ask button
                ask_btn.click(
                    fn=ask_question,
                    inputs=[question_input, company_input, gr.State(""),
                            filing_input, app_type_input, commodity_input,
                            gr.State(""), after_input, before_input,
                            sort_date, top_k_slider],
                    outputs=[answer_output, sources_output],
                )

                # Wire up example buttons
                for btn, ex in example_btns:
                    btn.click(
                        fn=lambda e=ex: (e[0], e[1], e[3], e[4], e[5], e[6], e[7]),
                        outputs=[question_input, company_input, app_type_input,
                                 commodity_input, filing_input, sort_date, top_k_slider],
                    )

            # =================================================================
            # TRENDS TAB
            # =================================================================
            with gr.Tab("📈 Trends"):
                gr.Markdown(
                    "### Filing Duration Analysis\n"
                    "How long do filings take? Compare by company, application type, or commodity. "
                    "Includes duration estimates for new filings."
                )
                with gr.Row():
                    trends_company = gr.Textbox(label="Company", placeholder="e.g., Enbridge")
                    trends_app_type = gr.Textbox(label="Application type", placeholder="e.g., CERA 183")
                    trends_commodity = gr.Dropdown(
                        label="Commodity",
                        choices=["", "Oil", "Gas", "Natural Gas", "Electricity"],
                        value="",
                    )
                trends_btn = gr.Button("Analyze", variant="primary")
                trends_output = gr.Markdown(elem_classes=["tab-content"])

                trends_btn.click(
                    fn=get_trends,
                    inputs=[trends_app_type, trends_commodity, trends_company],
                    outputs=[trends_output],
                )

            # =================================================================
            # SEARCH TAB
            # =================================================================
            with gr.Tab("🔍 Search"):
                gr.Markdown("### Keyword Search\nSearch by filing number, company name, or keywords.")
                search_input = gr.Textbox(
                    label="Search",
                    placeholder="e.g., Trans Mountain, C37815, abandonment...",
                )
                search_btn = gr.Button("Search", variant="primary")
                search_output = gr.Markdown(elem_classes=["tab-content"])

                search_btn.click(fn=search_documents, inputs=[search_input], outputs=[search_output])

            # =================================================================
            # COMPLIANCE TAB
            # =================================================================
            with gr.Tab("⚠️ Compliance"):
                gr.Markdown(
                    "### Compliance Gap Detection\n"
                    "Filings with Orders issued but no Compliance documents filed — potential gaps."
                )
                compliance_company = gr.Textbox(
                    label="Company filter (optional)", placeholder="e.g., Trans Mountain"
                )
                compliance_btn = gr.Button("Check Gaps", variant="primary")
                compliance_output = gr.Markdown(elem_classes=["tab-content"])

                compliance_btn.click(
                    fn=get_compliance_gaps,
                    inputs=[compliance_company],
                    outputs=[compliance_output],
                )

            # =================================================================
            # DASHBOARD TAB
            # =================================================================
            with gr.Tab("⚙️ Dashboard"):
                gr.Markdown("### Pipeline Status\nOverview of the data pipeline and system health.")
                stats_btn = gr.Button("Refresh", variant="primary")
                stats_output = gr.Markdown(elem_classes=["tab-content"])

                stats_btn.click(fn=get_stats, outputs=[stats_output])
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

    try:
        import plotly
    except ImportError:
        print("Plotly not installed. Run: pip install plotly")
        sys.exit(1)

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run the pipeline first.")
        sys.exit(1)

    print(f"Starting CER REGDOCS UI on http://{args.host}:{args.port}")
    app = build_ui()
    app.launch(server_name=args.host, server_port=args.port, share=args.share)
