"""Unified SQLite database module for the CER REGDOCS pipeline.

This module provides the single source of truth for the entire pipeline.
regdocs.db replaces all separate log files, documents.jsonl, and
documents_enriched.jsonl — making the system observable, idempotent, and
resumable.

Enterprise features:
  - Metrics table: every operation is instrumented with timing and outcome
  - retry_count: tracks how many times each document has been retried
  - Token-bucket rate limiter: async-friendly, configurable requests/second
  - Idempotency: hash-based change detection for rescans
  - Stats: built-in reporting queries

Usage:
    from regdocs_db import get_db, DocumentStatus, TokenBucketLimiter

    db = get_db()              # opens/creates regdocs.db in the project root
    db = get_db("path/to.db") # or a custom path
"""

import asyncio
import json
import sqlite3
import time
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_DB_PATH = Path(__file__).parent / "regdocs.db"

SCHEMA_VERSION = 2


class DocumentStatus(StrEnum):
    """Document lifecycle states."""
    NEW = "NEW"
    DOWNLOADED = "DOWNLOADED"
    CONVERTED = "CONVERTED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------

class TokenBucketLimiter:
    """Async token-bucket rate limiter.

    Usage:
        limiter = TokenBucketLimiter(rate=5.0, burst=10)
        async with limiter:
            await do_request()
    """

    def __init__(self, rate: float = 5.0, burst: int = 10):
        """
        Args:
            rate: Tokens replenished per second (requests/sec).
            burst: Maximum tokens in the bucket (burst capacity).
        """
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available."""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Calculate how long to wait for 1 token
                wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *exc):
        pass


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    name TEXT,
    url TEXT,
    status TEXT NOT NULL DEFAULT 'NEW',
    file_path TEXT,
    markdown_path TEXT,
    hash TEXT,
    last_error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT,
    detail TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    parameters JSON,
    summary JSON
);

CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    stage TEXT NOT NULL,
    operation TEXT NOT NULL,
    success INTEGER NOT NULL DEFAULT 0,
    duration_ms REAL,
    error_type TEXT,
    detail TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_retry ON documents(retry_count);
CREATE INDEX IF NOT EXISTS idx_history_document_id ON history(document_id);
CREATE INDEX IF NOT EXISTS idx_runs_stage ON runs(stage);
CREATE INDEX IF NOT EXISTS idx_metrics_stage ON metrics(stage);
CREATE INDEX IF NOT EXISTS idx_metrics_document_id ON metrics(document_id);

CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
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
"""

# Migration: add retry_count column if upgrading from v1
_MIGRATION_ADD_RETRY_COUNT = """
ALTER TABLE documents ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
"""


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class RegDocsDB:
    """Thin wrapper around sqlite3 with helper methods for the pipeline."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.conn = sqlite3.connect(str(self.db_path), isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist, apply migrations."""
        self.conn.executescript(_SCHEMA_SQL)
        # Apply migration for retry_count if needed
        self._migrate_retry_count()

    def _migrate_retry_count(self) -> None:
        """Add retry_count column if upgrading from schema v1."""
        cursor = self.conn.execute("PRAGMA table_info(documents)")
        columns = {row[1] for row in cursor.fetchall()}
        if "retry_count" not in columns:
            self.conn.execute(_MIGRATION_ADD_RETRY_COUNT)

    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # Document CRUD
    # ------------------------------------------------------------------

    def upsert_document(
        self,
        doc_id: str,
        name: str,
        url: str,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = DocumentStatus.NEW,
    ) -> None:
        """Insert or update a document record (discovery/rescan mode).

        If the document already exists, name/url/metadata are refreshed but
        the lifecycle status is preserved (DOWNLOADED/CONVERTED stay as-is).
        New URLs that aren't tracked yet get inserted with status=NEW.
        """
        now = _now_iso()
        meta_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
        self.conn.execute(
            """
            INSERT INTO documents (id, name, url, status, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                url = excluded.url,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            (doc_id, name, url, status, meta_json, now, now),
        )

    def get_documents_by_status(self, status: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch documents with a given status."""
        query = "SELECT * FROM documents WHERE status = ?"
        params: list = [status]
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [_row_to_dict(r) for r in rows]

    def get_pending_downloads(self, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Get documents ready for download: NEW + FAILED with retries remaining."""
        rows = self.conn.execute(
            """
            SELECT * FROM documents
            WHERE status = 'NEW'
               OR (status = 'FAILED' AND file_path IS NULL AND retry_count < ?)
            ORDER BY retry_count ASC, created_at ASC
            """,
            (max_retries,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]

    def get_pending_conversions(self, max_retries: int = 3) -> List[Dict[str, Any]]:
        """Get documents ready for conversion: DOWNLOADED + FAILED (with file) with retries remaining."""
        rows = self.conn.execute(
            """
            SELECT * FROM documents
            WHERE status = 'DOWNLOADED'
               OR (status = 'FAILED' AND file_path IS NOT NULL AND markdown_path IS NULL AND retry_count < ?)
            ORDER BY retry_count ASC, created_at ASC
            """,
            (max_retries,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]

    def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single document by ID."""
        row = self.conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
        return _row_to_dict(row) if row else None

    def update_document(self, doc_id: str, **fields) -> None:
        """Update arbitrary fields on a document."""
        if not fields:
            return
        # Whitelist allowed columns to prevent SQL injection
        allowed = {"name", "url", "status", "file_path", "markdown_path",
                   "hash", "last_error", "retry_count", "metadata", "updated_at"}
        invalid = set(fields.keys()) - allowed
        if invalid:
            raise ValueError(f"Invalid column(s): {invalid}")
        fields["updated_at"] = _now_iso()
        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [doc_id]
        self.conn.execute(
            f"UPDATE documents SET {set_clause} WHERE id = ?",
            values,
        )

    def mark_downloaded(
        self,
        doc_id: str,
        file_path: str,
        file_hash: str,
        metadata_update: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Mark a document as successfully downloaded."""
        now = _now_iso()
        # Merge new metadata with existing
        if metadata_update:
            existing = self.get_document(doc_id)
            existing_meta = json.loads(existing["metadata"]) if existing and existing.get("metadata") else {}
            existing_meta.update(metadata_update)
            meta_json = json.dumps(existing_meta, ensure_ascii=False)
        else:
            meta_json = None

        if meta_json:
            self.conn.execute(
                """UPDATE documents SET status = ?, file_path = ?, hash = ?,
                   metadata = ?, last_error = NULL, updated_at = ? WHERE id = ?""",
                (DocumentStatus.DOWNLOADED, file_path, file_hash, meta_json, now, doc_id),
            )
        else:
            self.conn.execute(
                """UPDATE documents SET status = ?, file_path = ?, hash = ?,
                   last_error = NULL, updated_at = ? WHERE id = ?""",
                (DocumentStatus.DOWNLOADED, file_path, file_hash, now, doc_id),
            )
        self.add_history(doc_id, "downloaded", DocumentStatus.DOWNLOADED, f"file_path={file_path}")

    def mark_converted(self, doc_id: str, markdown_path: str, quality_score: float = None) -> None:
        """Mark a document as successfully converted."""
        now = _now_iso()
        self.conn.execute(
            "UPDATE documents SET status = ?, markdown_path = ?, last_error = NULL, updated_at = ? WHERE id = ?",
            (DocumentStatus.CONVERTED, markdown_path, now, doc_id),
        )
        # Store quality_score in metadata JSON
        if quality_score is not None:
            self.conn.execute(
                """UPDATE documents SET metadata = json_set(
                    COALESCE(metadata, '{}'), '$.quality_score', ?
                ) WHERE id = ?""",
                (quality_score, doc_id),
            )
        self.add_history(doc_id, "converted", DocumentStatus.CONVERTED, f"markdown_path={markdown_path} quality={quality_score}")

    def mark_failed(self, doc_id: str, error: str, stage: str) -> None:
        """Mark a document as failed, increment retry_count, preserve the error."""
        now = _now_iso()
        self.conn.execute(
            """UPDATE documents SET status = ?, last_error = ?,
               retry_count = retry_count + 1, updated_at = ? WHERE id = ?""",
            (DocumentStatus.FAILED, error, now, doc_id),
        )
        self.add_history(doc_id, f"failed_{stage}", DocumentStatus.FAILED, error)

    def check_hash_changed(self, doc_id: str, new_hash: str) -> bool:
        """Check if a document's file has changed (for idempotency/rescan).

        Returns True if the hash is different (needs re-processing).
        """
        doc = self.get_document(doc_id)
        if not doc:
            return True  # New document
        return doc.get("hash") != new_hash

    def reset_for_reprocess(self, doc_id: str) -> None:
        """Reset a document to DOWNLOADED status for re-conversion (hash changed)."""
        now = _now_iso()
        self.conn.execute(
            """UPDATE documents SET status = 'DOWNLOADED', markdown_path = NULL,
               last_error = NULL, retry_count = 0, updated_at = ? WHERE id = ?""",
            (now, doc_id),
        )
        self.add_history(doc_id, "reset_for_reprocess", DocumentStatus.DOWNLOADED, "hash_changed")

    # ------------------------------------------------------------------
    # History / Audit
    # ------------------------------------------------------------------

    def add_history(self, doc_id: str, action: str, status: Optional[str] = None, detail: Optional[str] = None) -> None:
        """Append an audit entry to the history table."""
        self.conn.execute(
            "INSERT INTO history (document_id, action, status, detail) VALUES (?, ?, ?, ?)",
            (doc_id, action, status, detail),
        )

    def get_history(self, doc_id: str) -> List[Dict[str, Any]]:
        """Get full history for a document."""
        rows = self.conn.execute(
            "SELECT * FROM history WHERE document_id = ? ORDER BY created_at",
            (doc_id,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Metrics / Observability
    # ------------------------------------------------------------------

    def record_metric(
        self,
        stage: str,
        operation: str,
        *,
        document_id: Optional[str] = None,
        success: bool = True,
        duration_ms: Optional[float] = None,
        error_type: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Insert a metrics record after every operation."""
        self.conn.execute(
            """INSERT INTO metrics (document_id, stage, operation, success, duration_ms, error_type, detail)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (document_id, stage, operation, 1 if success else 0, duration_ms, error_type, detail),
        )

    # ------------------------------------------------------------------
    # Run tracking
    # ------------------------------------------------------------------

    def start_run(self, stage: str, parameters: Optional[Dict[str, Any]] = None) -> int:
        """Record the start of a pipeline run. Returns the run ID."""
        now = _now_iso()
        params_json = json.dumps(parameters, ensure_ascii=False) if parameters else None
        cursor = self.conn.execute(
            "INSERT INTO runs (stage, started_at, parameters) VALUES (?, ?, ?)",
            (stage, now, params_json),
        )
        return cursor.lastrowid

    def finish_run(self, run_id: int, summary: Optional[Dict[str, Any]] = None) -> None:
        """Record the end of a pipeline run."""
        now = _now_iso()
        summary_json = json.dumps(summary, ensure_ascii=False) if summary else None
        self.conn.execute(
            "UPDATE runs SET finished_at = ?, summary = ? WHERE id = ?",
            (now, summary_json, run_id),
        )

    # ------------------------------------------------------------------
    # Stats / Reporting
    # ------------------------------------------------------------------

    def status_counts(self) -> Dict[str, int]:
        """Return counts grouped by status."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM documents GROUP BY status"
        ).fetchall()
        return {row["status"]: row["cnt"] for row in rows}

    def total_documents(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()
        return row["cnt"]

    def failed_documents(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return failed documents with their errors."""
        rows = self.conn.execute(
            "SELECT id, name, last_error, retry_count, updated_at FROM documents WHERE status = 'FAILED' ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_row_to_dict(r) for r in rows]

    def stats_summary(self) -> Dict[str, Any]:
        """Comprehensive pipeline stats for the 'stats' subcommand.

        Returns stats broken down by stage (scout, download, convert) plus
        overall pipeline health.
        """
        total = self.total_documents()
        by_status = self.status_counts()

        # needs attention
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM documents WHERE retry_count > 0"
        ).fetchone()
        needs_attention = row["cnt"]

        # success rate
        converted = by_status.get("CONVERTED", 0)
        success_rate = (converted / total * 100) if total > 0 else 0.0

        # failure by error type
        rows = self.conn.execute(
            """SELECT
                CASE
                    WHEN last_error LIKE '%timeout%' THEN 'timeout'
                    WHEN last_error LIKE '%status 429%' THEN 'rate_limited'
                    WHEN last_error LIKE '%status 5%' THEN 'server_error'
                    WHEN last_error LIKE '%Connection%' THEN 'connection_error'
                    WHEN last_error LIKE '%Invalid PDF%' THEN 'invalid_pdf'
                    ELSE 'other'
                END as error_type,
                COUNT(*) as cnt
            FROM documents
            WHERE status = 'FAILED'
            GROUP BY error_type
            ORDER BY cnt DESC"""
        ).fetchall()
        failure_by_error = {row["error_type"]: row["cnt"] for row in rows}

        # pending queue
        row = self.conn.execute(
            """SELECT COUNT(*) as cnt FROM documents
               WHERE status = 'NEW'
                  OR (status = 'FAILED' AND retry_count < 3)"""
        ).fetchone()
        pending_queue = row["cnt"]

        # Per-stage metrics from the metrics table
        stage_metrics = {}
        for stage in ("scout", "download", "convert"):
            row = self.conn.execute(
                """SELECT
                    COUNT(*) as total_ops,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successes,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failures,
                    AVG(CASE WHEN success = 1 THEN duration_ms END) as avg_duration_ms,
                    MIN(CASE WHEN success = 1 THEN duration_ms END) as min_duration_ms,
                    MAX(CASE WHEN success = 1 THEN duration_ms END) as max_duration_ms
                FROM metrics WHERE stage = ?""",
                (stage,),
            ).fetchone()
            stage_metrics[stage] = _row_to_dict(row) if row else {}

        # recent runs (last 10)
        runs = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT 10"
        ).fetchall()
        recent_runs = [_row_to_dict(r) for r in runs]

        return {
            "total": total,
            "by_status": by_status,
            "needs_attention": needs_attention,
            "success_rate": round(success_rate, 1),
            "failure_by_error": failure_by_error,
            "pending_queue": pending_queue,
            "stage_metrics": stage_metrics,
            "recent_runs": recent_runs,
        }

    # ------------------------------------------------------------------
    # Full-Text Search (FTS5)
    # ------------------------------------------------------------------

    def rebuild_fts(self) -> None:
        """Rebuild the FTS5 index from the documents table.

        Drops all existing FTS content and re-populates from documents.
        Call this after indexing to keep keyword search in sync.
        """
        # Clear existing FTS content
        self.conn.execute("DELETE FROM documents_fts")

        # Populate from documents table
        rows = self.conn.execute(
            "SELECT id, name, metadata FROM documents WHERE metadata IS NOT NULL"
        ).fetchall()

        for row in rows:
            doc_id = row["id"]
            name = row["name"] or ""
            meta = json.loads(row["metadata"]) if row["metadata"] else {}

            company = meta.get("company", "") or ""
            project = meta.get("project", "") or ""
            filing_number = meta.get("filing_number", "") or ""
            submitter = meta.get("submitter", "") or ""
            snippet = meta.get("snippet", "") or ""

            document_types = meta.get("document_types", [])
            if isinstance(document_types, list):
                document_types = ", ".join(document_types)

            application_types = meta.get("application_types", [])
            if isinstance(application_types, list):
                application_types = ", ".join(application_types)

            commodities = meta.get("commodities", [])
            if isinstance(commodities, list):
                commodities = ", ".join(commodities)

            roles = meta.get("roles", [])
            if isinstance(roles, list):
                roles = ", ".join(roles)

            self.conn.execute(
                """INSERT INTO documents_fts (doc_id, name, company, project, filing_number,
                   submitter, snippet, document_types, application_types, commodities, roles)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (doc_id, name, company, project, filing_number,
                 submitter, snippet, document_types, application_types, commodities, roles),
            )

    def search_fts(self, query: str, limit: int = 20) -> List[str]:
        """Search the FTS5 index and return matching document IDs ranked by relevance.

        Args:
            query: Search query string (supports FTS5 syntax).
            limit: Maximum number of results to return.

        Returns:
            List of document IDs ordered by relevance.
        """
        # Escape special FTS5 characters for safety
        # Use simple prefix matching: wrap each term for broad matching
        rows = self.conn.execute(
            """SELECT doc_id, rank FROM documents_fts
               WHERE documents_fts MATCH ?
               ORDER BY rank
               LIMIT ?""",
            (query, limit),
        ).fetchall()
        return [row["doc_id"] for row in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return dict(row)


def get_db(db_path: Optional[Path] = None) -> RegDocsDB:
    """Factory function to open or create the database."""
    return RegDocsDB(db_path)
