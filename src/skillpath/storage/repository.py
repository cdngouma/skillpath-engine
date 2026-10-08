
from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from uuid import uuid4

import duckdb

from skillpath.ingestion.provider import JobRecord


class DuckDBRepository:
    def __init__(self, db_path: Path, schema_path: Path):
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.connection: duckdb.DuckDBPyConnection | None = None

    def __enter__(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(self.db_path))

        # Executes the CREATE SCHEMA / CREATE TABLE statements.
        self.connection.execute(
            self.schema_path.read_text(encoding="utf-8")
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    @property
    def db(self) -> duckdb.DuckDBPyConnection:
        if self.connection is None:
            raise RuntimeError("Repository is not open")
        return self.connection

    def upsert_raw_jobs(
        self,
        records: Sequence[JobRecord],
        canonical_role: str,
    ) -> int:
        """
        Persist one batch atomically.

        Returns the number of records processed, NOT
        the number of newly inserted jobs.
        """
        if not records:
            return 0

        if not canonical_role.strip():
            raise ValueError("canonical_role cannot be empty")

        self.db.execute("BEGIN TRANSACTION")

        try:
            for job in records:
                self.db.execute(
                    """
                    INSERT INTO skillpath.jobs_raw (
                        source,
                        source_job_id,
                        title,
                        company,
                        location,
                        posted_at,
                        description,
                        url,
                        salary_min,
                        salary_max,
                        raw_payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (source, source_job_id)
                    DO UPDATE SET
                        title = EXCLUDED.title,
                        company = EXCLUDED.company,
                        location = EXCLUDED.location,
                        posted_at = EXCLUDED.posted_at,
                        description = EXCLUDED.description,
                        url = EXCLUDED.url,
                        salary_min = EXCLUDED.salary_min,
                        salary_max = EXCLUDED.salary_max,
                        raw_payload = EXCLUDED.raw_payload,
                        last_seen_at = NOW()
                    """,
                    [
                        job.source,
                        job.source_job_id,
                        job.title,
                        job.company,
                        job.location,
                        job.posted_at,
                        job.description,
                        job.url,
                        job.salary_min,
                        job.salary_max,
                        json.dumps(job.raw_payload),
                    ],
                )

                self.db.execute(
                    """
                    INSERT INTO skillpath.job_search_matches (
                        source,
                        source_job_id,
                        canonical_role,
                        search_term
                    )
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (
                        source,
                        source_job_id,
                        canonical_role,
                        search_term
                    )
                    DO UPDATE SET
                        last_matched_at = NOW()
                    """,
                    [
                        job.source,
                        job.source_job_id,
                        canonical_role,
                        job.search_term,
                    ],
                )

            self.db.execute("COMMIT")

        except Exception:
            self.db.execute("ROLLBACK")
            raise

        return len(records)

    def start_ingestion_run(self, source: str) -> str:
        run_id = str(uuid4())

        self.db.execute(
            """
            INSERT INTO skillpath.ingestion_runs (
                run_id, source, status
            )
            VALUES (?, ?, 'running')
            """,
            [run_id, source],
        )
        return run_id

    def finish_ingestion_run(
        self,
        run_id: str,
        status: str,
        records_processed: int,
        error_message: str | None = None,
    ) -> None:
        if status not in ("completed", "failed"):
            raise ValueError("Invalid final ingestion status")

        self.db.execute(
            """
            UPDATE skillpath.ingestion_runs
            SET
                finished_at = NOW(),
                status = ?,
                records_processed = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            [
                status,
                records_processed,
                error_message,
                run_id,
            ],
        )
