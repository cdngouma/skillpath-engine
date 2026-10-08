
from pathlib import Path

import pytest

from skillpath.ingestion.provider import JobRecord
from skillpath.pipelines.ingest import ingest_jobs
from skillpath.storage.repository import DuckDBRepository


SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "sql"
    / "schema.sql"
)


class FakeProvider:
    source = "adzuna"

    def fetch_jobs(self, query: str, max_pages: int = 1):
        yield JobRecord(
            source=FakeProvider.source,
            source_job_id="123",
            title="Machine Learning Engineer",
            search_term=query,
            raw_payload={"id": "123"},
        )


def test_ingestion_preserves_query_provenance(tmp_path):
    with DuckDBRepository(
        tmp_path / "test.duckdb",
        SCHEMA_PATH,
    ) as repository:
        processed = ingest_jobs(
            provider=FakeProvider(),
            repository=repository,
            search_queries={
                "ml_engineer": [
                    "machine learning engineer",
                    "ml engineer",
                ]
            },
            max_pages=1,
            batch_size=1,
        )

        jobs = repository.db.execute(
            "SELECT COUNT(*) FROM skillpath.jobs_raw"
        ).fetchone()[0]

        matches = repository.db.execute(
            "SELECT COUNT(*) FROM skillpath.job_search_matches"
        ).fetchone()[0]

        status = repository.db.execute(
            "SELECT status FROM skillpath.ingestion_runs"
        ).fetchone()[0]

        source = repository.db.execute(
            "SELECT source FROM skillpath.ingestion_runs"
        ).fetchone()[0]

        assert processed == 2
        assert jobs == 1
        assert matches == 2
        assert source == "adzuna"
        assert status == "completed"


class FailingProvider:
    source = "adzuna"

    def fetch_jobs(self, query: str, max_pages: int = 1):
        raise RuntimeError("Simulated API failure")
        yield  # Unreachable, but makes this a generator function


def test_ingestion_records_failure(tmp_path):
    with DuckDBRepository(
        tmp_path / "test.duckdb",
        SCHEMA_PATH,
    ) as repository:
        with pytest.raises(
            RuntimeError,
            match="Simulated API failure",
        ):
            ingest_jobs(
                provider=FailingProvider(),
                repository=repository,
                search_queries={"ml_engineer": ["ml engineer"]},
            )

        result = repository.db.execute(
            """
            SELECT status, records_processed, error_message
            FROM skillpath.ingestion_runs
            """
        ).fetchone()

        assert result[0] == "failed"
        assert result[1] == 0
        assert "Simulated API failure" in result[2]
