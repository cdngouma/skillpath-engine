
from pathlib import Path

import pytest

from skillpath.ingestion.provider import JobRecord
from skillpath.storage.repository import DuckDBRepository


SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "sql"
    / "schema.sql"
)


@pytest.fixture
def repository(tmp_path):
    db_path = tmp_path / "test.duckdb"

    with DuckDBRepository(db_path, SCHEMA_PATH) as repo:
        yield repo


@pytest.fixture
def sample_job():
    return JobRecord(
        source="adzuna",
        source_job_id="123",
        title="Machine Learning Engineer",
        company="Bell",
        location="Montreal",
        search_term="machine learning engineer",
        raw_payload={"id": "123"},
    )


def count_rows(repository, table):
    return repository.db.execute(
        f"SELECT COUNT(*) FROM skillpath.{table}"
    ).fetchone()[0]


def test_insert_raw_job(repository, sample_job):
    processed = repository.upsert_raw_jobs(
        [sample_job],
        canonical_role="ml_engineer",
    )

    assert processed == 1
    assert count_rows(repository, "jobs_raw") == 1
    assert count_rows(repository, "job_search_matches") == 1


def test_duplicate_ingestion_is_idempotent(
    repository, sample_job
):
    repository.upsert_raw_jobs(
        [sample_job], canonical_role="ml_engineer"
    )
    repository.upsert_raw_jobs(
        [sample_job], canonical_role="ml_engineer"
    )

    assert count_rows(repository, "jobs_raw") == 1
    assert count_rows(repository, "job_search_matches") == 1


def test_multiple_queries_preserved(
    repository, sample_job
):
    repository.upsert_raw_jobs(
        [sample_job], canonical_role="ml_engineer"
    )

    second_match = sample_job.model_copy(
        update={"search_term": "ml engineer"}
    )

    repository.upsert_raw_jobs(
        [second_match], canonical_role="ml_engineer"
    )

    assert count_rows(repository, "jobs_raw") == 1
    assert count_rows(repository, "job_search_matches") == 2


def test_existing_job_updated(repository, sample_job):
    repository.upsert_raw_jobs(
        [sample_job], canonical_role="ml_engineer"
    )

    updated = sample_job.model_copy(
        update={"title": "Senior ML Engineer"}
    )

    repository.upsert_raw_jobs(
        [updated], canonical_role="ml_engineer"
    )

    title = repository.db.execute(
        """
        SELECT title
        FROM skillpath.jobs_raw
        WHERE source = 'adzuna'
          AND source_job_id = '123'
        """
    ).fetchone()[0]

    assert title == "Senior ML Engineer"
    assert count_rows(repository, "jobs_raw") == 1


def test_batch_rollback(repository, sample_job):
    invalid = sample_job.model_copy(
        update={
            "source_job_id": "456",
            "title": None,
        }
    )

    with pytest.raises(Exception):
        repository.upsert_raw_jobs(
            [sample_job, invalid],
            canonical_role="ml_engineer",
        )

    assert count_rows(repository, "jobs_raw") == 0
    assert count_rows(repository, "job_search_matches") == 0


def test_empty_batch(repository):
    assert repository.upsert_raw_jobs(
        [], canonical_role="ml_engineer"
    ) == 0


def test_ingestion_run_lifecycle(repository):
    run_id = repository.start_ingestion_run("adzuna")

    repository.finish_ingestion_run(
        run_id=run_id,
        status="completed",
        records_processed=10,
    )

    result = repository.db.execute(
        """
        SELECT status, records_processed, finished_at
        FROM skillpath.ingestion_runs
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()

    assert result[0] == "completed"
    assert result[1] == 10
    assert result[2] is not None
