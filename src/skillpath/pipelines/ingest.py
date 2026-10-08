
import logging
from collections.abc import Iterable
from itertools import islice

from skillpath.ingestion.provider import JobProvider
from skillpath.storage.repository import DuckDBRepository

logger = logging.getLogger(__name__)


def batched(items: Iterable, batch_size: int):
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    iterator = iter(items)

    while batch := list(islice(iterator, batch_size)):
        yield batch


def ingest_jobs(
    provider: JobProvider,
    repository: DuckDBRepository,
    search_queries: dict[str, list[str]],
    max_pages: int = 5,
    batch_size: int = 100,
) -> int:
    if max_pages < 1:
        raise ValueError("max_pages must be positive")

    processed = 0
    run_id = repository.start_ingestion_run(provider.source)

    try:
        for canonical_role, queries in search_queries.items():
            for query in queries:
                logger.info(
                    "Ingesting role=%s query=%s",
                    canonical_role,
                    query,
                )

                records = provider.fetch_jobs(
                    query=query,
                    max_pages=max_pages,
                )

                for batch in batched(records, batch_size):
                    processed += repository.upsert_raw_jobs(
                        batch,
                        canonical_role=canonical_role,
                    )

        repository.finish_ingestion_run(
            run_id=run_id,
            status="completed",
            records_processed=processed,
        )

    except Exception as exc:
        logger.exception("Ingestion failed")

        repository.finish_ingestion_run(
            run_id=run_id,
            status="failed",
            records_processed=processed,
            error_message=str(exc),
        )
        raise

    return processed
