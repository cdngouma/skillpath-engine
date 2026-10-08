
import argparse
import logging

from skillpath.config import PROJECT_ROOT, load_config
from skillpath.ingestion.adzuna import AdzunaProvider
from skillpath.pipelines.ingest import ingest_jobs
from skillpath.storage.repository import DuckDBRepository

from dotenv import load_dotenv


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    
    parser = argparse.ArgumentParser(
        description="SkillPath job market pipeline"
    )

    parser.add_argument(
        "command",
        choices=["ingest"],
        help="Pipeline to execute",
    )

    parser.add_argument(
        "--role",
        type=str,
        default=None,
        help="Canonical role to ingest (e.g. ml_engineer). Default: all roles",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum Adzuna pages per search query",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of jobs per database batch",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config()

    search_queries = config.search_queries

    if args.role:
        if args.role not in search_queries:
            parser.error(
                f"Unknown role '{args.role}'. "
                f"Available roles: {', '.join(search_queries)}"
            )

        search_queries = {args.role: search_queries[args.role]}

    if args.command == "ingest":
        with (
            AdzunaProvider(
                country=config.country,
                results_per_page=config.results_per_page,
                max_days_old=config.max_days_old,
            ) as provider,
            DuckDBRepository(
                db_path=config.db_path,
                schema_path=config.schema_path,
            ) as repository,
        ):
            processed = ingest_jobs(
                provider=provider,
                repository=repository,
                search_queries=search_queries,
                max_pages=args.max_pages or config.max_pages,
                batch_size=args.batch_size,
            )

            logging.info(
                "Ingestion completed: %d records processed",
                processed,
            )


if __name__ == "__main__":
    main()
