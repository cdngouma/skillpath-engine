import argparse
from src.ingestion import adzuna

def main(args):
    if args.target not in ['all', 'jobs', 'desc']:
        ValueError("Not recognized")

    if args.target in ("jobs", "all"):
        adzuna.ingest_jobs()
    if args.target in ("desc", "all"):
        adzuna.ingest_job_descriptions()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the job ingestion pipeline.")
    parser.add_argument(
        "--target",
        type=str,
        default='all',
        help=(
            "Which ingestion step to run: "
            "'jobs' for raw job listings, "
            "'descriptions' for scraped full descriptions, "
            "'all' for both."
        )
    )
    args = parser.parse_args()
    main(args)