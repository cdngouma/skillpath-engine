
import logging
import os
from collections.abc import Iterator
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from skillpath.ingestion.provider import JobRecord

logger = logging.getLogger(__name__)


class AdzunaProvider:
    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    source = "adzuna"

    def __init__(
        self,
        app_id: str | None = None,
        app_key: str | None = None,
        country: str = "ca",
        results_per_page: int = 50,
        max_days_old: int = 30,
        timeout: int = 20,
    ):
        self.app_id = app_id or os.getenv("ADZUNA_APP_ID")
        self.app_key = app_key or os.getenv("ADZUNA_APP_KEY")

        if not self.app_id or not self.app_key:
            print(self.app_id, self.app_key)
            raise ValueError("Missing Adzuna API credentials")

        self.country = country
        self.results_per_page = results_per_page
        self.max_days_old = max_days_old
        self.timeout = timeout

        self.session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)

    def fetch_jobs(
        self,
        query: str,
        max_pages: int = 1,
    ) -> Iterator[JobRecord]:
        """Fetch and normalize paginated Adzuna results."""

        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        for page in range(1, max_pages + 1):
            payload = self._fetch_page(query, page)
            results = payload["results"]

            logger.info(
                "Adzuna query=%s page=%d jobs=%d",
                query,
                page,
                len(results),
            )

            for item in results:
                yield self._normalize(item, query)

            if len(results) < self.results_per_page:
                break

    def _fetch_page(self, query: str, page: int) -> dict:
        url = f"{self.BASE_URL}/{self.country}/search/{page}"

        response = self.session.get(
            url,
            params={
                "app_id": self.app_id,
                "app_key": self.app_key,
                "what_phrase": query,
                "results_per_page": self.results_per_page,
                "max_days_old": self.max_days_old,
                "content-type": "application/json",
            },
            timeout=self.timeout,
        )

        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, dict):
            raise ValueError("Unexpected Adzuna response format")

        if not isinstance(payload.get("results"), list):
            raise ValueError("Adzuna response missing results list")

        return payload

    @staticmethod
    def _normalize(item: dict, query: str) -> JobRecord:
        posted_at = item.get("created")

        return JobRecord(
            source=AdzunaProvider.source,
            source_job_id=str(item["id"]),
            title=item["title"],
            company=(item.get("company") or {}).get("display_name"),
            location=(item.get("location") or {}).get("display_name"),
            posted_at=(
                datetime.fromisoformat(
                    posted_at.replace("Z", "+00:00")
                )
                if posted_at
                else None
            ),
            description=item.get("description"),
            url=item.get("redirect_url"),
            salary_min=item.get("salary_min"),
            salary_max=item.get("salary_max"),
            search_term=query,
            raw_payload=item,
        )

    def close(self) -> None:
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
