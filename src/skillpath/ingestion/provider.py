
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel, Field


class JobRecord(BaseModel):
    source: str
    source_job_id: str

    title: str
    company: str | None = None
    location: str | None = None

    posted_at: datetime | None = None
    description: str | None = None
    url: str | None = None

    salary_min: float | None = None
    salary_max: float | None = None

    search_term: str
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class JobProvider(Protocol):
    def fetch_jobs(
        self,
        query: str,
        max_pages: int = 1,
    ) -> Iterator[JobRecord]:
        ...
