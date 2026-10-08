
from unittest.mock import Mock

import pytest
import requests

from skillpath.config import load_config
from skillpath.ingestion.adzuna import AdzunaProvider


@pytest.fixture
def provider():
    client = AdzunaProvider(
        app_id="test_id",
        app_key="test_key",
        country="ca",
        results_per_page=2,
        max_days_old=30,
        timeout=20,
    )
    yield client
    client.close()


@pytest.fixture
def sample_job():
    return {
        "id": "12345",
        "title": "Machine Learning Engineer",
        "company": {"display_name": "Bell"},
        "location": {"display_name": "Montreal"},
        "created": "2026-10-01T12:00:00Z",
        "description": "Python, SQL, PyTorch",
        "redirect_url": "https://example.com/jobs/12345",
        "salary_min": 85000,
        "salary_max": 115000,
    }


def mock_response(payload, status_code=200):
    response = Mock(spec=requests.Response)
    response.status_code = status_code
    response.json.return_value = payload

    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"HTTP {status_code}"
        )

    return response


# ---------------------------------------------------------
# Normalization
# ---------------------------------------------------------

def test_normalizes_job(provider, sample_job):
    job = provider._normalize(sample_job, "ml engineer")

    assert job.source == "adzuna"
    assert job.source_job_id == "12345"
    assert job.title == "Machine Learning Engineer"
    assert job.company == "Bell"
    assert job.location == "Montreal"
    assert job.posted_at.year == 2026
    assert job.posted_at.utcoffset().total_seconds() == 0
    assert job.description == "Python, SQL, PyTorch"
    assert job.salary_min == 85000
    assert job.salary_max == 115000
    assert job.search_term == "ml engineer"
    assert job.raw_payload == sample_job


def test_missing_optional_fields(provider):
    job = provider._normalize(
        {"id": 123, "title": "Data Engineer"},
        "data engineer",
    )

    assert job.source_job_id == "123"
    assert job.company is None
    assert job.location is None
    assert job.posted_at is None
    assert job.description is None
    assert job.url is None
    assert job.salary_min is None
    assert job.salary_max is None


@pytest.mark.parametrize(
    "invalid_job",
    [
        {"title": "Data Scientist"},
        {"id": "123"},
    ],
)
def test_missing_required_fields(provider, invalid_job):
    with pytest.raises((KeyError, ValueError)):
        provider._normalize(invalid_job, "data scientist")


# ---------------------------------------------------------
# API requests and pagination
# ---------------------------------------------------------

def test_fetch_single_page(provider, sample_job, monkeypatch):
    mock_get = Mock(
        return_value=mock_response({"results": [sample_job]})
    )
    monkeypatch.setattr(provider.session, "get", mock_get)

    jobs = list(
        provider.fetch_jobs(
            query="machine learning engineer",
            max_pages=5,
        )
    )

    assert len(jobs) == 1
    assert jobs[0].source_job_id == "12345"
    assert mock_get.call_count == 1

    kwargs = mock_get.call_args.kwargs
    assert kwargs["params"]["what_phrase"] == "machine learning engineer"
    assert kwargs["params"]["results_per_page"] == 2
    assert kwargs["params"]["max_days_old"] == 30
    assert kwargs["timeout"] == 20


def test_empty_results(provider, monkeypatch):
    mock_get = Mock(
        return_value=mock_response({"results": []})
    )
    monkeypatch.setattr(provider.session, "get", mock_get)

    jobs = list(provider.fetch_jobs("data scientist", max_pages=5))

    assert jobs == []
    mock_get.assert_called_once()


def test_multiple_pages(provider, sample_job, monkeypatch):
    job2 = {**sample_job, "id": "222"}
    job3 = {**sample_job, "id": "333"}

    mock_get = Mock(
        side_effect=[
            mock_response({"results": [sample_job, job2]}),
            mock_response({"results": [job3]}),
        ]
    )
    monkeypatch.setattr(provider.session, "get", mock_get)

    jobs = list(provider.fetch_jobs("ml engineer", max_pages=5))

    assert [job.source_job_id for job in jobs] == [
        "12345",
        "222",
        "333",
    ]
    assert mock_get.call_count == 2

    urls = [call.args[0] for call in mock_get.call_args_list]
    assert urls[0].endswith("/search/1")
    assert urls[1].endswith("/search/2")


def test_stops_at_max_pages(provider, sample_job, monkeypatch):
    second_job = {**sample_job, "id": "222"}

    mock_get = Mock(
        return_value=mock_response({
            "results": [sample_job, second_job]
        })
    )
    monkeypatch.setattr(provider.session, "get", mock_get)

    jobs = list(provider.fetch_jobs("ml engineer", max_pages=2))

    assert len(jobs) == 4
    assert mock_get.call_count == 2


@pytest.mark.parametrize("max_pages", [0, -1])
def test_invalid_max_pages(provider, max_pages):
    with pytest.raises(ValueError, match="max_pages"):
        list(provider.fetch_jobs("data scientist", max_pages))


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        {"results": None},
        {"results": "invalid"},
    ],
)
def test_invalid_api_response(provider, monkeypatch, payload):
    monkeypatch.setattr(
        provider.session,
        "get",
        Mock(return_value=mock_response(payload)),
    )

    with pytest.raises(ValueError):
        list(provider.fetch_jobs("data scientist"))


# ---------------------------------------------------------
# HTTP failures
# ---------------------------------------------------------

def test_http_error_propagates(provider, monkeypatch):
    monkeypatch.setattr(
        provider.session,
        "get",
        Mock(
            return_value=mock_response(
                {},
                status_code=500,
            )
        ),
    )

    with pytest.raises(requests.HTTPError):
        list(provider.fetch_jobs("data scientist"))


def test_timeout_propagates(provider, monkeypatch):
    monkeypatch.setattr(
        provider.session,
        "get",
        Mock(side_effect=requests.Timeout("Request timed out")),
    )

    with pytest.raises(requests.Timeout):
        list(provider.fetch_jobs("data scientist"))


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

def test_yaml_search_queries_loaded():
    config = load_config()

    assert isinstance(config.search_queries, dict)
    assert config.search_queries

    assert "data_scientist" in config.search_queries
    assert "ml_engineer" in config.search_queries
    assert "ai_engineer" in config.search_queries
    assert "data_engineer" in config.search_queries
    assert "analytics_engineer" in config.search_queries
    assert "data_analyst" in config.search_queries
    assert "forward_deployed_engineer" in config.search_queries

    for role_id, queries in config.search_queries.items():
        assert isinstance(role_id, str)
        assert isinstance(queries, list)
        assert queries
        assert all(
            isinstance(query, str) and query.strip()
            for query in queries
        )


def test_missing_credentials(monkeypatch):
    monkeypatch.delenv("ADZUNA_APP_ID", raising=False)
    monkeypatch.delenv("ADZUNA_APP_KEY", raising=False)

    with pytest.raises(ValueError, match="credentials"):
        AdzunaProvider()


def test_context_manager_closes_session(monkeypatch):
    client = AdzunaProvider(
        app_id="test_id",
        app_key="test_key",
    )

    mock_close = Mock()
    monkeypatch.setattr(client.session, "close", mock_close)

    with client as active_provider:
        assert active_provider is client

    mock_close.assert_called_once()
