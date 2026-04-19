"""
Tests for the USGS API fetcher.
All tests are offline — no live API calls.
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

from earthquake.fetcher import USGSFetcher
from earthquake.config import Config
from earthquake.models import EarthquakeEvent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    """Fresh config with small page size for easy pagination testing."""
    return Config(page_size=3, max_retries=2)


@pytest.fixture
def fetcher(config):
    return USGSFetcher(config=config)


def make_feature(event_id: str, magnitude: float | None, time_ms: int) -> dict:
    """
    Build a realistic USGS GeoJSON feature matching the actual API payload shape.
    Verified against live API response 2026-04-19.
    """
    return {
        "type": "Feature",
        "properties": {
            "mag": magnitude,
            "place": "10km N of Somewhere, CA",
            "time": time_ms,
            "updated": time_ms + 1000,
            "tz": None,
            "url": f"https://earthquake.usgs.gov/earthquakes/eventpage/{event_id}",
            "detail": f"https://earthquake.usgs.gov/fdsnws/event/1/query?eventid={event_id}&format=geojson",
            "felt": None,
            "cdi": None,
            "mmi": None,
            "alert": None,
            "status": "reviewed",
            "tsunami": 0,
            "sig": 44,
            "net": "nc",
            "code": event_id[2:],
            "ids": f",{event_id},",
            "sources": ",nc,",
            "types": ",origin,phase-data,",
            "nst": 11,
            "dmin": 0.01421,
            "rms": 0.02,
            "gap": 80,
            "magType": "md",
            "type": "earthquake",
            "title": f"M {magnitude} - 10km N of Somewhere, CA",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-122.768501281738, 38.7881660461426, 1.13999998569489],
        },
        "id": event_id,
    }


def make_api_response(features: list, count: int | None = None, offset: int = 1) -> dict:
    """
    Build a realistic USGS GeoJSON FeatureCollection matching the actual API payload shape.
    Verified against live API response 2026-04-19.
    """
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": 1776633280000,
            "url": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson",
            "title": "USGS Earthquakes",
            "status": 200,
            "api": "2.4.0",
            "limit": len(features),
            "offset": offset,
            "count": count if count is not None else len(features),
        },
        "features": features,
    }


# ---------------------------------------------------------------------------
# Count endpoint tests
# ---------------------------------------------------------------------------

class TestFetchCount:
    def test_returns_total_count(self, fetcher):
        mock_response = MagicMock()
        mock_response.json.return_value = {"count": 4200}
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response):
            count = fetcher.fetch_count()

        assert count == 4200

    def test_count_uses_correct_params(self, fetcher):
        mock_response = MagicMock()
        mock_response.json.return_value = {"count": 0}
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response) as mock_get:
            fetcher.fetch_count()

        call_kwargs = mock_get.call_args.kwargs["params"]
        assert call_kwargs["format"] == "geojson"
        assert "starttime" in call_kwargs
        assert "endtime" in call_kwargs


# ---------------------------------------------------------------------------
# Single page fetch tests
# ---------------------------------------------------------------------------

class TestFetchPage:
    def test_returns_parsed_events(self, fetcher):
        features = [
            make_feature("us001", 3.5, 1713484800000),
            make_feature("us002", 5.1, 1713484900000),
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = make_api_response(features)
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response):
            events = fetcher.fetch_page(offset=1)

        assert len(events) == 2
        assert all(isinstance(e, EarthquakeEvent) for e in events)
        assert events[0].event_id == "us001"
        assert events[1].magnitude == 5.1

    def test_passes_correct_pagination_params(self, fetcher):
        mock_response = MagicMock()
        mock_response.json.return_value = make_api_response([])
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response) as mock_get:
            fetcher.fetch_page(offset=1001)

        params = mock_get.call_args.kwargs["params"]
        assert params["offset"] == 1001
        assert params["limit"] == fetcher.config.page_size
        assert params["orderby"] == "time-asc"

    def test_skips_invalid_events_and_continues(self, fetcher):
        """A malformed feature should be skipped, not crash the pipeline."""
        features = [
            make_feature("us001", 3.5, 1713484800000),
            {"id": "bad_event", "properties": {}, "geometry": {}},  # missing required time
            make_feature("us003", 2.1, 1713484900000),
        ]
        mock_response = MagicMock()
        mock_response.json.return_value = make_api_response(features)
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response):
            events = fetcher.fetch_page(offset=1)

        assert len(events) == 2
        assert {e.event_id for e in events} == {"us001", "us003"}

    def test_handles_null_magnitude(self, fetcher):
        """
        USGS can return null mag on unreviewed events — should parse cleanly
        as None, not raise a ValidationError.
        """
        features = [make_feature("us001", None, 1713484800000)]
        mock_response = MagicMock()
        mock_response.json.return_value = make_api_response(features)
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response):
            events = fetcher.fetch_page(offset=1)

        assert len(events) == 1
        assert events[0].magnitude is None


# ---------------------------------------------------------------------------
# Pagination loop tests
# ---------------------------------------------------------------------------

class TestFetchAll:
    def test_single_page_terminates(self, fetcher):
        """Fewer results than page_size signals last page."""
        features = [make_feature(f"us00{i}", 1.0, 1713484800000 + i) for i in range(2)]
        mock_response = MagicMock()
        mock_response.json.return_value = make_api_response(features)
        mock_response.raise_for_status.return_value = None

        with patch("earthquake.fetcher.requests.get", return_value=mock_response):
            with patch.object(fetcher, "fetch_count", return_value=2):
                events = fetcher.fetch_all()

        assert len(events) == 2

    def test_multiple_pages_collected(self, fetcher):
        """With page_size=3, 7 events should require 3 API calls."""
        page1 = [make_feature(f"us00{i}", 1.0, 1713484800000 + i) for i in range(3)]
        page2 = [make_feature(f"us01{i}", 1.0, 1713484900000 + i) for i in range(3)]
        page3 = [make_feature(f"us02{i}", 1.0, 1713485000000 + i) for i in range(1)]

        responses = [
            MagicMock(**{"json.return_value": make_api_response(p), "raise_for_status.return_value": None})
            for p in [page1, page2, page3]
        ]

        with patch("earthquake.fetcher.requests.get", side_effect=responses):
            with patch.object(fetcher, "fetch_count", return_value=7):
                events = fetcher.fetch_all()

        assert len(events) == 7

    def test_deduplicates_events(self, fetcher):
        """Same event_id appearing on two pages should only appear once."""
        page1 = [make_feature("us001", 3.5, 1713484800000)]
        page2 = [make_feature("us001", 3.5, 1713484800000)]  # duplicate

        responses = [
            MagicMock(**{"json.return_value": make_api_response(p), "raise_for_status.return_value": None})
            for p in [page1, page2]
        ]

        with patch("earthquake.fetcher.requests.get", side_effect=responses):
            with patch.object(fetcher, "fetch_count", return_value=2):
                events = fetcher.fetch_all()

        assert len(events) == 1


# ---------------------------------------------------------------------------
# Retry behaviour tests
# ---------------------------------------------------------------------------

class TestRetryBehaviour:
    def test_succeeds_after_transient_failure(self, fetcher):
        """First call raises, second call succeeds — tenacity retries."""
        features = [make_feature("us001", 3.5, 1713484800000)]
        good_response = MagicMock()
        good_response.json.return_value = make_api_response(features)
        good_response.raise_for_status.return_value = None

        import requests as req
        with patch("earthquake.fetcher.requests.get",
                   side_effect=[req.exceptions.ConnectionError("timeout"), good_response]):
            with patch.object(fetcher, "fetch_count", return_value=1):
                events = fetcher.fetch_all()

        assert len(events) == 1

    def test_raises_after_max_retries_exceeded(self, fetcher):
        """Persistent failure should bubble up after retries exhausted."""
        import requests as req
        with patch("earthquake.fetcher.requests.get",
                   side_effect=req.exceptions.ConnectionError("timeout")):
            with pytest.raises(Exception):
                fetcher.fetch_page(offset=1)