"""
Tests for the pipeline orchestrator.
Verifies that fetch → transform → store are wired correctly
and that the audit trail is maintained through success and failure paths.

All external dependencies are mocked — this tests orchestration logic only,
not the behavior of individual layers (those are covered in their own suites).
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

from earthquake.pipeline import Pipeline
from earthquake.config import Config
from earthquake.models import EarthquakeEvent, DailyAggregate


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return Config(db_path=":memory:")


def make_event(event_id: str, magnitude: float, date_str: str) -> EarthquakeEvent:
    return EarthquakeEvent(
        event_id=event_id,
        magnitude=magnitude,
        place="Somewhere",
        occurred_at=datetime.fromisoformat(f"{date_str}T12:00:00+00:00"),
        usgs_updated_at=None,
        latitude=0.0,
        longitude=0.0,
        depth_km=10.0,
        event_type="earthquake",
        raw_status="reviewed",
    )


def make_aggregate(date: str, bucket: str, count: int) -> DailyAggregate:
    return DailyAggregate(date=date, bucket=bucket, count=count)


# ---------------------------------------------------------------------------
# Wiring tests — does the orchestrator call the right things?
# ---------------------------------------------------------------------------

class TestPipelineWiring:
    def test_fetch_transform_store_called_in_order(self, config):
        """
        The three stages must execute in sequence.
        A failure in fetch must prevent transform and store from running.
        """
        call_order = []

        mock_fetcher  = MagicMock()
        mock_storage  = MagicMock()

        events = [make_event("us001", 3.0, "2026-04-17")]
        aggregates = [make_aggregate("2026-04-17", "2-4", 1)]

        mock_fetcher.fetch_all.side_effect  = lambda: call_order.append("fetch") or events
        mock_storage.start_run.return_value = 1
        mock_storage.upsert_events.side_effect   = lambda e: call_order.append("store_events")
        mock_storage.upsert_aggregates.side_effect = lambda a: call_order.append("store_aggregates")

        with patch("earthquake.pipeline.USGSFetcher",  return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform", return_value=(events, aggregates)) as mock_transform:
            mock_transform.side_effect = lambda e, c: call_order.append("transform") or (events, aggregates)
            Pipeline(config=config).run()

        assert call_order == ["fetch", "transform", "store_events", "store_aggregates"]

    def test_storage_initialized_before_run(self, config):
        """initialize_schema must be called before any data is written."""
        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 1
        mock_fetcher.fetch_all.return_value = []

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=([], [])):
            Pipeline(config=config).run()

        mock_storage.initialize_schema.assert_called_once()

    def test_run_id_passed_to_complete(self, config):
        """The run_id from start_run must be passed to complete_run."""
        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 42
        mock_fetcher.fetch_all.return_value = []

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=([], [])):
            Pipeline(config=config).run()

        mock_storage.complete_run.assert_called_once()
        call_args = mock_storage.complete_run.call_args
        assert call_args.args[0] == 42

    def test_events_and_aggregates_passed_to_storage(self, config):
        """Fetched events and computed aggregates must reach storage."""
        events     = [make_event("us001", 3.0, "2026-04-17")]
        aggregates = [make_aggregate("2026-04-17", "2-4", 1)]

        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 1
        mock_fetcher.fetch_all.return_value = events

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=(events, aggregates)):
            Pipeline(config=config).run()

        mock_storage.upsert_events.assert_called_once_with(events)
        mock_storage.upsert_aggregates.assert_called_once_with(aggregates)


# ---------------------------------------------------------------------------
# Audit trail tests — does the pipeline_runs table stay consistent?
# ---------------------------------------------------------------------------

class TestAuditTrail:
    def test_successful_run_calls_complete_run(self, config):
        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 1
        mock_fetcher.fetch_all.return_value = [
            make_event("us001", 3.0, "2026-04-17"),
            make_event("us002", 1.0, "2026-04-17"),
        ]

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=([], [])):
            Pipeline(config=config).run()

        mock_storage.complete_run.assert_called_once()
        mock_storage.fail_run.assert_not_called()

    def test_failed_run_calls_fail_run(self, config):
        """If fetch raises, fail_run must be called with the error."""
        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 7
        mock_fetcher.fetch_all.side_effect = ConnectionError("USGS unreachable")

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage):
            with pytest.raises(ConnectionError):
                Pipeline(config=config).run()

        mock_storage.fail_run.assert_called_once()
        call_args = mock_storage.fail_run.call_args
        assert call_args.args[0] == 7
        assert "USGS unreachable" in call_args.kwargs["error"]

    def test_fail_run_called_even_if_transform_fails(self, config):
        """Failure anywhere after start_run must still record the run as failed."""
        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 3
        mock_fetcher.fetch_all.return_value = [
            make_event("us001", 3.0, "2026-04-17")
        ]

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform", side_effect=RuntimeError("transform exploded")):
            with pytest.raises(RuntimeError):
                Pipeline(config=config).run()

        mock_storage.fail_run.assert_called_once()
        assert mock_storage.complete_run.call_count == 0

    def test_complete_run_receives_correct_counts(self, config):
        """events_fetched and pages_fetched must be logged accurately."""
        events = [make_event(f"us{i:03d}", 1.0, "2026-04-17") for i in range(150)]

        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 1
        mock_fetcher.fetch_all.return_value        = events
        mock_fetcher.pages_fetched                 = 2

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=(events, [])):
            Pipeline(config=config).run()

        call_kwargs = mock_storage.complete_run.call_args
        assert call_kwargs.kwargs["events_fetched"] == 150


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------

class TestPipelineEdgeCases:
    def test_empty_fetch_result_completes_cleanly(self, config):
        """Zero events returned from API should not crash the pipeline."""
        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value = 1
        mock_fetcher.fetch_all.return_value = []

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=([], [])):
            Pipeline(config=config).run()

        mock_storage.complete_run.assert_called_once()
        mock_storage.fail_run.assert_not_called()

    def test_storage_failure_recorded_as_failed_run(self, config):
        """If upsert_events raises, the run must be marked failed."""
        events = [make_event("us001", 3.0, "2026-04-17")]

        mock_fetcher = MagicMock()
        mock_storage = MagicMock()
        mock_storage.start_run.return_value  = 1
        mock_fetcher.fetch_all.return_value  = events
        mock_storage.upsert_events.side_effect = Exception("disk full")

        with patch("earthquake.pipeline.USGSFetcher",   return_value=mock_fetcher), \
             patch("earthquake.pipeline.StorageManager", return_value=mock_storage), \
             patch("earthquake.pipeline.transform",      return_value=(events, [])):
            with pytest.raises(Exception, match="disk full"):
                Pipeline(config=config).run()

        mock_storage.fail_run.assert_called_once()