"""
Tests for the SQLite storage layer.
All tests use an in-memory SQLite database — no file I/O, no cleanup needed.
"""

import pytest
from datetime import datetime, timezone
from earthquake.storage import StorageManager
from earthquake.models import EarthquakeEvent, DailyAggregate
from earthquake.config import Config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    """In-memory DB — vanishes after each test, no cleanup needed."""
    return Config(db_path=":memory:")


@pytest.fixture
def storage(config):
    """Fresh StorageManager with schema already initialized."""
    mgr = StorageManager(config=config)
    mgr.initialize_schema()
    return mgr


@pytest.fixture
def sample_event():
    return EarthquakeEvent(
        event_id="us6000sqz1",
        magnitude=5.1,
        place="99 km NE of Finschhafen, Papua New Guinea",
        occurred_at=datetime(2026, 4, 17, 21, 46, 30, tzinfo=timezone.utc),
        usgs_updated_at=datetime(2026, 4, 17, 22, 2, 0, tzinfo=timezone.utc),
        latitude=-5.9581,
        longitude=148.5128,
        depth_km=94.504,
        event_type="earthquake",
        raw_status="reviewed",
    )


@pytest.fixture
def sample_aggregate():
    return DailyAggregate(
        date="2026-04-17",
        bucket="4-6",
        count=3,
    )


def make_event(event_id: str, magnitude: float, date_str: str) -> EarthquakeEvent:
    """Helper to mint events quickly for bulk tests."""
    return EarthquakeEvent(
        event_id=event_id,
        magnitude=magnitude,
        place="Somewhere",
        occurred_at=datetime.fromisoformat(f"{date_str}T00:00:00+00:00"),
        usgs_updated_at=None,
        latitude=0.0,
        longitude=0.0,
        depth_km=10.0,
        event_type="earthquake",
        raw_status="reviewed",
    )


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_tables_created_on_initialize(self, storage):
        """All three tables must exist after initialize_schema()."""
        conn = storage._connect()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert "raw_events" in tables
        assert "daily_aggregates" in tables
        assert "pipeline_runs" in tables

    def test_initialize_is_idempotent(self, storage):
        """Calling initialize_schema() twice must not raise or duplicate."""
        storage.initialize_schema()
        storage.initialize_schema()
        conn = storage._connect()
        cursor = conn.execute("SELECT count(*) FROM raw_events")
        assert cursor.fetchone()[0] == 0

    def test_indexes_created(self, storage):
        """Indexes on occurred_at and magnitude must exist for query performance."""
        conn = storage._connect()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indexes = {row[0] for row in cursor.fetchall()}
        assert "idx_raw_events_occurred_at" in indexes
        assert "idx_raw_events_magnitude" in indexes


# ---------------------------------------------------------------------------
# Raw events tests
# ---------------------------------------------------------------------------

class TestRawEvents:
    def test_insert_single_event(self, storage, sample_event):
        storage.upsert_events([sample_event])
        conn = storage._connect()
        row = conn.execute(
            "SELECT event_id, magnitude FROM raw_events WHERE event_id = ?",
            (sample_event.event_id,)
        ).fetchone()
        assert row is not None
        assert row[0] == "us6000sqz1"
        assert row[1] == 5.1

    def test_upsert_updates_existing_event(self, storage, sample_event):
        """Re-inserting same event_id with different magnitude should update."""
        storage.upsert_events([sample_event])

        updated = sample_event.model_copy(update={"magnitude": 5.9})
        storage.upsert_events([updated])

        conn = storage._connect()
        rows = conn.execute("SELECT magnitude FROM raw_events").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 5.9

    def test_insert_bulk_events(self, storage):
        """Bulk insert should store all events efficiently."""
        events = [make_event(f"us{i:04d}", float(i % 7), "2026-04-17") for i in range(100)]
        storage.upsert_events(events)
        conn = storage._connect()
        count = conn.execute("SELECT count(*) FROM raw_events").fetchone()[0]
        assert count == 100

    def test_null_magnitude_stored_correctly(self, storage):
        """None magnitude must round-trip cleanly — not coerced to 0."""
        event = make_event("us_null_mag", None, "2026-04-17")
        storage.upsert_events([event])
        conn = storage._connect()
        row = conn.execute(
            "SELECT magnitude FROM raw_events WHERE event_id = 'us_null_mag'"
        ).fetchone()
        assert row[0] is None

    def test_negative_magnitude_stored_correctly(self, storage):
        """Negative magnitudes (micro-seismic) must be stored as-is."""
        event = make_event("us_neg_mag", -1.08, "2026-04-17")
        storage.upsert_events([event])
        conn = storage._connect()
        row = conn.execute(
            "SELECT magnitude FROM raw_events WHERE event_id = 'us_neg_mag'"
        ).fetchone()
        assert row[0] == pytest.approx(-1.08)

    def test_timestamps_stored_as_iso_strings(self, storage, sample_event):
        """Datetimes must be stored as ISO 8601 strings, not raw integers."""
        storage.upsert_events([sample_event])
        conn = storage._connect()
        row = conn.execute(
            "SELECT occurred_at FROM raw_events WHERE event_id = ?",
            (sample_event.event_id,)
        ).fetchone()
        # Should be parseable as ISO and round-trip to the same value
        parsed = datetime.fromisoformat(row[0])
        assert parsed == sample_event.occurred_at

    def test_get_events_by_date_range(self, storage):
        """Date range query must return only events within the window."""
        events = [
            make_event("us001", 1.0, "2026-04-15"),
            make_event("us002", 2.0, "2026-04-17"),
            make_event("us003", 3.0, "2026-04-19"),
        ]
        storage.upsert_events(events)
        results = storage.get_events_by_date_range("2026-04-16", "2026-04-18")
        assert len(results) == 1
        assert results[0].event_id == "us002"


# ---------------------------------------------------------------------------
# Daily aggregates tests
# ---------------------------------------------------------------------------

class TestDailyAggregates:
    def test_insert_single_aggregate(self, storage, sample_aggregate):
        storage.upsert_aggregates([sample_aggregate])
        conn = storage._connect()
        row = conn.execute(
            "SELECT date, bucket, count FROM daily_aggregates"
        ).fetchone()
        assert tuple(row) == ("2026-04-17", "4-6", 3)

    def test_upsert_overwrites_existing_aggregate(self, storage, sample_aggregate):
        """Re-running pipeline for same window must overwrite, not duplicate."""
        storage.upsert_aggregates([sample_aggregate])

        updated = DailyAggregate(date="2026-04-17", bucket="4-6", count=99)
        storage.upsert_aggregates([updated])

        conn = storage._connect()
        rows = conn.execute("SELECT count(*) FROM daily_aggregates").fetchone()
        assert rows[0] == 1

        row = conn.execute("SELECT count FROM daily_aggregates").fetchone()
        assert row[0] == 99

    def test_all_four_buckets_stored(self, storage):
        """All four magnitude buckets must be storable for a single date."""
        aggregates = [
            DailyAggregate(date="2026-04-17", bucket="<2",  count=80),
            DailyAggregate(date="2026-04-17", bucket="2-4", count=15),
            DailyAggregate(date="2026-04-17", bucket="4-6", count=4),
            DailyAggregate(date="2026-04-17", bucket="6+",  count=1),
        ]
        storage.upsert_aggregates(aggregates)
        conn = storage._connect()
        count = conn.execute("SELECT count(*) FROM daily_aggregates").fetchone()[0]
        assert count == 4

    def test_get_aggregates_by_date(self, storage):
        """Querying aggregates for a specific date returns correct rows."""
        aggregates = [
            DailyAggregate(date="2026-04-17", bucket="<2",  count=80),
            DailyAggregate(date="2026-04-17", bucket="2-4", count=15),
            DailyAggregate(date="2026-04-18", bucket="<2",  count=60),
        ]
        storage.upsert_aggregates(aggregates)
        results = storage.get_aggregates_by_date("2026-04-17")
        assert len(results) == 2
        buckets = {r.bucket for r in results}
        assert buckets == {"<2", "2-4"}


# ---------------------------------------------------------------------------
# Pipeline runs tests
# ---------------------------------------------------------------------------

class TestPipelineRuns:
    def test_start_run_creates_record(self, storage):
        run_id = storage.start_run()
        assert isinstance(run_id, int)
        conn = storage._connect()
        row = conn.execute(
            "SELECT status FROM pipeline_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert row[0] == "running"

    def test_complete_run_updates_record(self, storage):
        run_id = storage.start_run()
        storage.complete_run(run_id, events_fetched=1200, pages_fetched=2)
        conn = storage._connect()
        row = conn.execute(
            "SELECT status, events_fetched, pages_fetched, completed_at FROM pipeline_runs WHERE id = ?",
            (run_id,)
        ).fetchone()
        assert row[0] == "success"
        assert row[1] == 1200
        assert row[2] == 2
        assert row[3] is not None

    def test_fail_run_records_error(self, storage):
        run_id = storage.start_run()
        storage.fail_run(run_id, error="ConnectionError: timeout after 30s")
        conn = storage._connect()
        row = conn.execute(
            "SELECT status, error_message FROM pipeline_runs WHERE id = ?",
            (run_id,)
        ).fetchone()
        assert row[0] == "failed"
        assert "timeout" in row[1]

    def test_multiple_runs_tracked_independently(self, storage):
        """Each pipeline execution gets its own row — full audit trail."""
        id1 = storage.start_run()
        id2 = storage.start_run()
        storage.complete_run(id1, events_fetched=100, pages_fetched=1)
        storage.fail_run(id2, error="something broke")

        conn = storage._connect()
        rows = conn.execute(
            "SELECT id, status FROM pipeline_runs ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert tuple(rows[0]) == (id1, "success")
        assert tuple(rows[1]) == (id2, "failed")