"""
Tests for the transform layer.
Pure functions, zero I/O — no mocks, no DB, no API calls needed.
These are the fastest tests in the suite and should stay that way.
"""

import pytest
from datetime import datetime, timezone

from earthquake.transform import (
    assign_bucket,
    aggregate_by_day,
    transform,
)
from earthquake.models import EarthquakeEvent, DailyAggregate
from earthquake.config import Config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return Config()


def make_event(
    event_id: str,
    magnitude: float | None,
    date_str: str,
) -> EarthquakeEvent:
    """Mint a minimal EarthquakeEvent for transform testing."""
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


# ---------------------------------------------------------------------------
# assign_bucket tests
# Pure function: (magnitude, buckets) -> bucket label
# ---------------------------------------------------------------------------

class TestAssignBucket:
    def test_negative_magnitude_assigned_to_sub_two(self, config):
        """
        Verified against live API — USGS returns negative magnitudes
        for micro-seismic events. Must land in '<2' not fall through.
        """
        assert assign_bucket(-1.08, config.magnitude_buckets) == "<2"

    def test_zero_magnitude(self, config):
        assert assign_bucket(0.0, config.magnitude_buckets) == "<2"

    def test_mid_bucket_value(self, config):
        assert assign_bucket(1.5, config.magnitude_buckets) == "<2"

    def test_exact_lower_boundary_2(self, config):
        """2.0 is the lower bound of '2-4' — boundary belongs to upper bucket."""
        assert assign_bucket(2.0, config.magnitude_buckets) == "2-4"

    def test_mid_2_4_bucket(self, config):
        assert assign_bucket(3.3, config.magnitude_buckets) == "2-4"

    def test_exact_lower_boundary_4(self, config):
        assert assign_bucket(4.0, config.magnitude_buckets) == "4-6"

    def test_mid_4_6_bucket(self, config):
        assert assign_bucket(5.1, config.magnitude_buckets) == "4-6"

    def test_exact_lower_boundary_6(self, config):
        assert assign_bucket(6.0, config.magnitude_buckets) == "6+"

    def test_large_magnitude(self, config):
        """Major earthquakes well above 6 must still resolve correctly."""
        assert assign_bucket(9.1, config.magnitude_buckets) == "6+"

    def test_null_magnitude_returns_none(self, config):
        """
        None magnitude events exist in live API — must return None,
        not crash or silently assign to a bucket.
        """
        assert assign_bucket(None, config.magnitude_buckets) is None


# ---------------------------------------------------------------------------
# aggregate_by_day tests
# Pure function: List[EarthquakeEvent] -> List[DailyAggregate]
# ---------------------------------------------------------------------------

class TestAggregateByDay:
    def test_single_event_single_bucket(self, config):
        events = [make_event("us001", 5.1, "2026-04-17")]
        results = aggregate_by_day(events, config)

        assert len(results) == 1
        assert results[0].date == "2026-04-17"
        assert results[0].bucket == "4-6"
        assert results[0].count == 1

    def test_multiple_events_same_day_same_bucket(self, config):
        events = [
            make_event("us001", 1.0, "2026-04-17"),
            make_event("us002", 0.5, "2026-04-17"),
            make_event("us003", -0.3, "2026-04-17"),
        ]
        results = aggregate_by_day(events, config)

        assert len(results) == 1
        assert results[0].bucket == "<2"
        assert results[0].count == 3

    def test_multiple_events_same_day_different_buckets(self, config):
        events = [
            make_event("us001", 1.0, "2026-04-17"),
            make_event("us002", 3.0, "2026-04-17"),
            make_event("us003", 5.0, "2026-04-17"),
            make_event("us004", 7.0, "2026-04-17"),
        ]
        results = aggregate_by_day(events, config)
        by_bucket = {r.bucket: r.count for r in results}

        assert by_bucket["<2"]  == 1
        assert by_bucket["2-4"] == 1
        assert by_bucket["4-6"] == 1
        assert by_bucket["6+"]  == 1

    def test_multiple_days(self, config):
        events = [
            make_event("us001", 1.0, "2026-04-17"),
            make_event("us002", 1.0, "2026-04-17"),
            make_event("us003", 1.0, "2026-04-18"),
        ]
        results = aggregate_by_day(events, config)
        by_date = {r.date: r.count for r in results}

        assert by_date["2026-04-17"] == 2
        assert by_date["2026-04-18"] == 1

    def test_null_magnitude_events_excluded(self, config):
        """
        Events with no magnitude cannot be bucketed — they must be
        excluded from aggregates, not silently counted in any bucket.
        """
        events = [
            make_event("us001", 3.0, "2026-04-17"),
            make_event("us002", None, "2026-04-17"),  # should be excluded
        ]
        results = aggregate_by_day(events, config)

        assert len(results) == 1
        assert results[0].count == 1

    def test_empty_event_list_returns_empty(self, config):
        results = aggregate_by_day([], config)
        assert results == []

    def test_all_null_magnitude_returns_empty(self, config):
        events = [make_event(f"us00{i}", None, "2026-04-17") for i in range(5)]
        results = aggregate_by_day(events, config)
        assert results == []

    def test_returns_daily_aggregate_objects(self, config):
        """Return type must be List[DailyAggregate] — not dicts or tuples."""
        events = [make_event("us001", 1.0, "2026-04-17")]
        results = aggregate_by_day(events, config)
        assert all(isinstance(r, DailyAggregate) for r in results)

    def test_aggregate_counts_are_non_negative(self, config):
        """DailyAggregate.count has ge=0 constraint — transform must respect it."""
        events = [make_event(f"us{i:03d}", 1.0, "2026-04-17") for i in range(10)]
        results = aggregate_by_day(events, config)
        assert all(r.count >= 0 for r in results)

    def test_thirty_days_of_data(self, config):
        """
        Realistic volume test — 30 days, mixed magnitudes.
        Proves aggregation scales without blowing up.
        """
        events = []
        for day in range(1, 31):
            date_str = f"2026-04-{day:02d}" if day <= 30 else f"2026-05-{day-30:02d}"
            for i in range(50):
                mag = (i % 8) - 1.0  # range: -1.0 to 6.0
                events.append(make_event(f"us{day:02d}{i:02d}", mag, date_str))

        results = aggregate_by_day(events, config)

        dates = {r.date for r in results}
        assert len(dates) == 30
        assert all(r.count > 0 for r in results)


# ---------------------------------------------------------------------------
# transform (top-level orchestrator) tests
# ---------------------------------------------------------------------------

class TestTransform:
    def test_returns_tuple_of_events_and_aggregates(self, config):
        events = [make_event("us001", 3.0, "2026-04-17")]
        result_events, result_aggregates = transform(events, config)

        assert isinstance(result_events, list)
        assert isinstance(result_aggregates, list)

    def test_passthrough_events_unchanged(self, config):
        """
        Transform must not mutate or filter the raw event list —
        all events pass through to storage regardless of magnitude.
        """
        events = [
            make_event("us001", 3.0, "2026-04-17"),
            make_event("us002", None, "2026-04-17"),  # null mag — still stored raw
        ]
        result_events, _ = transform(events, config)

        assert len(result_events) == 2

    def test_aggregates_exclude_null_magnitude(self, config):
        events = [
            make_event("us001", 3.0, "2026-04-17"),
            make_event("us002", None, "2026-04-17"),
        ]
        _, aggregates = transform(events, config)

        total_count = sum(a.count for a in aggregates)
        assert total_count == 1

    def test_empty_input_returns_empty_output(self, config):
        result_events, result_aggregates = transform([], config)
        assert result_events == []
        assert result_aggregates == []