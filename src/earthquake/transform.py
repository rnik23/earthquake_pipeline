"""
Transform layer for the earthquake pipeline.

Responsibilities:
- Assign each earthquake event to a magnitude bucket
- Aggregate events into daily counts per bucket
- Return both raw events (unchanged) and daily aggregates to the pipeline

Design decisions:
- Pure functions only — no I/O, no side effects, no DB, no API calls.
  This makes the transform layer trivially testable and completely
  reusable regardless of where data comes from or goes to.
- Null magnitude events are stored raw but excluded from aggregates.
  You cannot bucket what you cannot measure — but you also should not
  silently discard data that may be revised by USGS later.
- Buckets use half-open intervals [lower, upper) so boundary values
  always belong to exactly one bucket with no ambiguity.
- The lowest bucket extends to -inf to handle negative magnitude
  micro-seismic events confirmed in live USGS API (e.g. -1.08).
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone

from earthquake.config import Config
from earthquake.models import DailyAggregate, EarthquakeEvent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def transform(
    events: list[EarthquakeEvent],
    config: Config,
) -> tuple[list[EarthquakeEvent], list[DailyAggregate]]:
    """
    Top-level transform orchestrator.

    Takes raw validated events and returns:
      - The same event list unchanged (all events stored raw)
      - Daily aggregates bucketed by magnitude (null mag events excluded)

    The pipeline calls this and passes both return values to storage.
    """
    if not events:
        logger.debug("transform_noop", extra={"reason": "empty event list"})
        return [], []

    aggregates = aggregate_by_day(events, config)

    null_count = sum(1 for e in events if e.magnitude is None)
    logger.info(
        "transform_complete",
        extra={
            "events_in":          len(events),
            "aggregates_out":     len(aggregates),
            "null_magnitude_skipped": null_count,
        },
    )

    return events, aggregates


def assign_bucket(
    magnitude: float | None,
    buckets: list[tuple[float, float, str]],
) -> str | None:
    """
    Assign a magnitude value to a bucket label.

    Buckets are half-open intervals [lower, upper):
      lower <= magnitude < upper

    Returns None if magnitude is None — caller decides how to handle.
    Logs a warning if no bucket matches (should never happen with -inf lower bound).

    Args:
        magnitude: the earthquake magnitude, may be None
        buckets:   list of (lower, upper, label) tuples from config

    Returns:
        bucket label string, or None if magnitude is None
    """
    if magnitude is None:
        return None

    for lower, upper, label in buckets:
        if lower <= magnitude < upper:
            return label

    # Should never reach here given -inf lower bound on first bucket
    logger.warning(
        "magnitude_unmatched",
        extra={"magnitude": magnitude, "buckets": buckets},
    )
    return None


def aggregate_by_day(
    events: list[EarthquakeEvent],
    config: Config,
) -> list[DailyAggregate]:
    """
    Group events by (date, bucket) and count them.

    Events with null magnitude are excluded from aggregates — they cannot
    be bucketed. They are still stored in raw_events by the pipeline.

    Returns a flat list of DailyAggregate objects sorted by date then bucket.
    One object per (date, bucket) pair that has at least one event.

    Args:
        events: validated EarthquakeEvent objects from the fetcher
        config: pipeline config carrying magnitude bucket definitions

    Returns:
        List[DailyAggregate] sorted by date ASC, bucket ASC
    """
    # counts[(date, bucket)] = int
    counts: dict[tuple[str, str], int] = defaultdict(int)

    skipped = 0
    for event in events:
        bucket = assign_bucket(event.magnitude, config.magnitude_buckets)

        if bucket is None:
            skipped += 1
            continue

        date_str = event.occurred_at.strftime("%Y-%m-%d")
        counts[(date_str, bucket)] += 1

    if skipped:
        logger.debug(
            "null_magnitude_events_skipped",
            extra={"count": skipped},
        )

    now = datetime.now(timezone.utc)

    aggregates = [
        DailyAggregate(
            date=date_str,
            bucket=bucket,
            count=count,
            updated_at=now,
        )
        for (date_str, bucket), count in sorted(counts.items())
    ]

    logger.debug(
        "aggregation_complete",
        extra={
            "unique_date_bucket_pairs": len(aggregates),
            "null_skipped": skipped,
        },
    )

    return aggregates