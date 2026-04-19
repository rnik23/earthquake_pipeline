"""
SQLite storage layer for the earthquake pipeline.

Responsibilities:
- Own the database schema (CREATE TABLE, indexes)
- Provide idempotent upserts for raw events and daily aggregates
- Track pipeline runs for operational auditability
- Deserialize rows back into Pydantic models on reads
- Keep all SQL explicit — no ORM magic

Design decisions:
- All timestamps stored as ISO 8601 UTC strings (SQLite has no datetime type)
  ISO strings sort lexicographically correctly so range queries work natively.
- ON CONFLICT DO UPDATE makes every write idempotent — safe to rerun.
- Each public write method runs in a single transaction — all or nothing.
- :memory: supported via config.db_path for fully isolated test runs.
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from earthquake.config import Config
from earthquake.models import DailyAggregate, EarthquakeEvent

logger = logging.getLogger(__name__)


class StorageManager:
    def __init__(self, config: Config):
        self.config = config
        self._db_path = str(config.db_path)
        # In-memory DBs are ephemeral per-connection, so we keep one alive for
        # the lifetime of this manager to preserve the schema across calls.
        self._persistent_conn: sqlite3.Connection | None = (
            self._make_connection() if self._db_path == ":memory:" else None
        )

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _make_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _connect(self) -> sqlite3.Connection:
        """
        Return a connection with sane defaults.
        For :memory: databases, reuses the persistent connection so the schema
        isn't lost between calls. For file DBs, opens a fresh connection.
        """
        if self._persistent_conn is not None:
            return self._persistent_conn
        return self._make_connection()

    @contextmanager
    def _transaction(self):
        """
        Context manager that wraps operations in a single transaction.
        Commits on clean exit, rolls back on any exception.
        Skips close() for the persistent in-memory connection.
        """
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            if conn is not self._persistent_conn:
                conn.close()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def initialize_schema(self) -> None:
        """
        Create tables and indexes if they don't exist.
        Safe to call multiple times — fully idempotent.
        """
        with self._transaction() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS raw_events (
                    event_id        TEXT PRIMARY KEY,
                    magnitude       REAL,
                    place           TEXT,
                    occurred_at     TEXT NOT NULL,
                    usgs_updated_at TEXT,
                    latitude        REAL,
                    longitude       REAL,
                    depth_km        REAL,
                    event_type      TEXT,
                    raw_status      TEXT,
                    ingested_at     TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_raw_events_occurred_at
                    ON raw_events(occurred_at);

                CREATE INDEX IF NOT EXISTS idx_raw_events_magnitude
                    ON raw_events(magnitude);

                CREATE TABLE IF NOT EXISTS daily_aggregates (
                    date        TEXT NOT NULL,
                    bucket      TEXT NOT NULL,
                    count       INTEGER NOT NULL DEFAULT 0,
                    updated_at  TEXT NOT NULL,
                    PRIMARY KEY (date, bucket)
                );

                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at     TEXT NOT NULL,
                    completed_at   TEXT,
                    events_fetched INTEGER,
                    pages_fetched  INTEGER,
                    status         TEXT NOT NULL DEFAULT 'running',
                    error_message  TEXT
                );
            """)

        logger.debug("schema_initialized", extra={"db_path": self._db_path})

    # ------------------------------------------------------------------
    # Raw events
    # ------------------------------------------------------------------

    def upsert_events(self, events: list[EarthquakeEvent]) -> None:
        """
        Insert or update a batch of raw earthquake events.
        ON CONFLICT on event_id updates all fields — handles USGS revisions.
        Empty list is a safe no-op.
        """
        if not events:
            logger.debug("upsert_events_noop")
            return

        ingested_at = _now_iso()

        rows = [
            (
                event.event_id,
                event.magnitude,
                event.place,
                _dt_to_iso(event.occurred_at),
                _dt_to_iso(event.usgs_updated_at),
                event.latitude,
                event.longitude,
                event.depth_km,
                event.event_type,
                event.raw_status,
                ingested_at,
            )
            for event in events
        ]

        sql = """
            INSERT INTO raw_events (
                event_id, magnitude, place, occurred_at, usgs_updated_at,
                latitude, longitude, depth_km, event_type, raw_status, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO UPDATE SET
                magnitude       = excluded.magnitude,
                place           = excluded.place,
                occurred_at     = excluded.occurred_at,
                usgs_updated_at = excluded.usgs_updated_at,
                latitude        = excluded.latitude,
                longitude       = excluded.longitude,
                depth_km        = excluded.depth_km,
                event_type      = excluded.event_type,
                raw_status      = excluded.raw_status,
                ingested_at     = excluded.ingested_at
        """

        with self._transaction() as conn:
            conn.executemany(sql, rows)

        logger.info(
            "events_upserted",
            extra={"count": len(events)},
        )

    def get_events_by_date_range(
        self, start_date: str, end_date: str
    ) -> list[EarthquakeEvent]:
        """
        Fetch raw events within an inclusive date range (YYYY-MM-DD).
        Returns deserialized EarthquakeEvent objects — not raw tuples.
        ISO string comparison works correctly for date-prefixed timestamps.
        """
        sql = """
            SELECT * FROM raw_events
            WHERE occurred_at >= ? AND occurred_at < ?
            ORDER BY occurred_at ASC
        """
        # end_date is inclusive so bump to next day for the < comparison
        end_exclusive = _next_day(end_date)

        conn = self._connect()
        try:
            rows = conn.execute(sql, (start_date, end_exclusive)).fetchall()
        finally:
            conn.close()

        return [_row_to_event(row) for row in rows]

    # ------------------------------------------------------------------
    # Daily aggregates
    # ------------------------------------------------------------------

    def upsert_aggregates(self, aggregates: list[DailyAggregate]) -> None:
        """
        Insert or overwrite daily magnitude bucket counts.
        ON CONFLICT on (date, bucket) makes reruns fully idempotent.
        Empty list is a safe no-op.
        """
        if not aggregates:
            logger.debug("upsert_aggregates_noop")
            return

        rows = [
            (
                agg.date,
                agg.bucket,
                agg.count,
                _dt_to_iso(agg.updated_at),
            )
            for agg in aggregates
        ]

        sql = """
            INSERT INTO daily_aggregates (date, bucket, count, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (date, bucket) DO UPDATE SET
                count      = excluded.count,
                updated_at = excluded.updated_at
        """

        with self._transaction() as conn:
            conn.executemany(sql, rows)

        logger.info(
            "aggregates_upserted",
            extra={"count": len(aggregates)},
        )

    def get_aggregates_by_date(self, date: str) -> list[DailyAggregate]:
        """
        Fetch all bucket counts for a specific date (YYYY-MM-DD).
        Returns deserialized DailyAggregate objects.
        """
        sql = """
            SELECT date, bucket, count, updated_at
            FROM daily_aggregates
            WHERE date = ?
            ORDER BY bucket ASC
        """
        conn = self._connect()
        try:
            rows = conn.execute(sql, (date,)).fetchall()
        finally:
            conn.close()

        return [
            DailyAggregate(
                date=row["date"],
                bucket=row["bucket"],
                count=row["count"],
                updated_at=datetime.fromisoformat(row["updated_at"]),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Pipeline runs
    # ------------------------------------------------------------------

    def start_run(self) -> int:
        """
        Record the start of a pipeline execution.
        Returns the run_id — pass this to complete_run() or fail_run().
        """
        sql = """
            INSERT INTO pipeline_runs (started_at, status)
            VALUES (?, 'running')
        """
        with self._transaction() as conn:
            cursor = conn.execute(sql, (_now_iso(),))
            run_id = cursor.lastrowid

        logger.info("pipeline_run_started", extra={"run_id": run_id})
        return run_id

    def complete_run(
        self, run_id: int, events_fetched: int, pages_fetched: int
    ) -> None:
        """Mark a pipeline run as successful with final metrics."""
        sql = """
            UPDATE pipeline_runs
            SET status         = 'success',
                completed_at   = ?,
                events_fetched = ?,
                pages_fetched  = ?
            WHERE id = ?
        """
        with self._transaction() as conn:
            conn.execute(sql, (_now_iso(), events_fetched, pages_fetched, run_id))

        logger.info(
            "pipeline_run_completed",
            extra={
                "run_id": run_id,
                "events_fetched": events_fetched,
                "pages_fetched": pages_fetched,
            },
        )

    def fail_run(self, run_id: int, error: str) -> None:
        """Mark a pipeline run as failed with the error message."""
        sql = """
            UPDATE pipeline_runs
            SET status        = 'failed',
                completed_at  = ?,
                error_message = ?
            WHERE id = ?
        """
        with self._transaction() as conn:
            conn.execute(sql, (_now_iso(), error, run_id))

        logger.warning(
            "pipeline_run_failed",
            extra={"run_id": run_id, "error": error},
        )


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dt_to_iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _next_day(date_str: str) -> str:
    """Bump a YYYY-MM-DD string by one day for exclusive range queries."""
    from datetime import timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d")
    return (d + timedelta(days=1)).strftime("%Y-%m-%d")


def _row_to_event(row: sqlite3.Row) -> EarthquakeEvent:
    """Deserialize a raw_events DB row back into an EarthquakeEvent."""
    return EarthquakeEvent(
        event_id=row["event_id"],
        magnitude=row["magnitude"],
        place=row["place"],
        occurred_at=datetime.fromisoformat(row["occurred_at"]),
        usgs_updated_at=(
            datetime.fromisoformat(row["usgs_updated_at"])
            if row["usgs_updated_at"] else None
        ),
        latitude=row["latitude"],
        longitude=row["longitude"],
        depth_km=row["depth_km"],
        event_type=row["event_type"],
        raw_status=row["raw_status"],
    )