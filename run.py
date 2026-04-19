"""
Entrypoint for the earthquake pipeline.

Usage:
    python run.py                    # run with defaults
    python run.py --lookback 7       # last 7 days only
    python run.py --db /tmp/eq.db    # custom DB path
    python run.py --log-level DEBUG  # verbose console output

The pipeline:
  1. Configures structured logging (console + file)
  2. Instantiates Config
  3. Runs the Pipeline
  4. Exits 0 on success, 1 on failure

Exit codes matter for schedulers — cron and Airflow both check them.
"""

import argparse
import sys
from pathlib import Path

from earthquake.config import Config
from earthquake.logging_config import configure_logging
from earthquake.pipeline import Pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch USGS earthquake data and store daily aggregates."
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to SQLite database (default: earthquake.db in project root)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level (default: INFO)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = Config(
        lookback_days=args.lookback,
        **({"db_path": args.db} if args.db else {}),
    )

    configure_logging(
        log_level=args.log_level,
        log_file=config.log_file,
    )

    try:
        Pipeline(config=config).run()
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())