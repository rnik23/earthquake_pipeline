"""
Logging configuration for the earthquake pipeline.

Design decisions:
- Two handlers: console (INFO) and file (DEBUG).
  Console stays clean for normal operation.
  File captures full detail for 3am debugging.
- Structured extra fields (stage, run_id, events, etc) are logged
  as key=value pairs in the file handler for easy grep/parsing.
- JSON logging is intentionally skipped here — would add a dependency
  (python-json-logger) for minimal gain at this scale. In production
  with a log aggregator (Datadog, CloudWatch) you'd swap to JSON.
- Root logger is not touched — only the 'earthquake' namespace is
  configured. This prevents swallowing logs from third party libraries
  like requests or tenacity which have their own logging behavior.
"""

import logging
import sys
from pathlib import Path


class StructuredFormatter(logging.Formatter):
    """
    Formats log records as:
      2026-04-19 14:32:00 UTC | INFO     | fetcher | page_fetched | page=2 offset=1001 events=1000

    Extra fields passed via the `extra` dict are appended as key=value pairs.
    This makes log lines grep-friendly without requiring a JSON parser.
    """

    BASE_FORMAT = "{asctime} UTC | {levelname:<8} | {name:<20} | {message}"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # Fields that are always present on a LogRecord — exclude from extras
    BUILTIN_FIELDS = {
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "asctime", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        record.asctime = self.formatTime(record, self.DATE_FORMAT)
        base = self.BASE_FORMAT.format(
            asctime=record.asctime,
            levelname=record.levelname,
            name=record.name.replace("earthquake.", ""),
            message=record.getMessage(),
        )

        # Append any structured extra fields as key=value pairs
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in self.BUILTIN_FIELDS
        }
        if extras:
            kv = " | " + " ".join(f"{k}={v}" for k, v in extras.items())
            return base + kv

        return base


def configure_logging(
    log_level: str = "INFO",
    log_file: Path | None = None,
) -> None:
    """
    Configure the 'earthquake' logger with console and file handlers.

    Call this once at startup in run.py before anything else.
    Safe to call multiple times — clears existing handlers first.

    Args:
        log_level: console handler level, e.g. 'INFO' or 'DEBUG'
        log_file:  path to the log file. If None, file logging is skipped.
    """
    logger = logging.getLogger("earthquake")
    logger.setLevel(logging.DEBUG)  # capture everything, handlers filter
    logger.handlers.clear()         # safe to reconfigure

    formatter = StructuredFormatter()

    # --- Console handler (INFO by default) ---
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    console.setFormatter(formatter)
    logger.addHandler(console)

    # --- File handler (DEBUG always) ---
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.debug(
        "logging_configured",
        extra={
            "console_level": log_level,
            "log_file":      str(log_file) if log_file else "none",
        },
    )