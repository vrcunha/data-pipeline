"""Structured logger helpers used across the pipeline."""

import json
import logging


class StructuredLogger:
    """Emit JSON-formatted logs with consistent severity fields."""

    def __init__(self, name: str):
        """Create a logger instance.

        Args:
            name: Logger name.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)

    def info(self, message: str, **kwargs):
        """Log an informational event.

        Args:
            message: Main log message.
            **kwargs: Extra fields appended to the JSON payload.
        """
        log_entry = {"level": "INFO", "message": message, **kwargs}
        self.logger.info(json.dumps(log_entry))

    def error(self, message: str, **kwargs):
        """Log an error event.

        Args:
            message: Main log message.
            **kwargs: Extra fields appended to the JSON payload.
        """
        log_entry = {"level": "ERROR", "message": message, **kwargs}
        self.logger.error(json.dumps(log_entry))

    def warning(self, message: str, **kwargs):
        """Log a warning event.

        Args:
            message: Main log message.
            **kwargs: Extra fields appended to the JSON payload.
        """
        log_entry = {"level": "WARNING", "message": message, **kwargs}
        self.logger.warning(json.dumps(log_entry))
