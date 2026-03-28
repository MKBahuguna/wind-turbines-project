import io
import logging
from typing import Optional
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row
from datetime import datetime


class CustomFormatter(logging.Formatter):
    """
    CustomFormatter shortens logger names and optionally appends function names for concise log output.
    Useful in Databricks notebooks and Spark jobs to improve log readability.
    """
    def __init__(self, fmt=None, datefmt=None, style="%"):
        super().__init__(fmt, datefmt, style)

    def format(self, record):
        """
        Format the log record by abbreviating the logger name and appending the function name for concise output.
        """
        # Abbreviate logger name for concise log output
        parts = record.name.split('.')
        if len(parts) > 1:
            abbrev = '.'.join([p[0] for p in parts[:-1]])
            last = parts[-1]
            short_name = f"{abbrev}.{last}"
        else:
            short_name = record.name
        func = getattr(record, "funcName", None)
        if func:
            short_name = f"{short_name}:{func}"
        original_name = record.name
        record.name = short_name
        formatted = super().format(record)
        record.name = original_name
        return formatted

class LogManager:
    """
    LogManager provides comprehensive log management for the wheel job.
    It enables in-memory log capturing, custom formatting, and structured log persistence to Delta tables.
    Features include:
      - Capturing stdout and stderr logs with custom formatting for improved readability.
      - Attaching and detaching log handlers dynamically to control log capture lifecycle.
      - Retrieving and clearing captured logs for inspection or downstream processing.
      - Writing structured logs and audit logs to Delta tables, supporting operational monitoring and traceability.
    Designed for integration with Databricks workflows, LogManager supports both standard and audit log persistence,
    and can be extended for additional log enrichment or custom storage targets.
    """
    def __init__(self):
        """Initialize LogManager with in-memory streams and custom log handlers."""
        # Initialize in-memory streams for capturing logs
        self.stdout_stream = io.StringIO()
        self.stderr_stream = io.StringIO()
        # Set up logging handlers to write logs to the in-memory streams
        self.stdout_handler = logging.StreamHandler(self.stdout_stream)
        self.stderr_handler = logging.StreamHandler(self.stderr_stream)
        # Set log levels for handlers: INFO for stdout, ERROR for stderr
        self.stdout_handler.setLevel(logging.INFO)
        self.stderr_handler.setLevel(logging.ERROR)
        # Apply pretty custom formatter to both handlers for consistent log formatting
        formatter = CustomFormatter(
            fmt="\n🌟 %(asctime)s 🌟\n[%(levelname)s] | %(name)s | %(funcName)s\n→ %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.stdout_handler.setFormatter(formatter)
        self.stderr_handler.setFormatter(formatter)
        # Track whether log capture is currently active
        self.is_active = False

    def setup_logger(self, name: str) -> logging.Logger:
        """Set up a logger with custom formatting."""
        # Set up a logger with custom formatting
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = CustomFormatter(
                fmt="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def start_capture(self):
        """Attach handlers to root logger to start capturing logs."""
        # Attach handlers to root logger to start capturing logs
        if self.is_active:
            return
        root_logger = logging.getLogger()
        root_logger.addHandler(self.stdout_handler)
        root_logger.addHandler(self.stderr_handler)
        self.is_active = True

    def stop_capture(self):
        """Detach handlers from root logger to stop capturing logs."""
        # Detach handlers from root logger to stop capturing logs
        if not self.is_active:
            return
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.stdout_handler)
        root_logger.removeHandler(self.stderr_handler)
        self.is_active = False

    def get_captured_output(self):
        """Retrieve captured stdout and stderr logs."""
        # Retrieve captured stdout and stderr logs
        return self.stdout_stream.getvalue(), self.stderr_stream.getvalue()

    def clear_captured_output(self):
        """Clear the in-memory log streams."""
        # Clear the in-memory log streams
        self.stdout_stream.seek(0)
        self.stdout_stream.truncate(0)
        self.stderr_stream.seek(0)
        self.stderr_stream.truncate(0)

    def write_structured_logs_to_delta(
        self,
        delta_table_path: str,
        spark: Optional[object] = None
    ):
        """Write structured logs to Delta table."""
        # Write structured logs to Delta table
        if not self.is_active:
            raise RuntimeError("Log capture is not active. Call start_capture() first.")
        stdout_logs, stderr_logs = self.get_captured_output()
        import re
        log_pattern = re.compile(
            r"\n🌟 (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) 🌟\n\[(\w+)\] \| ([^|]+) \| ([^\\n]+)\n→ (.+)"
        )
        log_rows = []
        for line in stdout_logs.split("\n🌟")[1:] + stderr_logs.split("\n🌟")[1:]:
            # Each log entry starts with "\n🌟", so split and parse
            entry = "\n🌟" + line
            match = log_pattern.match(entry)
            if match:
                timestamp, level, logger, func, message = match.groups()
                log_rows.append(Row(
                    timestamp=timestamp,
                    logger=logger,
                    level=level,
                    func=func,
                    message=message
                ))
        if not log_rows:
            return
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(log_rows)
        df.write.format("delta").mode("append").save(delta_table_path)
        self.clear_captured_output()

    def write_audit_logs_to_delta(
        log_manager,
        delta_table_path,
        args=None,
        layer=None,
        spark=None
    ):
        """Write audit logs to Delta table with additional metadata."""
        # Write audit logs to Delta table with additional metadata
        schema = StructType([
            StructField("created_timestamp", StringType(), True),
            StructField("args", StringType(), True),
            StructField("layer", StringType(), True),
            StructField("log_message", StringType(), True)
        ])

        if not log_manager.is_active:
            raise RuntimeError("Log capture is not active. Call start_capture() first.")

        stdout_logs, stderr_logs = log_manager.get_captured_output()
        all_logs = [line for line in stdout_logs.splitlines() + stderr_logs.splitlines() if line.strip()]
        created_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        log_rows = [
            Row(
                created_timestamp=created_timestamp,
                args=args,
                layer=layer,
                log_message=line
            )
            for line in all_logs
        ]

        if not log_rows:
            return

        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

        df = spark.createDataFrame(log_rows, schema=schema)
        df.write.format("delta").mode("append").save(delta_table_path)
        log_manager.clear_captured_output()
