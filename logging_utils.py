"""
Utility functions for logging configuration.
"""

import io
import logging
from typing import Tuple


class LogCapture:
    """Capture logging output to separate stdout/stderr buffers."""

    def __init__(self):
        self.stdout_stream = io.StringIO()
        self.stderr_stream = io.StringIO()
        self.stdout_handler = logging.StreamHandler(self.stdout_stream)
        self.stderr_handler = logging.StreamHandler(self.stderr_stream)
        self.stdout_handler.setLevel(logging.INFO)
        self.stderr_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            fmt="[%(asctime)s - %(name)s - %(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.stdout_handler.setFormatter(formatter)
        self.stderr_handler.setFormatter(formatter)
        self.is_active = False

    def start_capture(self):
        """Start capturing logging output."""
        if self.is_active:
            return
        root_logger = logging.getLogger()
        root_logger.addHandler(self.stdout_handler)
        root_logger.addHandler(self.stderr_handler)
        self.is_active = True

    def stop_capture(self):
        """Stop capturing logging output and remove handlers."""
        if not self.is_active:
            return
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.stdout_handler)
        root_logger.removeHandler(self.stderr_handler)
        self.is_active = False

    def get_captured_output(self) -> Tuple[str, str]:
        """Return captured stdout and stderr logs."""
        return self.stdout_stream.getvalue(), self.stderr_stream.getvalue()

    def clear_captured_output(self):
        """Clear captured logs."""
        self.stdout_stream.seek(0)
        self.stdout_stream.truncate(0)
        self.stderr_stream.seek(0)
        self.stderr_stream.truncate(0)


class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style="%"):
        super().__init__(fmt, datefmt, style)

    def format(self, record):
        # Abbreviate logger name: f.m.i.reader.FileManager
        parts = record.name.split('.')
        if len(parts) > 1:
            abbrev = '.'.join([p[0] for p in parts[:-1]])
            last = parts[-1]
            short_name = f"{abbrev}.{last}"
        else:
            short_name = record.name

        # Add function name if available
        func = getattr(record, "funcName", None)
        if func:
            short_name = f"{short_name}:{func}"

        original_name = record.name
        record.name = short_name
        formatted = super().format(record)
        record.name = original_name
        return formatted

def setup_logger(name: str) -> logging.Logger:
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
    
def write_structured_logs_to_delta(log_capture: LogCapture, delta_table_path: str, spark=None):
    """
    Write captured logs to a Delta table with structured columns.
    """
    if not log_capture.is_active:
        raise RuntimeError("Log capture is not active. Call start_capture() first.")

    stdout_logs, stderr_logs = log_capture.get_captured_output()
    import re
    from pyspark.sql import Row

    log_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - ([^ ]+) - (\w+)\] (.+)')
    log_rows = []

    for line in stdout_logs.splitlines() + stderr_logs.splitlines():
        match = log_pattern.match(line.strip())
        if match:
            timestamp, name, level, message = match.groups()
            log_rows.append(Row(timestamp=timestamp, logger=name, level=level, message=message))

    if not log_rows:
        return

    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(log_rows)
    df.write.format("delta").mode("append").save(delta_table_path)

# def write_raw_logs_to_delta(log_capture: LogCapture, delta_table_path: str, spark=None):
#     """
#     Write captured logs to a Delta table as a single column.
#     """
#     if not log_capture.is_active:
#         raise RuntimeError("Log capture is not active. Call start_capture() first.")

#     stdout_logs, stderr_logs = log_capture.get_captured_output()
#     all_logs = stdout_logs.splitlines() + stderr_logs.splitlines()

#     from pyspark.sql import Row

#     log_rows = [Row(log=line) for line in all_logs if line.strip()]

#     if not log_rows:
#         return

#     if spark is None:
#         from pyspark.sql import SparkSession
#         spark = SparkSession.builder.getOrCreate()

#     df = spark.createDataFrame(log_rows)
#     df.write.format("delta").mode("append").save(delta_table_path)
