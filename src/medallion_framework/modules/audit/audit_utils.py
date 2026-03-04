"""Simple audit and error logging helpers"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class AuditLogger:
    """Records start/end counts to an audit table"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config or {}
        # mandatory framework table names (can override via config)
        self.audit_table = self.config.get("audit_table", "pipeline_audit")

    def log_start(self, layer_name: str, source_count: int) -> None:
        logger.info(f"[AUDIT] {layer_name} start: source_count={source_count}")
        # Placeholder: actual write to audit_table

    def log_end(self, layer_name: str, target_count: int, status: str) -> None:
        logger.info(f"[AUDIT] {layer_name} end: target_count={target_count}, status={status}")
        # Placeholder: actual write to audit_table


class ErrorLogger:
    """Logs errors to an error table"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config or {}
        self.error_table = self.config.get("error_table", "pipeline_errors")

    def log_error(self, layer_name: str, error: Exception) -> None:
        logger.error(f"[ERROR_LOG] {layer_name}: {str(error)}")
        # Placeholder: write exception details to error_table
