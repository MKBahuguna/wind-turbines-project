from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from typing import Dict, Any
import logging
from ..core.layer import MedallionLayer
from ..modules.utils.transformations import TransformationUtils

logger = logging.getLogger(__name__)


class BronzeLayer(MedallionLayer):
    """
    Bronze Layer: Ingests raw data from external sources
    
    Responsibilities:
    - Read raw data from files/APIs without modification
    - Minimal schema enforcement
    - Add metadata (load_timestamp, source, file_name)
    - Support incremental loads
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config, layer_name="BRONZE")
        
        # Source configuration
        source = config.get('source', {})
        self.source_path = source.get('path')
        self.file_format = source.get('format', 'csv')
        self.source_options = source.get('options', {})
        
        # Transform configuration
        transform = config.get('transform', {})
        self.remove_duplicates = transform.get('remove_duplicates', False)
        self.add_metadata_columns = transform.get('add_metadata_columns', [])
        self.column_case = transform.get('column_case', 'as_is')
        
        # Destination configuration
        destination = config.get('destination', {})
        self.database = destination.get('database', 'default')
        self.output_table = destination.get('table')
        self.partition_columns = destination.get('partitions', [])
        self.write_mode = destination.get('write_mode', 'overwrite')
    
    def extract(self) -> DataFrame:
        """
        Read raw data from source path
        Supports CSV, Parquet, JSON, Delta formats
        """
        logger.info(f"Extracting data from {self.source_path} (format: {self.file_format})")
        
        if self.file_format.lower() == 'csv':
            df = self.spark.read \
                .options(**self.source_options) \
                .csv(self.source_path)
                
        elif self.file_format.lower() == 'parquet':
            df = self.spark.read.parquet(self.source_path)
            
        elif self.file_format.lower() == 'json':
            df = self.spark.read.json(self.source_path)
            
        elif self.file_format.lower() == 'delta':
            df = self.spark.read.format("delta").load(self.source_path)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")
        
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Minimal transformations for bronze layer:
        - Add load_timestamp and source_file metadata (if configured)
        - Convert column case (lowercase, uppercase, as_is)
        - Handle duplicates (optional)
        """
        logger.info("Applying bronze transformations (metadata)")
        
        # 1. Add metadata columns
        metadata_cols = self.add_metadata_columns
        if metadata_cols:
            df = TransformationUtils.add_metadata_columns(df, metadata_cols, "BRONZE")
        
        # 2. Remove duplicates if specified
        if self.remove_duplicates:
            df = TransformationUtils.remove_duplicates(df)
        
        # 3. Handle column case conversion
        if self.column_case == 'lowercase':
            df = TransformationUtils.rename_columns_to_lowercase(df)
        elif self.column_case == 'uppercase':
            df = TransformationUtils.rename_columns_to_uppercase(df)
        
        return df
    
    def load(self, df: DataFrame) -> None:
        """
        Write raw data to bronze table using Delta format
        - Uses write mode from config (overwrite or append)
        - Partition if specified
        """
        logger.info(f"Loading data to {self.database}.{self.output_table} (mode: {self.write_mode})")
        
        write_df = df.write \
            .format("delta") \
            .mode(self.write_mode) \
            .option("mergeSchema", "true")
        
        # Add partitioning if specified
        if self.partition_columns:
            write_df = write_df.partitionBy(*self.partition_columns)
        
        write_df.saveAsTable(
            f"{self.database}.{self.output_table}",
            path=f"/user/hive/warehouse/{self.database}.db/{self.output_table}"
        )
    
    def run(self) -> None:
        """Customized run sequence for bronze layer"""
        from ..modules.audit.audit_utils import AuditLogger, ErrorLogger
        audit_logger = AuditLogger(self.spark, self.config.get('audit', {}))
        error_logger = ErrorLogger(self.spark, self.config.get('audit', {}))
        
        try:
            logger.info(f"Starting {self.layer_name} layer")
            df = self.extract()
            src_count = df.count()
            logger.info(f"{self.layer_name}: Extracted {src_count} rows")
            audit_logger.log_start(self.layer_name, src_count)

            df = self.transform(df)
            logger.info(f"{self.layer_name}: Transformed data")

            self.load(df)
            tgt_count = df.count()
            logger.info(f"{self.layer_name}: Successfully loaded data")
            audit_logger.log_end(self.layer_name, tgt_count, status="SUCCESS")

        except Exception as e:
            logger.error(f"Error in {self.layer_name} layer: {e}")
            error_logger.log_error(self.layer_name, e)
            raise
