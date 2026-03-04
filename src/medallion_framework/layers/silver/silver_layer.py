from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, coalesce, trim, lower
from typing import Dict, Any, List
import logging
from ..core.layer import MedallionLayer
from ..modules.utils.transformations import TransformationUtils

logger = logging.getLogger(__name__)


class SilverLayer(MedallionLayer):
    """
    Silver Layer: Applies data quality and business rules
    
    Responsibilities:
    - Data validation and quality checks
    - Deduplication
    - Standardization (case, nulls, data types)
    - Data lineage tracking
    - Cleansing transformations
    - Business rule validations
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config, layer_name="SILVER")
        
        # Source configuration
        source = config.get('source', {})
        self.database = source.get('database', 'default')
        self.source_table = source.get('table')
        
        # Transform configuration
        transform = config.get('transform', {})
        self.dedup_config = transform.get('deduplication', {})
        self.standardize_config = transform.get('standardization', {})
        self.quality_config = transform.get('quality_checks', {})
        self.business_rules_config = transform.get('business_rules', {})
        self.custom_columns_config = transform.get('custom_columns', {})
        
        # Destination configuration
        destination = config.get('destination', {})
        self.output_table = destination.get('table')
        self.partition_columns = destination.get('partitions', [])
        self.write_mode = destination.get('write_mode', 'overwrite')
    
    def extract(self) -> DataFrame:
        """
        Read cleaned/validated data from bronze table
        """
        logger.info(f"Extracting data from {self.database}.{self.source_table}")
        
        df = self.spark.table(f"{self.database}.{self.source_table}")
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality rules and transformations:
        - Deduplication
        - Null handling
        - Data type standardization
        - Business rule validations
        - String standardization (trim, lowercase)
        """
        logger.info("Applying silver transformations")
        
        # 1. Deduplication
        if self.dedup_config.get('enabled', False):
            df = TransformationUtils.remove_duplicates(df)
        
        # 2. Data quality checks
        if self.quality_config.get('enabled', False):
            logger.info("Running data quality checks")
            df = self._apply_quality_rules(df)
        
        # 3. Standardization (trim, lowercase, null handling)
        if self.standardize_config.get('enabled', False):
            trim_strings = self.standardize_config.get('trim_strings', False)
            lowercase = self.standardize_config.get('lowercase', False)
            df = TransformationUtils.standardize_columns(df, trim_strings, lowercase)
        
        # 4. Business rule validations
        if self.business_rules_config.get('enabled', False):
            logger.info("Applying business validations")
            df = self._apply_business_rules(df)
        
        # 5. Custom columns
        if self.custom_columns_config:
            df = TransformationUtils.add_custom_columns(df, self.custom_columns_config)
        
        return df
    
    def _apply_quality_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality rules (null checks, outlier detection, etc.)
        Rules defined in config YAML under transform.quality_checks.rules
        """
        rules = self.quality_config.get('rules', {})
        
        for column, rule in rules.items():
            logger.info(f"Applying quality rule to {column}: {rule}")
            
            if column not in df.columns:
                logger.warning(f"Column {column} not found in DataFrame")
                continue
            
            if isinstance(rule, dict):
                if 'max_nulls_percent' in rule:
                    max_nulls = rule['max_nulls_percent']
                    null_count = df.filter(col(column).isNull()).count()
                    null_percent = (null_count / df.count()) * 100 if df.count() > 0 else 0
                    
                    if null_percent > max_nulls:
                        logger.warning(f"Column {column} has {null_percent:.2f}% nulls (threshold: {max_nulls}%)")
        
        return df
    
    def _standardize_data(self, df: DataFrame) -> DataFrame:
        """
        Delegated to TransformationUtils.standardize_columns()
        """
        return df
    
    def _add_custom_columns(self, df: DataFrame) -> DataFrame:
        """
        Delegated to TransformationUtils.add_custom_columns()
        """
        return df
    
    def load(self, df: DataFrame) -> None:
        """
        Write validated/cleaned data to silver table
        - Uses Delta format for ACID transactions
        - Merge strategy for incremental loads
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
        
        logger.info(f"Silver table created/updated: {self.database}.{self.output_table}")
    
    def run(self) -> None:
        """Customized run for silver layer"""
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
