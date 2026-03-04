from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, concat_ws, 
    row_number, dense_rank, current_timestamp
)
from typing import Dict, Any, List
import logging
from ..core.layer import MedallionLayer
from ..modules.utils.transformations import TransformationUtils

logger = logging.getLogger(__name__)


class GoldLayer(MedallionLayer):
    """
    Gold Layer: Business-ready aggregations and reporting tables
    
    Responsibilities:
    - Create aggregated views for reporting
    - Business metrics and KPIs
    - Dimensional tables
    - Star schema modeling
    - Optimized for analytics/BI tools
    - High-level business logic
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__(spark, config, layer_name="GOLD")
        
        # Source configuration
        source = config.get('source', {})
        self.database = source.get('database', 'default')
        self.source_table = source.get('table')
        
        # Transform configuration
        transform = config.get('transform', {})
        self.grain = transform.get('aggregation_grain', 'daily')
        self.dimensions = transform.get('dimensions', [])
        self.metrics = transform.get('metrics', {})
        self.custom_dimensions = transform.get('custom_dimensions', {})
        
        # Destination configuration
        destination = config.get('destination', {})
        self.output_table = destination.get('table')
        self.partition_columns = destination.get('partitions', [])
        self.write_mode = destination.get('write_mode', 'overwrite')
        self.create_view = destination.get('create_view', False)
        self.view_name = destination.get('view_name', f"{self.output_table}_view")
    
    def extract(self) -> DataFrame:
        """
        Read validated data from silver table
        """
        logger.info(f"Extracting data from {self.database}.{self.source_table}")
        
        df = self.spark.table(f"{self.database}.{self.source_table}")
        return df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply business logic and aggregations:
        - Create aggregated metrics
        - Build dimensional tables
        - Calculate KPIs
        - Apply business rules for reporting
        """
        logger.info("Applying gold transformations (business aggregations)")
        
        # 1. Add custom business dimensions
        if self.custom_dimensions:
            logger.info(f"Adding custom dimensions")
            df = TransformationUtils.add_custom_columns(df, self.custom_dimensions)
        
        # 2. Apply time-series aggregation grain
        logger.info(f"Applying {self.grain} aggregation grain")
        df = self._apply_aggregations(df)
        
        # 3. Calculate metrics/KPIs
        if self.metrics:
            logger.info(f"Computing {len(self.metrics)} metrics")
            df = self._calculate_metrics(df)
        
        # 4. Add reporting metadata
        df = self._add_reporting_metadata(df)
        
        return df
    
    def _add_dimensions(self, df: DataFrame) -> DataFrame:
        """
        Delegated to TransformationUtils.add_custom_columns()
        """
        return df
    
    def _calculate_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate business KPIs and metrics based on config
        Metrics defined as key-value pairs with SQL expressions
        """
        if not self.dimensions:
            logger.warning("No dimensions specified for aggregation, skipping metric calculation")
            return df
        
        from pyspark.sql import functions as F
        
        agg_dict = {}
        
        # Convert metric expressions to aggregation functions
        for metric_name, metric_expr in self.metrics.items():
            logger.info(f"Adding metric: {metric_name}")
            try:
                # Parse common SQL aggregation functions
                expr_lower = metric_expr.lower()
                if 'sum(' in expr_lower:
                    col_name = metric_expr.split('(')[1].split(')')[0]
                    agg_dict[metric_name] = F.sum(col_name)
                elif 'avg(' in expr_lower:
                    col_name = metric_expr.split('(')[1].split(')')[0]
                    agg_dict[metric_name] = F.avg(col_name)
                elif 'max(' in expr_lower:
                    col_name = metric_expr.split('(')[1].split(')')[0]
                    agg_dict[metric_name] = F.max(col_name)
                elif 'min(' in expr_lower:
                    col_name = metric_expr.split('(')[1].split(')')[0]
                    agg_dict[metric_name] = F.min(col_name)
                elif 'count(' in expr_lower:
                    agg_dict[metric_name] = F.count('*')
                else:
                    logger.warning(f"Unsupported metric expression: {metric_expr}")
            except Exception as e:
                logger.warning(f"Could not parse metric {metric_name}: {str(e)}")
        
        if agg_dict:
            df = df.groupBy(*self.dimensions).agg(agg_dict)
        
        return df
    
    def _apply_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Apply time-series aggregations based on grain (daily, hourly, etc.)
        """
        from pyspark.sql.functions import date_trunc
        
        if 'load_timestamp' in df.columns:
            # Truncate to specified grain
            if self.grain == 'daily':
                df = df.withColumn('agg_date', date_trunc('day', col('load_timestamp')))
            elif self.grain == 'hourly':
                df = df.withColumn('agg_date', date_trunc('hour', col('load_timestamp')))
            elif self.grain == 'weekly':
                df = df.withColumn('agg_date', date_trunc('week', col('load_timestamp')))
            elif self.grain == 'monthly':
                df = df.withColumn('agg_date', date_trunc('month', col('load_timestamp')))
        
        return df
    
    def _add_reporting_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add metadata for reporting: processing timestamp, version, etc.
        """
        df = df.withColumn('report_generated_at', current_timestamp()) \
               .withColumn('report_version', col('report_generated_at').cast('string'))
        
        return df
    
    def load(self, df: DataFrame) -> None:
        """
        Write aggregated/business-ready data to gold table
        - Optimized for BI/Analytics tools
        - Can be exposed via Databricks SQL
        """
        logger.info(f"Loading data to {self.database}.{self.output_table} (mode: {self.write_mode})")
        
        # Show summary stats before writing
        logger.info(f"Gold table row count: {df.count()}")
        
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
        
        logger.info(f"Gold table created/updated: {self.database}.{self.output_table}")
        
        # Create optional view for easy SQL access
        if self.create_view:
            self.spark.sql(f"""
                CREATE OR REPLACE VIEW {self.database}.{self.view_name} AS
                SELECT * FROM {self.database}.{self.output_table}
            """)
            logger.info(f"View created: {self.database}.{self.view_name}")
    
    def run(self) -> None:
        """Customized run for gold layer"""
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
