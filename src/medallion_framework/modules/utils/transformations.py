"""
Common transformation utilities used across all medallion layers
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp, current_date, col, trim, lower, 
    when, lit, input_file_name, monotonically_increasing_id
)
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class TransformationUtils:
    """Reusable transformation methods for all layers"""
    
    @staticmethod
    def add_metadata_columns(
        df: DataFrame,
        columns: List[str],
        layer_name: str = None
    ) -> DataFrame:
        """
        Add standard metadata columns to DataFrame
        
        Args:
            df: Input DataFrame
            columns: List of metadata columns to add
            layer_name: Current layer name (bronze, silver, gold)
            
        Returns:
            DataFrame with metadata columns added
        """
        if 'load_timestamp' in columns:
            df = df.withColumn("load_timestamp", current_timestamp())
        
        if 'load_date' in columns:
            df = df.withColumn("load_date", current_date())
        
        if 'load_run_id' in columns:
            # Use unique identifier per run
            df = df.withColumn("load_run_id", lit("run_" + current_timestamp().cast("string")))
        
        if 'source_file' in columns:
            df = df.withColumn("source_file", input_file_name())
        
        if 'layer_name' in columns and layer_name:
            df = df.withColumn("layer_name", lit(layer_name))
        
        if 'processed_at' in columns:
            df = df.withColumn("processed_at", current_timestamp())
        
        if 'record_id' in columns:
            df = df.withColumn("record_id", monotonically_increasing_id())
        
        logger.info(f"Added metadata columns: {columns}")
        return df
    
    @staticmethod
    def standardize_columns(
        df: DataFrame,
        trim_strings: bool = False,
        lowercase: bool = False,
        remove_special_chars: bool = False
    ) -> DataFrame:
        """
        Standardize string columns across DataFrame
        
        Args:
            df: Input DataFrame
            trim_strings: Remove leading/trailing whitespace
            lowercase: Convert to lowercase
            remove_special_chars: Remove special characters
            
        Returns:
            Standardized DataFrame
        """
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            
            if col_type == 'string':
                transformations = []
                
                if trim_strings:
                    df = df.withColumn(col_name, trim(col(col_name)))
                    transformations.append("trim")
                
                if lowercase:
                    df = df.withColumn(col_name, lower(col(col_name)))
                    transformations.append("lowercase")
                
                if remove_special_chars:
                    from pyspark.sql.functions import regexp_replace
                    df = df.withColumn(
                        col_name, 
                        regexp_replace(col(col_name), r'[^a-zA-Z0-9_]', '')
                    )
                    transformations.append("remove_special_chars")
                
                if transformations:
                    logger.debug(f"Column {col_name}: {', '.join(transformations)}")
        
        logger.info("Standardization applied to string columns")
        return df
    
    @staticmethod
    def rename_columns_to_lowercase(df: DataFrame) -> DataFrame:
        """
        Rename all columns to lowercase
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with lowercase column names
        """
        new_columns = [col_name.lower() for col_name in df.columns]
        df = df.toDF(*new_columns)
        logger.info(f"Renamed {len(new_columns)} columns to lowercase")
        return df
    
    @staticmethod
    def rename_columns_to_uppercase(df: DataFrame) -> DataFrame:
        """
        Rename all columns to uppercase
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with uppercase column names
        """
        new_columns = [col_name.upper() for col_name in df.columns]
        df = df.toDF(*new_columns)
        logger.info(f"Renamed {len(new_columns)} columns to uppercase")
        return df
    
    @staticmethod
    def rename_columns_by_mapping(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
        """
        Rename columns based on provided mapping
        
        Args:
            df: Input DataFrame
            mapping: Dictionary of {old_name: new_name}
            
        Returns:
            DataFrame with renamed columns
        """
        for old_name, new_name in mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        logger.info(f"Renamed {len(mapping)} columns")
        return df
    
    @staticmethod
    def add_custom_columns(
        df: DataFrame,
        columns: Dict[str, str]
    ) -> DataFrame:
        """
        Add custom calculated columns
        
        Args:
            df: Input DataFrame
            columns: Dictionary of {column_name: sql_expression}
            
        Returns:
            DataFrame with new columns
        """
        from pyspark.sql.functions import expr
        
        for col_name, col_expression in columns.items():
            try:
                df = df.withColumn(col_name, expr(col_expression))
                logger.info(f"Added column: {col_name}")
            except Exception as e:
                logger.warning(f"Failed to add column {col_name}: {str(e)}")
        
        return df
    
    @staticmethod
    def remove_duplicates(df: DataFrame, subset: List[str] = None) -> DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for duplication (all if None)
            
        Returns:
            DataFrame with duplicates removed
        """
        original_count = df.count()
        
        if subset:
            df = df.dropDuplicates(subset)
            logger.info(f"Removed duplicates based on columns: {subset}")
        else:
            df = df.dropDuplicates()
            logger.info("Removed exact duplicates")
        
        final_count = df.count()
        logger.info(f"Rows removed: {original_count - final_count}")
        
        return df
    
    @staticmethod
    def fill_nulls(df: DataFrame, fill_values: Dict[str, Any]) -> DataFrame:
        """
        Fill null values with specified defaults
        
        Args:
            df: Input DataFrame
            fill_values: Dictionary of {column: fill_value}
            
        Returns:
            DataFrame with nulls filled
        """
        df = df.fillna(fill_values)
        logger.info(f"Filled null values in {len(fill_values)} columns")
        return df
    
    @staticmethod
    def drop_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Drop specified columns
        
        Args:
            df: Input DataFrame
            columns: List of column names to drop
            
        Returns:
            DataFrame with columns dropped
        """
        existing_cols = [col for col in columns if col in df.columns]
        df = df.drop(*existing_cols)
        logger.info(f"Dropped {len(existing_cols)} columns")
        return df
    
    @staticmethod
    def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Select only specified columns
        
        Args:
            df: Input DataFrame
            columns: List of column names to keep
            
        Returns:
            DataFrame with only specified columns
        """
        existing_cols = [col for col in columns if col in df.columns]
        df = df.select(*existing_cols)
        logger.info(f"Selected {len(existing_cols)} columns")
        return df
    
    @staticmethod
    def add_data_quality_flag(
        df: DataFrame,
        quality_rules: Dict[str, Any]
    ) -> DataFrame:
        """
        Add quality flag column based on rules
        
        Args:
            df: Input DataFrame
            quality_rules: Dictionary of quality rules per column
            
        Returns:
            DataFrame with quality_flag column
        """
        quality_flag = lit(True)
        
        for column, rules in quality_rules.items():
            if column not in df.columns:
                continue
            
            if isinstance(rules, dict):
                # Rule: column cannot be null
                if 'nulls_allowed' in rules and not rules['nulls_allowed']:
                    quality_flag = quality_flag & col(column).isNotNull()
                
                # Rule: column must be within range
                if 'min_value' in rules:
                    quality_flag = quality_flag & (col(column) >= rules['min_value'])
                
                if 'max_value' in rules:
                    quality_flag = quality_flag & (col(column) <= rules['max_value'])
        
        df = df.withColumn("quality_flag", quality_flag)
        logger.info("Added quality_flag column")
        return df
    
    @staticmethod
    def log_dataframe_stats(df: DataFrame, label: str = "") -> DataFrame:
        """
        Log DataFrame statistics for debugging
        
        Args:
            df: Input DataFrame
            label: Label for the log message
            
        Returns:
            DataFrame (unchanged)
        """
        row_count = df.count()
        col_count = len(df.columns)
        
        logger.info(f"{label} | Rows: {row_count}, Columns: {col_count}")
        logger.debug(f"Schema: {df.schema}")
        
        return df
