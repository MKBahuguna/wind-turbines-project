from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
import logging

# audit helpers imported lazily inside run to avoid circular imports

logger = logging.getLogger(__name__)


class MedallionLayer(ABC):
    """Abstract base class for medallion architecture layers"""
    
    def __init__(
        self, 
        spark: SparkSession, 
        config: Dict[str, Any], 
        layer_name: str
    ):
        self.spark = spark
        self.config = config
        self.layer_name = layer_name
        self.database = config.get('database', 'default')
        
    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extract/Read data from source (file, table, API, etc.)
        
        Returns:
            DataFrame: Raw data from source
        """
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply layer-specific transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        pass
    
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Write transformed data to target (table, delta, etc.)
        
        Args:
            df: DataFrame to write
        """
        pass
    
    @abstractmethod
    def run(self) -> None:
        """
        Execute the pipeline for this layer.

        Concrete implementations may include additional steps such as
        validation, enrichment, or custom auditing logic.  The base
        class does not provide a default so layers can override freely.
        """
        pass
