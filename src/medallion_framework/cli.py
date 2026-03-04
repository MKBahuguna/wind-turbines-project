"""
Command-line interface for Medallion Architecture pipeline

Entry points:
  run_bronze    - Execute bronze layer (raw data ingestion)
  run_silver    - Execute silver layer (data validation & cleansing)
  run_gold      - Execute gold layer (business aggregations)
"""

import sys
import logging
from pyspark.sql import SparkSession
from medallion_framework.core.config import ConfigLoader
from medallion_framework.layers.bronze.bronze_layer import BronzeLayer
from medallion_framework.layers.silver.silver_layer import SilverLayer
from medallion_framework.layers.gold.gold_layer import GoldLayer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Initialize Spark session for Databricks"""
    return SparkSession.builder \
        .appName("Medallion Architecture Pipeline") \
        .getOrCreate()


def run_bronze():
    """Execute bronze layer: raw data ingestion"""
    try:
        logger.info("=" * 60)
        logger.info("STARTING BRONZE LAYER EXECUTION")
        logger.info("=" * 60)
        
        # Load configuration
        config = ConfigLoader.load_config("bronze")
        
        # Initialize Spark
        spark = get_spark_session()
        
        # Create and run bronze layer
        bronze = BronzeLayer(spark, config)
        bronze.run()
        
        logger.info("=" * 60)
        logger.info("BRONZE LAYER COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"BRONZE LAYER FAILED: {str(e)}", exc_info=True)
        sys.exit(1)


def run_silver():
    """Execute silver layer: data validation and cleansing"""
    try:
        logger.info("=" * 60)
        logger.info("STARTING SILVER LAYER EXECUTION")
        logger.info("=" * 60)
        
        # Load configuration
        config = ConfigLoader.load_config("silver")
        
        # Initialize Spark
        spark = get_spark_session()
        
        # Create and run silver layer
        silver = SilverLayer(spark, config)
        silver.run()
        
        logger.info("=" * 60)
        logger.info("SILVER LAYER COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"SILVER LAYER FAILED: {str(e)}", exc_info=True)
        sys.exit(1)


def run_gold():
    """Execute gold layer: business-ready aggregations"""
    try:
        logger.info("=" * 60)
        logger.info("STARTING GOLD LAYER EXECUTION")
        logger.info("=" * 60)
        
        # Load configuration
        config = ConfigLoader.load_config("gold")
        
        # Initialize Spark
        spark = get_spark_session()
        
        # Create and run gold layer
        gold = GoldLayer(spark, config)
        gold.run()
        
        logger.info("=" * 60)
        logger.info("GOLD LAYER COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"GOLD LAYER FAILED: {str(e)}", exc_info=True)
        sys.exit(1)


def run_full_pipeline():
    """
    Execute the complete medallion pipeline:
    Bronze -> Silver -> Gold
    """
    try:
        logger.info("\n" + "=" * 60)
        logger.info("STARTING FULL MEDALLION PIPELINE")
        logger.info("=" * 60 + "\n")
        
        # Run all layers sequentially
        run_bronze()
        run_silver()
        run_gold()
        
        logger.info("\n" + "=" * 60)
        logger.info("FULL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60 + "\n")
        
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Parse command-line arguments to determine which layer to run
    if len(sys.argv) > 1:
        layer = sys.argv[1].lower()
        
        if layer == "bronze":
            run_bronze()
        elif layer == "silver":
            run_silver()
        elif layer == "gold":
            run_gold()
        elif layer == "all":
            run_full_pipeline()
        else:
            print(f"Unknown layer: {layer}")
            print("Usage: python -m medallion_framework.cli [bronze|silver|gold|all]")
            sys.exit(1)
    else:
        # Default: run full pipeline
        run_full_pipeline()
