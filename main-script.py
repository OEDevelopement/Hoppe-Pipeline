#!/usr/bin/env python3
"""
Hauptskript zum Ausführen der Hoppe ETL Pipeline
"""

import logging
import os
import argparse
from dotenv import load_dotenv
import sqlalchemy as sa

from config import Config
from pipeline import Pipeline

def setup_logging():
    """Konfiguriert das Logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("pipeline.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def run_pipeline(mode: str = "all") -> None:
    """
    Hauptfunktionsaufruf für die Pipeline
    
    Args:
        mode (str): 'all', 'timeseries', oder 'fleet'
    """
    logger = setup_logging()
    
    # Load environment variables
    load_dotenv()
    
    # Check if environment variables are set
    api_key = os.getenv('HOPPE_API_KEY')
    if not api_key:
        logger.error("HOPPE_API_KEY environment variable not set")
        return
    
    # Configure pipeline
    config = Config(
        base_url=os.getenv('HOPPE_BASE_URL', "https://api.hoppe-sts.com/"),
        raw_path=os.getenv('RAW_PATH', "./data/raw_data"),
        transformed_path=os.getenv('TRANSFORMED_PATH', "./data/transformed_data"),
        gaps_path=os.getenv('GAPS_PATH', "./data/gaps_data"),
        max_workers=int(os.getenv('MAX_WORKERS', "8")),
        retry_attempts=int(os.getenv('RETRY_ATTEMPTS', "5")),
        timeout=int(os.getenv('TIMEOUT', "45")),
        days_to_keep=int(os.getenv('DAYS_TO_KEEP', "90")),
        history_days=int(os.getenv('HISTORY_DAYS', "5"))
    )
    
    # Create and run pipeline
    pipeline = Pipeline(config, api_key)
    run_timestamp = pipeline.run(mode)
    
    # If database connection is configured, store to database
    db_connection_string = os.getenv('MSSQL_CONNECTION_STRING')
    if db_connection_string:
        try:
            engine = sa.create_engine(db_connection_string)
            logger.info("Database connection established")
            
            # Process and store data to database
            pipeline.process_and_store_to_db(engine, run_timestamp)
            logger.info("Database integration completed successfully")
        except Exception as e:
            logger.error(f"Database integration failed: {str(e)}")
    else:
        logger.warning("MSSQL_CONNECTION_STRING not set, skipping database integration")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the Hoppe API ETL pipeline")
    parser.add_argument(
        "--mode", 
        choices=["all", "timeseries", "fleet"], 
        default="all",
        help="Pipeline execution mode: 'all' (default), 'timeseries', or 'fleet'"
    )
    args = parser.parse_args()
    
    # Run the pipeline
    run_pipeline(args.mode)
