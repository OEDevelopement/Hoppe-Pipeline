import logging
import os
from dotenv import load_dotenv
from config import Config
from pipeline import Pipeline

def setup_logging():
    """Konfiguriert das Logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("./logs/pipeline.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('pipeline')


logger = setup_logging()

# Load environment variables
load_dotenv()

# Check if environment variables are set
api_key = os.getenv('HOPPE_API_KEY')
if not api_key:
    logger.error("HOPPE_API_KEY environment variable not set")
    raise ValueError("HOPPE_API_KEY environment variable not set")

# Configure pipeline
config = Config(
    base_url=os.getenv('HOPPE_BASE_URL', "https://api.hoppe-sts.com/"),
    raw_path=os.getenv('RAW_PATH', "./data/raw_data"),
    transformed_path=os.getenv('TRANSFORMED_PATH', "./data/transformed_data"),
    gaps_path=os.getenv('GAPS_PATH', "./data/gaps_data"),
    batch_size = int(os.getenv('BATCH_SIZE', "1000")),
    max_workers=int(os.getenv('MAX_WORKERS', "4")),
    days_to_keep=int(os.getenv('DAYS_TO_KEEP', "90")),
    history_days=int(os.getenv('HISTORY_DAYS', "5"))
)

mode = "timeseries" # "all" or "timeseries" or "fleet"


# Create and run pipeline
try:
    pipeline = Pipeline(config, api_key)
    run_start, run_end = pipeline.run(mode)  
    logger.info(f"Pipeline run completed at {run_end}: total runtime {run_end - run_start}")
except Exception as e:
    logger.error(f"Pipeline run failed: {str(e)}")