import logging
import sys

def setup_logger():
    """Configures and returns a simple logger instance."""
    logger = logging.getLogger('WebScraper')
    logger.setLevel(logging.INFO)
    
    # Check if handlers already exist to prevent duplicate logs in subsequent calls
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

# Initialize the logger for easy importing
log = setup_logger()