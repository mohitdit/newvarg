import asyncio
import os
import json
from datetime import datetime
from scrapers.virginia_scraper import VirginiaScraper
from utils.logger import log
from utils.json_grouper import group_and_merge_json_files

# ----------------------------------------
# HARDCODED CONFIGURATIONS
# ----------------------------------------
# These will be replaced with API calls later

# Example 1: Civil Cases (GV prefix)
CIVIL_CONFIG = {
    "courtFips": "177",
    "courtName": "Virginia Beach General District Court",
    "searchFipsCode": 177,
    "searchDivision": "V",
    "docketNumber": "9120",  # Starting number (becomes 0007900)
    "docketYear": 2025,
    "caseType": "civil"  # Will use GV prefix
}

# Example 2: Criminal Cases (GC and GT prefixes)
CRIMINAL_CONFIG = {
    "courtFips": "750",
    "courtName": "Radford General District Court",
    "searchFipsCode": 750,
    "searchDivision": "T",
    "docketNumber": "2070",  # Starting number
    "docketYear": 2025,
    "caseType": "criminal"  # Will use GC and GT prefixes
}

# Choose which configuration to use
ACTIVE_CONFIG = CRIMINAL_CONFIG  # Change to CRIMINAL_CONFIG for criminal cases

# Output directories
OUTPUT_DIR = "data"
HTML_DIR = os.path.join(OUTPUT_DIR, "htmldata")
JSON_DIR = os.path.join(OUTPUT_DIR, "jsondata")

# ----------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------

def ensure_directories():
    """Create necessary directories if they don't exist"""
    os.makedirs(HTML_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)
    log.info(f"Output directories ready: {HTML_DIR}, {JSON_DIR}")


def print_summary(results: list, config: dict):
    """Print a formatted summary of the scraping session"""
    successful = [r for r in results if r['status'] == 'success']
    no_results = [r for r in results if r['status'] == 'no_results']
    errors = [r for r in results if r['status'] in ['error', 'timeout']]
    
    print("\n" + "="*60)
    print("SCRAPING SESSION SUMMARY")
    print("="*60)
    print(f"Court: {config['courtName']}")
    print(f"Case Type: {config['caseType'].upper()}")
    print(f"Starting Docket: {config['docketNumber']}")
    print(f"Year: {config['docketYear']}")
    print("-"*60)
    print(f"✅ Successful Cases: {len(successful)}")
    print(f"❌ No Results Found: {len(no_results)}")
    print(f"⚠️  Errors/Timeouts: {len(errors)}")
    print(f"📊 Total Attempts: {len(results)}")
    print("-"*60)
    
    if successful:
        print("\nSuccessful Cases:")
        for r in successful[:10]:  # Show first 10
            print(f"  • {r['case_number']}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more")
    
    print("="*60 + "\n")

# ----------------------------------------
# MAIN EXECUTION
# ----------------------------------------

async def scrape_single_config(config: dict):
    """
    Scrape cases for a single configuration
    """
    log.info("="*60)
    log.info("STARTING VIRGINIA COURT SCRAPER")
    log.info("="*60)
    log.info(f"Court: {config['courtName']}")
    log.info(f"FIPS Code: {config['courtFips']}")
    log.info(f"Case Type: {config['caseType'].upper()}")
    log.info(f"Starting Docket Number: {config['docketNumber']}")
    log.info(f"Year: {config['docketYear']}")
    log.info("="*60)
    
    # Initialize scraper
    scraper = VirginiaScraper(config=config)
    
    # Run the scraper
    results = await scraper.run_scraper()
    
    
    # Print summary
    print_summary(results, config)
    
    return results

async def scrape_multiple_configs(configs: list):
    """
    Scrape cases for multiple configurations sequentially
    """
    all_results = []
    
    for idx, config in enumerate(configs, 1):
        log.info(f"\n{'#'*60}")
        log.info(f"PROCESSING CONFIGURATION {idx}/{len(configs)}")
        log.info(f"{'#'*60}\n")
        
        results = await scrape_single_config(config)
        all_results.extend(results)
        
        # Delay between different configurations
        if idx < len(configs):
            log.info("Waiting 10 seconds before next configuration...")
            await asyncio.sleep(10)
    
    return all_results

async def main():
    """
    Main entry point for the scraper
    """
    # Ensure output directories exist
    ensure_directories()
    
    # Single configuration mode
    if isinstance(ACTIVE_CONFIG, dict):
        await scrape_single_config(ACTIVE_CONFIG)
    
    # Multiple configuration mode (uncomment to use)
    # configs = [CIVIL_CONFIG, CRIMINAL_CONFIG]
    # await scrape_multiple_configs(configs)
    
    # Group and merge JSON files after scraping
    log.info("\n" + "="*60)
    log.info("GROUPING AND MERGING JSON FILES")
    log.info("="*60)
    
    try:
        from utils.json_grouper import group_and_merge_json_files
        grouped_results = group_and_merge_json_files(JSON_DIR)
        
        log.info(f"✅ Successfully created {len(grouped_results)} grouped records")
        log.info(f"📁 Grouped files saved to: {os.path.join(JSON_DIR, 'grouped')}")
    except Exception as e:
        log.error(f"Error during grouping: {e}")
    
    log.info("="*60)
    
    # Multiple configuration mode (uncomment to use)
    # configs = [CIVIL_CONFIG, CRIMINAL_CONFIG]
    # await scrape_multiple_configs(configs)

# ----------------------------------------
# ENTRY POINT
# ----------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("\n\n⚠️  Scraping interrupted by user (Ctrl+C)")
        print("\nGracefully shutting down...")
    except Exception as e:
        log.error(f"\n\n🚨 Fatal error occurred: {e}")
        raise
    finally:
        log.info("\n" + "="*60)
        log.info("SCRAPER TERMINATED")
        log.info("="*60)