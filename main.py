import asyncio
import os
import json
from datetime import datetime
from scrapers.virginia_scraper import VirginiaScraper
from utils.logger import log
from utils.json_grouper import group_and_merge_json_files

# ----------------------------------------
# HARDCODED CONFIGURATIONS (examples)
# ----------------------------------------
CIVIL_CONFIG = {
    "courtFips": "177",
    "courtName": "Virginia Beach General District Court",
    "searchFipsCode": 177,
    "searchDivision": "V",
    "docketNumber": "9120",  # Starting number (becomes 0009120 -> 0009120)
    "docketYear": 2025,
    "caseType": "civil"  # Will use GV prefix
}

CRIMINAL_CONFIG = {
    "courtFips": "001",
    "courtName": "Accomack General District Court",
    "searchFipsCode": "001",
    "searchDivision": "T",
    "docketNumber": "9792",  # Starting number
    "docketYear": 2025,
    "caseType": "criminal"  # Will use GC and GT prefixes
}

# Example API object (you're using this directly as ACTIVE_CONFIG in your snippet)
API_EXAMPLE = {
    "county_no": 1,
    "county_name": "Accomack General District Court",
    "docket_type": "GT",
    "docket_year": 2025,
    "docket_number": "010198",
}

# Choose which configuration to use
# ACTIVE_CONFIG = CIVIL_CONFIG
# ACTIVE_CONFIG = CRIMINAL_CONFIG
ACTIVE_CONFIG = API_EXAMPLE  # for testing an API-style object

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


def pad_3_digits(value):
    """Pad an int or numeric string to 3-digit zero-padded string."""
    try:
        n = int(value)
    except Exception:
        try:
            n = int(str(value).strip())
        except Exception:
            return str(value)
    return str(n).zfill(3)


def normalize_config_from_api(obj: dict) -> dict:
    """
    Normalize an API-provided object into the config shape your scraper expects.
    Ensures county_no, searchFipsCode and courtFips are the same 3-digit string.
    Forces searchDivision 'T' for GC/GT (criminal docket types).
    """
    config = dict(obj)  # shallow copy

    # Normalize county/fips fields
    if 'county_no' in config:
        padded = pad_3_digits(config['county_no'])
        config['county_no'] = padded
        config['searchFipsCode'] = padded
        config['courtFips'] = padded
    else:
        if 'searchFipsCode' in config:
            config['searchFipsCode'] = pad_3_digits(config['searchFipsCode'])
            config['courtFips'] = config.get('courtFips', config['searchFipsCode'])
            config['courtFips'] = pad_3_digits(config['courtFips'])
            config['searchFipsCode'] = config['courtFips']

    # Map docket fields to expected keys
    if 'docket_number' in config and 'docketNumber' not in config:
        config['docketNumber'] = config['docket_number']
    if 'docket_year' in config and 'docketYear' not in config:
        config['docketYear'] = config['docket_year']
    if 'docket_number' not in config and 'docketNumber' not in config and 'docket_number' in config:
        config['docketNumber'] = config['docket_number']

    if 'county_name' in config and 'courtName' not in config:
        config['courtName'] = config['county_name']

    # If API gives docket_type (GC/GT/GV) force caseType and searchDivision accordingly
    if 'docket_type' in config:
        dt = str(config['docket_type']).upper()
        if dt in ('GC', 'GT'):
            config['caseType'] = 'criminal'
            # IMPORTANT: for criminal GC/GT we must use searchDivision "T"
            config['searchDivision'] = 'T'
        elif dt == 'GV':
            config['caseType'] = 'civil'
            # civil default division might be 'V' but don't override if provided
            config['searchDivision'] = config.get('searchDivision', 'V')

    # Final defaults if not set
    config['caseType'] = config.get('caseType', 'civil')
    config['searchDivision'] = config.get('searchDivision', 'V')

    return config


def print_summary(results: list, config: dict):
    """Print a formatted summary of the scraping session"""
    successful = [r for r in results if r['status'] == 'success']
    no_results = [r for r in results if r['status'] == 'no_results']
    errors = [r for r in results if r['status'] in ['error', 'timeout', 'save_parse_error']]
    
    print("\n" + "="*60)
    print("SCRAPING SESSION SUMMARY")
    print("="*60)
    print(f"Court: {config.get('courtName')}")
    print(f"Case Type: {config.get('caseType','').upper()}")
    print(f"Starting Docket: {config.get('docketNumber')}")
    print(f"Year: {config.get('docketYear')}")
    print("-"*60)
    print(f"✅ Successful Cases: {len(successful)}")
    print(f"❌ No Results Found: {len(no_results)}")
    print(f"⚠️  Errors/Timeouts/Save Errors: {len(errors)}")
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
    # Normalize config (handle API-style object conversions)
    config = normalize_config_from_api(config)

    log.info("="*60)
    log.info("STARTING VIRGINIA COURT SCRAPER")
    log.info("="*60)
    log.info(f"Court: {config.get('courtName')}")
    log.info(f"FIPS Code: {config.get('courtFips') or config.get('searchFipsCode')}")
    log.info(f"Case Type: {config.get('caseType','').upper()}")
    log.info(f"Starting Docket Number: {config.get('docketNumber')}")
    log.info(f"Year: {config.get('docketYear')}")
    log.info(f"Search Division: {config.get('searchDivision')}")
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
    
    # Example: If you receive an array from API you could use:
    # api_configs = [API_EXAMPLE, ...]
    # normalized = [normalize_config_from_api(c) for c in api_configs]
    # await scrape_multiple_configs(normalized)
    
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