import asyncio
import os
import json
from datetime import datetime
from scrapers.virginia_scraper import VirginiaScraper
from utils.logger import log
from utils.json_grouper import group_and_merge_json_files
from api.api import ApiClient
import shutil
import time
from vpn.vpnbot import SurfsharkManager

# ----------------------------------------
# VPN MANAGEMENT GLOBALS
# ----------------------------------------
vpn_manager = None
last_vpn_reconnect_time = None

def initialize_vpn():
    """Initialize VPN manager and connect"""
    global vpn_manager, last_vpn_reconnect_time
    vpn_manager = SurfsharkManager()
    log.info("= Initializing VPN connection...")
    vpn_manager.reconnect()
    last_vpn_reconnect_time = time.time()
    log.info(f" VPN connected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def should_reconnect_vpn():
    """Check if VPN should reconnect based on time interval"""
    global last_vpn_reconnect_time
    
    if last_vpn_reconnect_time is None:
        return True
    
    interval_minutes = vpn_manager.get_reconnect_interval_minutes()
    elapsed_seconds = time.time() - last_vpn_reconnect_time
    elapsed_minutes = elapsed_seconds / 60
    
    if elapsed_minutes >= interval_minutes:
        log.info(f"� VPN reconnection needed: {elapsed_minutes:.1f} minutes elapsed (limit: {interval_minutes} minutes)")
        return True
    
    return False

def reconnect_vpn_if_needed():
    """Reconnect VPN and update timestamp"""
    global last_vpn_reconnect_time
    
    log.info("\n" + "="*60)
    log.info("= VPN RECONNECTION IN PROGRESS")
    log.info("="*60)
    log.info("�  All operations paused during VPN reconnection...")
    
    vpn_manager.reconnect()
    last_vpn_reconnect_time = time.time()
    
    log.info(f" VPN reconnected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("�  Operations resumed")
    log.info("="*60 + "\n")

# ----------------------------------------
# HARDCODED CONFIGURATIONS (example)  - for testing
# ----------------------------------------
# CIVIL_CONFIG = {
#     "courtFips": "177",
#     "courtName": "Virginia Beach General District Court",
#     "searchFipsCode": 177,
#     "searchDivision": "V",
#     "docketNumber": "9120",  # Starting number (becomes 0009120 -> 0009120)
#     "docketYear": 2025,
#     "caseType": "civil"  # Will use GV prefix
# }

# CRIMINAL_CONFIG = {
#     "courtFips": "001",
#     "courtName": "Accomack General District Court",
#     "searchFipsCode": "001",
#     "searchDivision": "T",
#     "docketNumber": "9792",  # Starting number
#     "docketYear": 2025,
#     "caseType": "criminal"  # Will use GC and GT prefixes
# }

# # Example API object (you're using this directly as ACTIVE_CONFIG in your snippet)
# API_EXAMPLE = {
#     "county_no": 1,
#     "county_name": "Accomack General District Court",
#     "docket_type": "GT",
#     "docket_year": 2025,
#     "docket_number": "010198",
# }

# # Choose which configuration to use
# # ACTIVE_CONFIG = CIVIL_CONFIG
# # ACTIVE_CONFIG = CRIMINAL_CONFIG
# ACTIVE_CONFIG = API_EXAMPLE  # for testing an API-style object

# Output directories
OUTPUT_DIR = None
HTML_DIR = None
JSON_DIR = None


# ----------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------
def ensure_directories(base_output_dir):
    """Create necessary directories if they don't exist"""
    global HTML_DIR, JSON_DIR
    HTML_DIR = os.path.join(base_output_dir, "data","htmldata")
    JSON_DIR = os.path.join(base_output_dir, "data","jsondata")
    # os.makedirs(HTML_DIR, exist_ok=True)
    # os.makedirs(JSON_DIR, exist_ok=True)
    log.info(f"Output directories ready: {HTML_DIR}, {JSON_DIR}")

def manage_processed_data():
    """
    Move files from groupeddata to processeddata before starting new job
    This prevents duplicate insertions from previous runs
    """
    import shutil
    
    grouped_dir = os.path.join(OUTPUT_DIR, "data","groupeddata")
    processed_dir = os.path.join(OUTPUT_DIR, "data","processeddata")
    
    # Create processeddata directory if it doesn't exist
    os.makedirs(processed_dir, exist_ok=True)
    
    # Check if groupeddata exists and has files
    if os.path.exists(grouped_dir):
        files = [f for f in os.listdir(grouped_dir) if f.endswith('.json')]
        
        if files:
            log.info(f"Found {len(files)} files in groupeddata folder")
            log.info("Moving files to processeddata folder...")
            
            # Move each file to processeddata
            for filename in files:
                src = os.path.join(grouped_dir, filename)
                dst = os.path.join(processed_dir, filename)
                
                # If file already exists in processeddata, add timestamp to avoid overwrite
                if os.path.exists(dst):
                    from datetime import datetime
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    name, ext = os.path.splitext(filename)
                    dst = os.path.join(processed_dir, f"{name}_{timestamp}{ext}")
                
                shutil.move(src, dst)
                log.info(f"  Moved: {filename}")
            
            log.info(f" Successfully moved {len(files)} files to processeddata")
        else:
            log.info("No files found in groupeddata folder")
    else:
        log.info("groupeddata folder does not exist yet")


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

    # Normalize county/fips fields - countyNo becomes 3-digit string
    if 'countyNo' in config:
        padded = pad_3_digits(config['countyNo'])
        config['countyNo'] = padded
        config['searchFipsCode'] = padded
        config['courtFips'] = padded
    
    # Map API fields to expected fields
    if 'countyName' in config and 'courtName' not in config:
        config['courtName'] = config['countyName']
    
    if 'stateAbbreviation' in config and 'state' not in config:
        config['state'] = config['stateAbbreviation']
    
    # Handle docket_type (GC/GT/GV) and set caseType and searchDivision
    if 'docketType' in config:
        dt = str(config['docketType']).upper()
        if dt in ('GC', 'GT'):
            config['caseType'] = 'criminal'
            # IMPORTANT: for criminal GC/GT we must use searchDivision "T"
            config['searchDivision'] = 'T'
        elif dt == 'GV':
            config['caseType'] = 'civil'
            # civil uses division 'V'
            config['searchDivision'] = 'V'
    
    # Convert docketNumber to integer and add 1 to start from next number
    if 'docketNumber' in config:
        try:
            config['docketNumber'] = int(config['docketNumber']) + 1
        except:
            pass
    
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
    print(f" Successful Cases: {len(successful)}")
    print(f"L No Results Found: {len(no_results)}")
    print(f"�  Errors/Timeouts/Save Errors: {len(errors)}")
    print(f"=� Total Attempts: {len(results)}")
    print("-"*60)
    
    if successful:
        print("\nSuccessful Cases:")
        for r in successful[:10]:  # Show first 10
            print(f"  {r['case_number']}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more")
    
    print("="*60 + "\n")


# ----------------------------------------
# API INTEGRATION
# ----------------------------------------
def fetch_job_from_api():
    """
    Fetch job details from the API endpoint
    Returns the courtOfficeDetails from the API response, or None if no jobs available
    """
    try:
        log.info("="*60)
        log.info("FETCHING JOB FROM API")
        log.info("="*60)
        
        api_client = ApiClient()
        response = api_client.post(f"VA_Downloader_Job_SQS_GET",{})
        
        log.info("API Response:")
        log.info(json.dumps(response, indent=2))
        log.info("="*60)
        
        # Check if response indicates no jobs available
        if response and 'message' in response:
            message = response['message']
            if isinstance(message, dict):
                code = message.get('code')
                if code == 202:  # No jobs available
                    log.info("No jobs available in queue (Code 202)")
                    return None
        
        # Check for successful job response
        if response and 'courtOfficeDetails' in response:
            return response['courtOfficeDetails']
        else:
            log.error("No courtOfficeDetails found in API response")
            return None
            
    except Exception as e:
        log.error(f"Error fetching job from API: {e}")
        return None


# ----------------------------------------
# MAIN EXECUTION
# ----------------------------------------
# async def scrape_single_config(config: dict):
#     """
#     Scrape cases for a single configuration
#     """
#     # Normalize config (handle API-style object conversions)
#     config = normalize_config_from_api(config)

#     log.info("="*60)
#     log.info("STARTING VIRGINIA COURT SCRAPER")
#     log.info("="*60)
#     log.info(f"Court: {config.get('courtName')}")
#     log.info(f"FIPS Code: {config.get('courtFips') or config.get('searchFipsCode')}")
#     log.info(f"Case Type: {config.get('caseType','').upper()}")
#     log.info(f"Starting Docket Number: {config.get('docketNumber')}")
#     log.info(f"Year: {config.get('docketYear')}")
#     log.info(f"Search Division: {config.get('searchDivision')}")
#     log.info(f"Docket Type: {config.get('docketType')}")
#     log.info("="*60)
    
#     # Initialize scraper
#     scraper = VirginiaScraper(config=config)
    
#     # Run the scraper
#     results = await scraper.run_scraper()
    
#     # Print summary
#     print_summary(results, config)
    
#     return results


async def main():
    """
    Main entry point for the scraper
    Continuously fetches and processes jobs until queue is empty
    """
    # Initialize VPN once at startup
    initialize_vpn()

    while True:
        # Fetch job from API
        job_config = fetch_job_from_api()
        
        if not job_config:
            log.info("="*60)
            log.info("NO MORE JOBS IN QUEUE - SCRAPER SHUTTING DOWN")
            log.info("="*60)
            break
        
        # Store original job config for API calls
        original_config = dict(job_config)
        api_client = ApiClient()
        
        # Normalize config for scraping
        config = normalize_config_from_api(job_config)
        
        # Initialize scraper to get the proper output_dir structure
        scraper = VirginiaScraper(config=config)
        
        # Set global output directories based on scraper's output_dir
        global OUTPUT_DIR
        OUTPUT_DIR = scraper.output_dir
        ensure_directories(OUTPUT_DIR)
        
        # Move any existing grouped files to processed folder
        log.info("="*60)
        log.info("CHECKING FOR PREVIOUS RUN DATA")
        log.info("="*60)
        manage_processed_data()
        log.info("="*60)
        
        
        log.info("="*60)
        log.info("STARTING VIRGINIA COURT SCRAPER")
        log.info("="*60)
        log.info(f"Court: {config.get('courtName')}")
        log.info(f"FIPS Code: {config.get('courtFips') or config.get('searchFipsCode')}")
        log.info(f"Case Type: {config.get('caseType','').upper()}")
        log.info(f"Starting Docket Number: {config.get('docketNumber')}")
        log.info(f"Year: {config.get('docketYear')}")
        log.info(f"Search Division: {config.get('searchDivision')}")
        log.info(f"Docket Type: {config.get('docketType')}")
        log.info("="*60)
        
        # Initialize scraper
        scraper = VirginiaScraper(config=config)
        
        # Run the scraper
        results, last_successful_number, error_occurred = await scraper.run_scraper()
        
        # Print summary
        print_summary(results, config)
        
        # Group and merge JSON files after scraping
        log.info("\n" + "="*60)
        log.info("GROUPING AND MERGING JSON FILES")
        log.info("="*60)
        
        grouped_results = []
        try:
            grouped_results = group_and_merge_json_files(JSON_DIR)
            log.info(f" Successfully created {len(grouped_results)} grouped records")
            log.info(f"=� Grouped files saved to: {os.path.join(OUTPUT_DIR, 'groupeddata')}")
        except Exception as e:
            log.error(f"Error during grouping: {e}")
        
        
        # Handle error recovery or completion
        if error_occurred:
            log.info("\n" + "="*60)
            log.info("ERROR RECOVERY - ADDING JOB BACK TO QUEUE")
            log.info("="*60)
            
            # Add job back to queue with last successful docket number
            recovery_job = {
                "stateName": original_config.get("stateName", "VIRGINIA"),
                "stateAbbreviation": original_config.get("stateAbbreviation", "VA"),
                "countyNo": original_config.get("countyNo"),
                "countyName": original_config.get("countyName"),
                "docketNumber": str(last_successful_number).zfill(6),
                "docketYear": original_config.get("docketYear"),
                "docketType": original_config.get("docketType")
            }
            
            try:
                add_response = api_client.add_job_to_queue(recovery_job)
                log.info(f" Add Job API Response: {add_response}")
                log.info(f"=� Job added back to queue starting at docket: {recovery_job['docketNumber']}")
            except Exception as e:
                log.error(f"L Error adding job back to queue: {e}")
        
        else:
            # Job completed successfully - update docket number
            log.info("\n" + "="*60)
            log.info("JOB COMPLETED SUCCESSFULLY - UPDATING DOCKET NUMBER")
            log.info("="*60)
            
            try:
                update_response = api_client.update_docket_number(
                    state_name=original_config.get("stateName", "VIRGINIA"),
                    county_no=original_config.get("countyNo"),
                    county_name=original_config.get("countyName"),
                    docket_number=last_successful_number,
                    docket_year=original_config.get("docketYear"),
                    docket_type=original_config.get("docketType")
                )
                log.info(f" Update API Response: {update_response}")
                log.info(f" Updated last successful docket to: {str(last_successful_number).zfill(6)}")
            except Exception as e:
                log.error(f"L Error updating docket number: {e}")
        
        # Insert grouped records into MongoDB
        if grouped_results:
            log.info("\n" + "="*60)
            log.info("INSERTING RECORDS INTO DATABASE")
            log.info("="*60)
            
            try:
                insert_response = api_client.insert_records(grouped_results)
                log.info(f" Insert API Response: {insert_response}")
                inserted_count = insert_response.get('body', {}).get('insertedCount', 0)
                log.info(f" Inserted {inserted_count} records (from {len(grouped_results)} grouped records)")
            except Exception as e:
                log.error(f"L Error inserting records: {e}")
                error_occurred = True
        
        log.info("="*60)
        log.info("JOB COMPLETED - PREPARING FOR NEXT JOB")
        log.info("="*60)
        
        # Determine if VPN reconnection is needed
        needs_vpn_reconnect = False
        
        if error_occurred:
            # Case 1: Error occurred, we made add_job_to_queue API call
            log.info("=4 Error occurred in job - VPN reconnection required")
            needs_vpn_reconnect = True
        elif should_reconnect_vpn():
            # Case 2: Time limit reached (successful jobs running continuously)
            needs_vpn_reconnect = True
        
        # Reconnect VPN if needed (this pauses everything)
        if needs_vpn_reconnect:
            reconnect_vpn_if_needed()
        else:
            elapsed = (time.time() - last_vpn_reconnect_time) / 60
            log.info(f"9  VPN reconnection not needed (elapsed: {elapsed:.1f} minutes)")
        
        log.info("= Fetching next job from queue...")
        
        # Small delay before next job
        await asyncio.sleep(2)

# ----------------------------------------
# ENTRY POINT
# ----------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("\n\n�  Scraping interrupted by user (Ctrl+C)")
        print("\nGracefully shutting down...")
    except Exception as e:
        log.error(f"\n\n=� Fatal error occurred: {e}")
        raise
    finally:
        log.info("\n" + "="*60)
        log.info("SCRAPER TERMINATED")
        log.info("="*60)