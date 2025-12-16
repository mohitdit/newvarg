# utils/json_grouper.py
import os
import json
from typing import Dict, List, Any
from collections import defaultdict
from utils.logger import log

def load_all_json_files(json_dir: str) -> List[Dict[str, Any]]:
    """Load all JSON files from the directory."""
    all_cases = []
    
    if not os.path.exists(json_dir):
        log.warning(f"JSON directory does not exist: {json_dir}")
        return all_cases
    
    for filename in os.listdir(json_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(json_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_cases.append(data)
            except Exception as e:
                log.error(f"Error loading {filename}: {e}")
    
    return all_cases

def create_grouping_key(case_data: Dict[str, Any]) -> str:
    """
    Create a unique key for grouping based on:
    Case Number, Locality, Name, Address, Gender, DOB
    This ensures each unique case is only processed once
    """
    case_info = case_data.get("Case/Defendant Information", {})
    
    # Extract key fields - INCLUDE Case Number to prevent duplicates
    case_number = case_info.get("Case Number", case_info.get("Case Number ", "")).strip()
    locality = case_info.get("Locality", case_info.get("Locality ", "")).strip()
    name = case_info.get("Name", case_info.get("Name ", "")).strip()
    address = case_info.get("Address", case_info.get("Address ", "")).strip()
    gender = case_info.get("Gender", case_info.get("Gender ", "")).strip()
    dob = case_info.get("DOB", case_info.get("DOB ", "")).strip()
    
    # Create composite key WITH case number to avoid duplicates
    key = f"{case_number}|{locality}|{name}|{address}|{gender}|{dob}"
    return key

def merge_grouped_cases(cases: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple cases into a single grouped record.
    Combines charges, hearings, services, and dispositions into arrays.
    Each item in arrays includes 'case_number' to track which case it came from.
    """
    if not cases:
        return {}
    
    # Get docket information from first case and remove Case Number
    docket_info = cases[0].get("Case/Defendant Information", {}).copy()
    # Remove Case Number from docket_information (it's tracked in each charge/hearing/disposition)
    docket_info.pop("Case Number", None)
    docket_info.pop("Case Number ", None)  # Also remove with trailing space if exists
    
    # Start with the first case as base
    merged = {
        "state": cases[0].get("state", "VA"),
        "court_name": cases[0].get("court_name", cases[0].get("courtName", "")),
        "download_date": cases[0].get("download_date", ""),
        "county_no": cases[0].get("searchFipsCode", cases[0].get("county_no", "")),
        "docket_information": docket_info,
        "charges": [],
        "hearings": [],
        "dispositions": []
    }
    
    # Merge all cases
    for case in cases:
        case_number = case.get("Case/Defendant Information", {}).get("Case Number",
                      case.get("Case/Defendant Information", {}).get("Case Number ", "")).strip()
        
        # Add charge with case number reference
        charge_info = case.get("Charge Information", {})
        if charge_info and any(v for v in charge_info.values() if v):
            charge_with_case = {"case_number": case_number, **charge_info}
            merged["charges"].append(charge_with_case)
        
        # Add hearings with case number reference
        hearings = case.get("Hearing Information", [])
        for hearing in hearings:
            if hearing and any(v for v in hearing.values() if v):
                hearing_with_case = {"case_number": case_number, **hearing}
                merged["hearings"].append(hearing_with_case)
        
        # Add services with case number reference
        # services = case.get("Service/Process", [])
        # for service in services:
        #     if service:
        #         service_with_case = {"case_number": case_number, **service}
        #         merged["services"].append(service_with_case)
        
        # Add disposition with case number reference
        disposition_info = case.get("Disposition Information", {})
        if disposition_info:
            disposition_with_case = {"case_number": case_number, **disposition_info}
            merged["dispositions"].append(disposition_with_case)
    
    return merged

def group_and_merge_json_files(json_dir: str, output_dir: str = None) -> List[Dict[str, Any]]:
    """
    Main function to group and merge JSON files.
    
    Args:
        json_dir: Directory containing individual JSON files
        output_dir: Directory to save grouped JSON files (optional)
    
    Returns:
        List of grouped and merged case records
    """
    if output_dir is None:
        parent_dir = os.path.dirname(json_dir)
        output_dir = os.path.join(parent_dir,"data", "groupeddata")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Load all JSON files
    all_cases = load_all_json_files(json_dir)
    log.info(f"Loaded {len(all_cases)} JSON files")
    
    # Group cases by key
    grouped_cases = defaultdict(list)
    for case in all_cases:
        key = create_grouping_key(case)
        grouped_cases[key].append(case)
    
    log.info(f"Found {len(grouped_cases)} unique person/case combinations")
    
    # Merge grouped cases
    merged_results = []
    for idx, (key, cases) in enumerate(grouped_cases.items(), 1):
        merged = merge_grouped_cases(cases)
        merged_results.append(merged)
        
        # Save individual grouped file
        # Collect case numbers and county number from original cases
        case_numbers_list = []
        county_no = merged.get("county_no", "000")  # Get county number from merged data
        
        for case in cases:
            cn = case.get("Case/Defendant Information", {}).get("Case Number", 
                 case.get("Case/Defendant Information", {}).get("Case Number ", "")).strip()
            if cn and cn not in case_numbers_list:
                case_numbers_list.append(cn)
        
        # Get first case number and count of merged cases
        if case_numbers_list:
            first_case = case_numbers_list[0].replace('-', '_')
            merged_count = len(case_numbers_list)
            filename = f"{first_case}_{merged_count}_{county_no}.json"
        else:
            # Fallback
            filename = f"grouped_case_{idx}_{county_no}.json"
        
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved grouped file: {filename} (merged {len(cases)} records)")
    
    return merged_results