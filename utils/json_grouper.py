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
    Case Number, Filed Date, Locality, Name, Address, Gender, DOB
    """
    case_info = case_data.get("Case/Defendant Information", {})
    
    # Extract key fields
    case_number = case_info.get("Case Number ", "").strip()
    filed_date = case_info.get("Filed Date ", "").strip()
    locality = case_info.get("Locality ", "").strip()
    name = case_info.get("Name ", "").strip()
    address = case_info.get("Address ", "").strip()
    gender = case_info.get("Gender ", "").strip()
    dob = case_info.get("DOB ", "").strip()
    
    # Create composite key
    key = f"{case_number}|{filed_date}|{locality}|{name}|{address}|{gender}|{dob}"
    return key

def merge_grouped_cases(cases: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple cases into a single grouped record.
    Combines charges, hearings, services, and dispositions into arrays.
    """
    if not cases:
        return {}
    
    # Start with the first case as base
    merged = {
        "state": cases[0].get("state", "VA"),
        "court_name": cases[0].get("court_name", ""),
        "download_date": cases[0].get("download_date", ""),
        "docket_information": cases[0].get("Case/Defendant Information", {}),
        "charges": [],
        "hearings": [],
        "services": [],
        "dispositions": []
    }
    
    # Merge all cases
    for case in cases:
        case_number = case.get("Case/Defendant Information", {}).get("Case Number ", "").strip()
        
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
        services = case.get("Service/Process", [])
        for service in services:
            if service and any(v for v in service.values() if v):
                service_with_case = {"case_number": case_number, **service}
                merged["services"].append(service_with_case)
        
        # Add disposition with case number reference
        disposition_info = case.get("Disposition Information", {})
        if disposition_info and any(v for v in disposition_info.values() if v):
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
        output_dir = os.path.join(json_dir, "grouped")
    
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
        if merged.get("docket_information", {}).get("Name ", ""):
            name = merged["docket_information"]["Name "].replace(" ", "_").replace(",", "")
            case_num = merged["docket_information"].get("Case Number ", "UNKNOWN").replace("-", "_")
            filename = f"grouped_{name}_{case_num}.json"
        else:
            filename = f"grouped_case_{idx}.json"
        
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)
        
        log.info(f"Saved grouped file: {filename} (merged {len(cases)} records)")
    
    return merged_results