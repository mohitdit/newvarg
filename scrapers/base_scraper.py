# base_scraper.py

import os
from abc import ABC, abstractmethod

class BaseScraper(ABC):
    """
    Base class for all scrapers.
    """

    def __init__(self, config: dict):
        from datetime import datetime
        
        self.config = config
        
        # Build hierarchical path: Date/State/County/CaseType/data
        current_date = datetime.now().strftime("%Y-%m-%d")
        state = self.config.get("state", self.config.get("stateAbbreviation", "VA"))
        county_name = self.config.get("courtName", self.config.get("countyName", "Unknown_County"))
        
        # Sanitize county name for filesystem
        safe_county_name = county_name.replace(' ', '_').replace('/', '_')
        
        # Determine case type folder name
        case_type = self.config.get("caseType", "civil")
        docket_type = self.config.get("docketType", "")
        
        if case_type == "criminal" and docket_type:
            # For criminal cases, use the actual docket type (GC or GT)
            case_type_folder = docket_type.upper()
        elif case_type == "civil":
            case_type_folder = "GV"
        else:
            case_type_folder = case_type.upper()
        
        # Build the full path: Date/State/County/CaseType/data
        self.output_dir = os.path.join(
            current_date,
            state,
            safe_county_name,
            case_type_folder,
        )
        
        os.makedirs(self.output_dir, exist_ok=True)

    def build_case_url(self) -> str:
        """
        Build full case URL using caseNo + countyNo.
        """
        case_no = f"{self.config['docketYear']}{self.config['docketType']}{self.config['docketNumber']}"

        case_url = self.config["urlFormat"].format(
            caseNo=case_no,
            CountyID=self.config["countyNo"]
        )

        return case_url

    @abstractmethod
    async def run_scraper(self):
        pass