# base_scraper.py

import os
from abc import ABC, abstractmethod

class BaseScraper(ABC):
    """
    Base class for all scrapers.
    """

    def __init__(self, config: dict):
        self.config = config
        self.output_dir = "data"
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
