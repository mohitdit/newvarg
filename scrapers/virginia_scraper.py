import os
import asyncio
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from scrapers.base_scraper import BaseScraper
from utils.browser_manager import get_stealth_browser
from utils.logger import log

# Parser functions
from scrapers.virginia_html_to_json import parse_case_div, save_parsed_json


class VirginiaScraper(BaseScraper):
    """
    Scraper for Virginia General District Courts
    Handles both Civil (GV) and Criminal (GC, GT) cases
    Case format: XX000000-00 (2 letters + 6 digits + dash + 2 digits)
    
    For criminal cases: If GC prefix fails, try GT with same number (and vice versa)
    """
    
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = "https://eapps.courts.state.va.us/gdcourts/criminalCivilCaseSearch.do"
        self.case_prefixes = {
            'civil': ['GV'],
            'criminal': ['GC', 'GT']
        }
    
    def build_case_number(self, prefix: str, year: str, number: str, suffix: str = "00") -> str:
        number_padded = str(number).zfill(6)
        return f"{prefix}{year}{number_padded}-{suffix}"
    
    async def check_no_results(self, page) -> bool:
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
            content = await page.content()
            return "No results found for the search criteria." in content or "No results found" in content
        except Exception as e:
            log.warning(f"Could not verify no results status: {e}")
            return False
    
    async def scrape_case(self, case_number: str, prefix: str) -> dict:
        browser, context, page = await get_stealth_browser(headless=True)
        try:
            log.info(f"Scraping case: {case_number}")
            referer_url = (
                f"{self.base_url}?fromSidebar=true&formAction=searchLanding"
                f"&searchDivision={self.config['searchDivision']}"
                f"&searchFipsCode={self.config['searchFipsCode']}"
                f"&curentFipsCode={self.config['searchFipsCode']}"
            )
            await page.goto(referer_url, wait_until="domcontentloaded", timeout=30000)
            await page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'max-age=0',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://eapps.courts.state.va.us',
                'Referer': referer_url,
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1'
            })
            form_data = {
                'formAction': 'submitCase',
                'searchFipsCode': str(self.config['searchFipsCode']),
                'searchDivision': self.config['searchDivision'],
                'searchType': 'caseNumber',
                'displayCaseNumber': case_number,
                'localFipsCode': str(self.config['searchFipsCode'])
            }
            await page.evaluate(f"""
                () => {{
                    const form = document.createElement('form');
                    form.method = 'POST';
                    form.action = '{self.base_url}';
                    
                    const fields = {form_data};
                    for (const [key, value] of Object.entries(fields)) {{
                        const input = document.createElement('input');
                        input.type = 'hidden';
                        input.name = key;
                        input.value = value;
                        form.appendChild(input);
                    }}
                    
                    document.body.appendChild(form);
                    form.submit();
                }}
            """)
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
            await asyncio.sleep(1)
            no_results = await self.check_no_results(page)
            if no_results:
                log.info(f"No results found for {case_number}")
                return {'status': 'no_results', 'case_number': case_number, 'html': None}
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except:
                pass
            html_content = await page.content()
            log.info(f"✅ Successfully scraped {case_number}")
            return {'status': 'success', 'case_number': case_number, 'html': html_content}
        except PlaywrightTimeoutError as e:
            log.error(f"Timeout error for {case_number}: {e}")
            return {'status': 'timeout', 'case_number': case_number, 'html': None}
        except Exception as e:
            log.error(f"Error scraping {case_number}: {e}")
            return {'status': 'error', 'case_number': case_number, 'html': None}
        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass
    
    def get_alternate_prefix(self, current_prefix: str) -> str:
        if current_prefix == 'GC':
            return 'GT'
        elif current_prefix == 'GT':
            return 'GC'
        else:
            return None
    
    async def run_scraper(self):
        case_type = self.config.get('caseType', 'civil')
        prefixes = self.case_prefixes.get(case_type, ['GV'])
        start_number = int(self.config['docketNumber'])
        docket_year = str(self.config['docketYear'])[-2:]
        results = []
        current_prefix = prefixes[0]
        log.info(f"Starting scrape with prefix: {current_prefix}")
        current_number = start_number
        consecutive_failures = 0
        max_consecutive_failures = 1
        while consecutive_failures < max_consecutive_failures:
            number_str = str(current_number).zfill(6)
            case_number = self.build_case_number(current_prefix, docket_year, number_str)
            result = await self.scrape_case(case_number, current_prefix)
            if result['status'] == 'success':
                results.append(result)
                consecutive_failures = 0
                self.save_html(result['html'], case_number)
                current_number += 1
            elif result['status'] == 'no_results':
                alternate_prefix = self.get_alternate_prefix(current_prefix)
                if alternate_prefix and case_type == 'criminal':
                    log.info(f"🔄 Trying alternate prefix: {alternate_prefix}")
                    alt_case_number = self.build_case_number(alternate_prefix, docket_year, number_str)
                    alt_result = await self.scrape_case(alt_case_number, alternate_prefix)
                    if alt_result['status'] == 'success':
                        log.info(f"✅ Found with alternate prefix! Switching from {current_prefix} to {alternate_prefix}")
                        current_prefix = alternate_prefix
                        results.append(alt_result)
                        consecutive_failures = 0
                        self.save_html(alt_result['html'], alt_case_number)
                        current_number += 1
                    else:
                        log.warning(f"❌ Both {result['case_number']} and {alt_case_number} not found")
                        consecutive_failures += 1
                        log.warning(f"No results count: {consecutive_failures}/{max_consecutive_failures}")
                        current_number += 1
                else:
                    consecutive_failures += 1
                    log.warning(f"No results count: {consecutive_failures}/{max_consecutive_failures}")
                    current_number += 1
            else:
                log.warning(f"Failed to scrape {case_number}, status: {result['status']}")
                current_number += 1
            await asyncio.sleep(2)
        log.info(f"Completed scraping. Found {len(results)} cases total.")
        return results
    
    def save_html(self, html_content: str, case_number: str):
        """Save HTML to data/htmldata folder, then parse it to JSON and save parsed JSON."""
        html_dir = os.path.join(self.output_dir, "htmldata")
        os.makedirs(html_dir, exist_ok=True)

        safe_court_name = self.config.get('courtName', 'unknown_court').replace(' ', '_')
        filename = f"{case_number}_{safe_court_name}.html"
        filepath = os.path.join(html_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)

        log.info(f"Saved HTML: {filepath}")

        # -------------------------
        # Parse HTML -> JSON
        # -------------------------
        try:
            parsed = parse_case_div(filepath)
        except Exception as e:
            parsed = {}
            log.error(f"Parsing HTML failed for {case_number}: {e}")

        try:
            json_dir = os.path.join(self.output_dir, "jsondata")
            os.makedirs(json_dir, exist_ok=True)

            # Prepare metadata that'll be injected at top of JSON
            metadata_court = self.config.get("courtName", "Radford General District Court")
            metadata_state = self.config.get("state", "VA")
            metadata_search_fips = int(self.config.get("searchFipsCode", 0))

            # Always save with Case/Defendant Information first (case_first=True)
            json_path = save_parsed_json(
                case_number=case_number,
                parsed=parsed,
                output_dir=json_dir,
                court_name=metadata_court,
                state=metadata_state,
                search_fips=metadata_search_fips,
                case_first=True
            )

            log.info(f"Saved parsed JSON: {json_path}")
        except Exception as e:
            log.error(f"Saving parsed JSON failed for {case_number}: {e}")
            json_path = None

        return filepath