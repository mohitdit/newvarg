
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
    """

    def __init__(self, config: dict):
        """
        Normalize and store config. Ensure searchFipsCode / courtFips / county_no are 3-digit strings.
        """
        super().__init__(config)

        # Make a shallow copy to avoid mutating user's original dict
        self.config = dict(config)

        def pad_3_digits(val):
            try:
                return str(int(val)).zfill(3)
            except Exception:
                return str(val)

        if 'county_no' in self.config:
            padded = pad_3_digits(self.config['county_no'])
            self.config['county_no'] = padded
            self.config['searchFipsCode'] = padded
            self.config['courtFips'] = padded
        else:
            if 'searchFipsCode' in self.config:
                padded = pad_3_digits(self.config['searchFipsCode'])
                self.config['searchFipsCode'] = padded
                self.config['courtFips'] = self.config.get('courtFips', padded)
                self.config['county_no'] = self.config.get('county_no', padded)

        if 'docket_type' in self.config:
            dt = str(self.config['docket_type']).upper()
            if dt in ('GC', 'GT'):
                self.config['caseType'] = 'criminal'
                self.config['searchDivision'] = 'T'
            elif dt == 'GV':
                self.config['caseType'] = 'civil'
                self.config['searchDivision'] = self.config.get('searchDivision', 'V')

        self.config['caseType'] = self.config.get('caseType', 'civil')
        self.config['searchDivision'] = self.config.get('searchDivision', 'V')

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
                f"&searchDivision={self.config.get('searchDivision')}"
                f"&searchFipsCode={self.config.get('searchFipsCode')}"
                f"&curentFipsCode={self.config.get('searchFipsCode')}"
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
                'searchFipsCode': str(self.config.get('searchFipsCode')),
                'searchDivision': self.config.get('searchDivision'),
                'searchType': 'caseNumber',
                'displayCaseNumber': case_number,
                'localFipsCode': str(self.config.get('searchFipsCode'))
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
        """
        Run the scraper but STOP IMMEDIATELY if any hard failure/error occurs (error/timeout),
        or if saving/parsing fails for a successful scrape.
        Returns: (results, last_successful_number, error_occurred)
        """
        case_type = self.config.get('caseType', 'civil')
        prefixes = self.case_prefixes.get(case_type, ['GV'])
        start_number = int(self.config.get('docketNumber', 0))
        docket_year = str(self.config.get('docketYear'))[-2:]
        results = []
        current_prefix = prefixes[0]
        log.info(f"Starting scrape with prefix: {current_prefix}")
        current_number = start_number
        last_successful_number = start_number - 1  # Track last successful docket
        consecutive_failures = 0
        max_consecutive_failures = 1
        error_occurred = False

        while consecutive_failures < max_consecutive_failures:
            number_str = str(current_number).zfill(6)
            case_number = self.build_case_number(current_prefix, docket_year, number_str)
            result = await self.scrape_case(case_number, current_prefix)

            # HARD failures -> stop entire run and return immediately
            if result['status'] in ('error', 'timeout'):
                log.error(f"Hard failure for {case_number}. Stopping scraper and returning results so far.")
                results.append(result)
                error_occurred = True
                return results, last_successful_number, error_occurred

            if result['status'] == 'success':
                # Attempt to save HTML and parse to JSON. save_html returns json_path or None
                json_path = self.save_html(result['html'], case_number)
                if not json_path:
                    log.error(f"Failed to save/parse HTML for {case_number}. Stopping scraper.")
                    results.append({'status': 'save_parse_error', 'case_number': case_number, 'html': None})
                    error_occurred = True
                    return results, last_successful_number, error_occurred
                # success overall: include json_path in the result
                result['json_path'] = json_path
                results.append(result)
                consecutive_failures = 0
                last_successful_number = current_number  # Update last successful
                current_number += 1

            elif result['status'] == 'no_results':
                # For criminal cases, try alternate prefix once
                alternate_prefix = self.get_alternate_prefix(current_prefix)
                if alternate_prefix and case_type == 'criminal':
                    log.info(f" Trying alternate prefix: {alternate_prefix}")
                    alt_case_number = self.build_case_number(alternate_prefix, docket_year, number_str)
                    alt_result = await self.scrape_case(alt_case_number, alternate_prefix)

                    # if alternate produced hard failure -> stop
                    if alt_result['status'] in ('error', 'timeout'):
                        log.error(f"Hard failure for alternate {alt_case_number}. Stopping scraper.")
                        results.append(alt_result)
                        error_occurred = True
                        return results, last_successful_number, error_occurred

                    if alt_result['status'] == 'success':
                        alt_json_path = self.save_html(alt_result['html'], alt_case_number)
                        if not alt_json_path:
                            log.error(f"Failed to save/parse HTML for {alt_case_number}. Stopping scraper.")
                            results.append({'status': 'save_parse_error', 'case_number': alt_case_number, 'html': None})
                            error_occurred = True
                            return results, last_successful_number, error_occurred
                        alt_result['json_path'] = alt_json_path
                        log.info(f"✅ Found with alternate prefix! Switching from {current_prefix} to {alternate_prefix}")
                        current_prefix = alternate_prefix
                        results.append(alt_result)
                        consecutive_failures = 0
                        last_successful_number = current_number  # Update last successful
                        current_number += 1
                    else:
                        log.warning(f"❌ Both {case_number} and {alt_case_number} not found")
                        consecutive_failures += 1
                        log.warning(f"No results count: {consecutive_failures}/{max_consecutive_failures}")
                        current_number += 1
                else:
                    consecutive_failures += 1
                    log.warning(f"No results for {case_number}. No alternate available.")
                    log.warning(f"No results count: {consecutive_failures}/{max_consecutive_failures}")
                    current_number += 1
            else:
                log.error(f"Unexpected status '{result['status']}' for {case_number}. Stopping.")
                results.append(result)
                error_occurred = True
                return results, last_successful_number, error_occurred

            await asyncio.sleep(2)

        log.info(f"Completed scraping. Found {len(results)} cases total.")
        return results, last_successful_number, error_occurred
    def save_html(self, html_content: str, case_number: str) -> str:
        """
        Save HTML to data/htmldata folder, then parse it to JSON and save parsed JSON.

        Returns the json_path (string) on success, or None on failure.
        """
        try:
            html_dir = os.path.join(self.output_dir,"data", "htmldata")
            os.makedirs(html_dir, exist_ok=True)

            safe_court_name = self.config.get('courtName', 'unknown_court').replace(' ', '_')
            filename = f"{case_number}_{safe_court_name}.html"
            filepath = os.path.join(html_dir, filename)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)

            log.info(f"Saved HTML: {filepath}")

            # Parse HTML -> JSON
            parsed = parse_case_div(filepath)

            json_dir = os.path.join(self.output_dir,"data", "jsondata")
            os.makedirs(json_dir, exist_ok=True)

            metadata_court = self.config.get("courtName", "Radford General District Court")
            metadata_state = self.config.get("state", "VA")
            search_fips = self.config.get("searchFipsCode")
            try:
                metadata_search_fips = int(search_fips)
            except Exception:
                metadata_search_fips = search_fips

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
            return json_path
        except Exception as e:
            log.error(f"Saving/parsing failed for {case_number}: {e}")
            return None