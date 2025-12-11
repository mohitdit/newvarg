from playwright.async_api import PlaywrightTimeoutError
from scrapers.base_scraper import BaseScraper
from utils.browser_manager import get_stealth_browser
from utils.logger import log
import asyncio

class WisconsinScraper(BaseScraper):
    """
    Scraper specific to the Wisconsin Circuit Court Access (WCCA) site.
    Handles the two-step process: consent click and then detail page navigation.
    """
    
    async def run_scraper(self):
        """
        Executes the scraping process: 
        1. Navigates to InitialURL and clicks 'I agree'.
        2. Navigates to the target Case Detail URL.
        3. Returns the case detail HTML.
        """
        # Set headless=False for demonstration, set to True for production
        is_headless = False
        browser, context, page = await get_stealth_browser(headless=is_headless)
        
        initial_url = self.config["InitialURL"]
        case_detail_url = self.build_case_url()
        docket = f"{self.config['docketYear']}{self.config['docketType']}{self.config['docketNumber']}"

        log.info(f"--- Starting Scrape for Docket: {docket} ---")
        log.info(f"1. Navigating to Initial URL (Consent Page): {initial_url}")

        html_content = ""
        try:
            # --- STEP 1: Navigate to the initial consent page ---
            await page.goto(initial_url, wait_until="domcontentloaded", timeout=30000)
            
            # --- STEP 2: Click 'I agree' to bypass the disclaimer ---
            I_AGREE_SELECTOR = "button:has-text('I agree'), text=I agree"
            
            try:
                log.info("2. Attempting to click 'I agree' button...")
                # Use page.click for robustness
                await page.click(I_AGREE_SELECTOR, timeout=10000)
                
                # Wait for the post-click state (could be a redirect or a simple visibility change)
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                log.info("✅ Consent bypassed successfully.")
                
            except PlaywrightTimeoutError:
                log.warning("❌ Could not find or click 'I agree' button. The session might already be established, continuing anyway.")


            # --- STEP 3: Navigate to the target Case Detail URL ---
            log.info(f"3. Navigating to Target Case Detail URL: {case_detail_url}")
            
            # Use the established context to navigate directly to the case
            await page.goto(case_detail_url, wait_until="domcontentloaded", timeout=30000)
            
            # --- STEP 4: Wait for the main content table and scrape HTML ---
            CASE_SUMMARY_SELECTOR = "table#caseSummary"
            log.info("Waiting for case summary table to confirm load...")
            await page.wait_for_selector(CASE_SUMMARY_SELECTOR, timeout=20000)
            
            html_content = await page.content()
            log.info(f"✅ Case detail HTML fetched successfully (Size: {len(html_content)} bytes).")

        except PlaywrightTimeoutError as e:
            log.error(f"🚨 Timeout error: The page took too long to load or the selector was not found. {e}")
        except Exception as e:
            log.error(f"🚨 An unexpected error occurred during scraping: {e}")
        finally:
            await context.close()
            await browser.close()
            log.info("--- Session Closed ---")

        return {
            "docket": docket,
            "url": case_detail_url,
            "html": html_content
        }