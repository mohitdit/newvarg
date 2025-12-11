import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def get_stealth_browser(headless: bool = True):
    """
    Launches a Chromium browser instance with Playwright Stealth applied.
    
    Args:
        headless (bool): Whether to run the browser in headless mode.
    
    Returns:
        tuple: (browser, context, page)
    """
    try:
        playwright = await async_playwright().start()
    except Exception as e:
        # Handle case where playwright is already started/installed improperly
        print(f"Error starting Playwright: {e}")
        raise

    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
        ]
    )
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
    page = await context.new_page()

    # Apply Playwright Stealth to evade bot detection
    stealth = Stealth()
    await stealth.apply_stealth_async(page)

    return browser, context, page

async def wait_for_user_confirmation(prompt: str = "Press Enter to continue..."):
    """
    Pauses execution and waits for the user to press Enter.
    Useful for manual CAPTCHA solving or interaction.
    """
    print(prompt)
    await asyncio.get_event_loop().run_in_executor(None, input)