import asyncio
import json
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

COOKIE_FILE = "wcca_cookies.json"
WCCA_URL = "https://wcca.wicourts.gov"


async def save_wcca_cookies():
    print("Launching browser...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # Apply stealth correctly
        stealth = Stealth()
        await stealth.apply_stealth_async(page)

        print(f"Opening WCCA: {WCCA_URL}")
        await page.goto(WCCA_URL, wait_until="domcontentloaded")

        print("")
        print("👉 Solve CAPTCHA manually in the opened browser window.")
        print("👉 Click the 'I Agree' button.")
        print("👉 Wait until the main search page loads.")
        input("Press ENTER here once the main WCCA page is loaded...")

        print("Saving cookies...")

        cookies = await context.cookies()

        with open(COOKIE_FILE, "w", encoding="utf-8") as f:
            json.dump(cookies, f, indent=2)

        print(f"✅ Cookies saved to {COOKIE_FILE}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(save_wcca_cookies())
