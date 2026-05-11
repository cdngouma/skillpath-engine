from contextlib import AbstractContextManager
from playwright.sync_api import sync_playwright
import logging

logger = logging.getLogger(__name__)

SELECTORS = (
    "section.adp-body, "
    "div.job-description, "
    "div.description, "
    "article#jobDescription, "
    "div[data-testid='viewJobBodyContainer'], "
    "div.sc-666b1ca0-11"
)

class PlaywrightScraper(AbstractContextManager):
    def __enter__(self):
        self._p = sync_playwright().start()
        self._browser = self._p.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        )
        self._page = self._context.new_page()
        return self

    def fetch(self, url):
        try:
            self._page.goto(url, wait_until="networkidle", timeout=20000)
            content = self._page.locator(SELECTORS).first
            return content.inner_html().strip() if content.count() > 0 else None
        except Exception:
            logger.warning(f"Playwright failed to scrape {url}")
            return None

    def __exit__(self, exc_type, exc, tb):
        self._browser.close()
        self._p.stop()