import requests
import logging
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

def get_stealth_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[403, 429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/"
    })
    return session

def fetch_job_description(url, session):
    try:
        response = session.get(url, timeout=15, allow_redirects=True)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        section = soup.find('section', class_='adp-body') or \
                  soup.find('div', class_='job-description') or \
                  soup.find('div', class_='description') or \
                  soup.find('article', id='jobDescription') or \
                  soup.find('div', attrs={'data-testid': 'viewJobBodyContainer'}) or \
                  soup.find('div', class_='sc-666b1ca0-11')
        
        return section.get_text(separator="\n", strip=True) if section else None
    except Exception as e:
        logger.warning(f"Could not scrape {url}: {e}")
        return None

def fetch_job_description_playwright(url):
    with sync_playwright() as p:
        # Launch a real browser (headless)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        )
        page = context.new_page()
        
        try:
            # Wait for the page to load and redirects to finish
            page.goto(url, wait_until="networkidle", timeout=20000)
            
            # Extract content using our known selectors
            selectors = (
                "section.adp-body, "
                "div.job-description, "
                "div.description, "
                "article#jobDescription, "
                "div[data-testid='viewJobBodyContainer'], "
                "div.sc-666b1ca0-11"
            )
            
            content = page.locator(selectors).first
            text = content.inner_text().strip() if content.count() > 0 else None
            return text
        except Exception as e:
            logger.warning(f"Playwright failed to scrape {url}: {e}")
            return None
        finally:
            browser.close()