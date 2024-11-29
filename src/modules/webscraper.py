import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.config import CONFIG
import logging
import time

logger = logging.getLogger(__name__)

class WebScraper:
    def process_chunks(self, chunks):
        logger.info("Starting web scraping...")
        with ThreadPoolExecutor(max_workers=CONFIG["scraper_threads"]) as executor:
            executor.map(self.process_chunk, chunks)
        logger.info("Web scraping complete.")

    def process_chunk(self, chunk_info):
        file_path = chunk_info["file_path"]
        chunk = chunk_info["data"]
        success_count, failure_count = 0, 0

        for index, sku in chunk.get("SKU", pd.Series()).dropna().items():
            if self.scrape_sku(sku):
                success_count += 1
            else:
                failure_count += 1

        logger.info(f"Chunk {file_path} processed - Success: {success_count}, Failures: {failure_count}")

    def scrape_sku(self, sku):
        url = f"https://egyptianlinens.com/search?view=ajax&q={sku}&options[prefix]=last&type=product"
        retries = CONFIG["max_retries"]
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    logger.debug(f"SKU: {sku} - SUCCESS")
                    return True
                else:
                    logger.warning(f"SKU: {sku} - Attempt {attempt + 1} failed with status {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"SKU: {sku} - Attempt {attempt + 1} failed with error: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
        logger.error(f"SKU: {sku} - All retries failed")
        return False
