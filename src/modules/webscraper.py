import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from src.config import CONFIG

logger = logging.getLogger(__name__)

class WebScraper:
    def process_chunks(self, chunks):
        logger.info("Starting web scraping...")
        with ThreadPoolExecutor(max_workers=CONFIG["scraper_threads"]) as executor:
            executor.map(self.log_connection_attempts, chunks)
        logger.info("Web scraping complete.")

    def log_connection_attempts(self, chunk_info):
        file_path = chunk_info["file_path"]
        chunk = chunk_info["data"]

        for index, sku in chunk.get("SKU", pd.Series()).dropna().items():
            url = CONFIG["scraper_base_url"].format(sku=sku)
            self.log_connection(url, sku)

        logger.debug(f"Chunk {file_path} processed. Connections logged.")

    def log_connection(self, url, sku):
        retries = CONFIG["max_retries"]
        delay = CONFIG["rate_limit_delay"]
        headers = CONFIG["custom_headers"]
        timeout = CONFIG["timeout"]

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=timeout)
                if response.status_code == 200:
                    logger.debug(f"Successfully connected to URL: {url} for SKU: {sku}")
                    return True
                else:
                    logger.warning(f"SKU: {sku} - Attempt {attempt + 1} failed with status {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"SKU: {sku} - Attempt {attempt + 1} failed with error: {e}")

            time.sleep(delay)  # Delay between retries
        logger.error(f"SKU: {sku} - All retries failed for URL: {url}")
        return False
