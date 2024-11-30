import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import time
from bs4 import BeautifulSoup
from src.config import CONFIG

logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(self, file_manager):
        self.file_manager = file_manager

    def scrape_product_details(self, html_content):
        """
        Extract product details from the HTML content using BeautifulSoup.
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract Product Name
        product_name_tag = soup.find('h1', class_='product-meta__title')
        product_name = product_name_tag.get_text(strip=True) if product_name_tag else 'N/A'

        # Extract SKU
        sku_tag = soup.find('span', class_='product-meta__sku-number')
        sku = sku_tag.get_text(strip=True) if sku_tag else 'N/A'

        # Extract Color Options
        color_options = []
        color_tags = soup.select('.color-swatch__radio')
        for tag in color_tags:
            color_options.append(tag['value'])

        # Extract Size Options
        size_options = []
        size_tags = soup.select('.block-swatch__radio')
        for tag in size_tags:
            size_options.append(tag['value'])

        # Extract Image URLs
        images = []
        image_tags = soup.select('.product-gallery__image')
        for img_tag in image_tags:
            img_src = img_tag.get('data-src', img_tag.get('src'))
            if img_src:
                images.append(img_src)

        return {
            'product_name': product_name,
            'sku': sku,
            'colors': color_options,
            'sizes': size_options,
            'images': images
        }

    def scrape_and_update_row(self, sku, chunk_data, index):
        """
        Scrapes the product details for the given SKU and updates the row in the chunk_data.
        """
        base_url = CONFIG["scraper_base_url"].format(sku=sku)
        headers = CONFIG["custom_headers"]
        response = self.make_request(base_url, headers)

        if not response:
            logger.warning(f"Skipping SKU {sku} due to request failure.")
            return

        product_details = self.scrape_product_details(response.text)

        # Update the DataFrame with scraped details
        chunk_data.at[index, "PRODUCT TITLE"] = product_details['product_name']
        chunk_data.at[index, "SKU"] = product_details['sku']
        chunk_data.at[index, "COLOR"] = ', '.join(product_details['colors'])
        chunk_data.at[index, "SIZE"] = ', '.join(product_details['sizes'])

        # Assign image URLs
        for i, col_index in enumerate(["IMAGE URL 1", "IMAGE URL 2", "IMAGE URL 3"], start=1):
            chunk_data.at[index, col_index] = product_details['images'][i - 1] if len(product_details['images']) >= i else 'N/A'

        logger.info(f"Updated SKU {sku} with product details.")

    def make_request(self, url, headers):
        """
        Makes a request to the given URL with retries and delay logic.
        """
        retries = CONFIG["max_retries"]
        delay = CONFIG["rate_limit_delay"]
        timeout = CONFIG["timeout"]

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=timeout)
                if response.status_code == 200:
                    logger.debug(f"Successfully connected to URL: {url}")
                    return response
                else:
                    logger.warning(f"Attempt {attempt + 1} failed with status {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"Attempt {attempt + 1} failed with error: {e}")

            time.sleep(delay)

        logger.error(f"All retries failed for URL: {url}")
        return None

    def update_reference_data(self):
        """
        Processes all chunks in the output directory to scrape and update reference data.
        """
        split_chunks_dir = CONFIG["output_directory"]
        updated_chunks_dir = os.path.join(split_chunks_dir, "updated_chunks")
        os.makedirs(updated_chunks_dir, exist_ok=True)

        chunk_files = [f for f in os.listdir(split_chunks_dir) if f.endswith('.xlsx')]

        def process_chunk(chunk_file):
            chunk_path = os.path.join(split_chunks_dir, chunk_file)
            updated_chunk_path = os.path.join(updated_chunks_dir, chunk_file)
            logger.info(f"Processing chunk: {chunk_path}")

            try:
                reference_data = pd.read_excel(chunk_path)

                for index, row in reference_data.iterrows():
                    sku = row.get("SKU")
                    if not sku:
                        logger.warning(f"No SKU for row {index + 1} in chunk: {chunk_file}")
                        continue

                    self.scrape_and_update_row(sku, reference_data, index)

                # Save the updated chunk
                reference_data.to_excel(updated_chunk_path, index=False)
                logger.info(f"Saved updated chunk: {updated_chunk_path}")

            except Exception as e:
                logger.error(f"Error processing chunk {chunk_file}: {e}")

        # Use multithreading for concurrency
        with ThreadPoolExecutor(max_workers=CONFIG["scraper_threads"]) as executor:
            executor.map(process_chunk, chunk_files)
