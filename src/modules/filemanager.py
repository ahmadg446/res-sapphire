import pandas as pd
from collections import defaultdict
import os
import glob
import logging
from src.config import CONFIG

logger = logging.getLogger(__name__)

class FileManager:
    def __init__(self, assets_dir, chunk_size):
        self.assets_dir = assets_dir
        self.chunk_size = chunk_size
        self.ref_file = CONFIG["input_file_path"]
        self.split_chunks_dir = CONFIG["output_directory"]
        self.processed_dir = CONFIG["processed_directory"]

    def process(self):
        logger.info("Starting file processing...")
        self.validate_and_prepare_directories()

        try:
            reference_data = pd.read_excel(self.ref_file)
            logger.debug(f"Loaded reference data: {reference_data.shape[0]} rows, {reference_data.shape[1]} columns.")
        except Exception as e:
            logger.error(f"Failed to load reference data: {e}")
            return

        chunk_number = 1
        for start_row in range(0, len(reference_data), self.chunk_size):
            chunk = reference_data.iloc[start_row:start_row + self.chunk_size]
            chunk_file = os.path.join(self.split_chunks_dir, f"chunk{chunk_number}.xlsx")
            chunk.to_excel(chunk_file, index=False)
            logger.debug(f"Saved chunk {chunk_number}: {chunk.shape[0]} rows to {chunk_file}")
            chunk_number += 1

    def extract_headers(self, file_path):
        
        try:
            # Load the Excel file without treating any rows as header rows
            df = pd.read_excel(file_path, header=None, nrows=2)

            # First row contains main categories
            main_categories = df.iloc[0].fillna("Uncategorized").tolist()

            # Second row contains subcategories
            subcategories = df.iloc[1].fillna("Unnamed").tolist()

            # Group subcategories under their main categories
            headers = defaultdict(list)
            for main, sub in zip(main_categories, subcategories):
                headers[main.strip()].append(sub.strip())

            return headers

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return None
            
    def process_update_sheet(self):
        update_file_path = CONFIG.get("update_template_path")
        if not update_file_path:
            logger.error("Update template path is not configured in CONFIG.")
            return None

        logger.debug(f"Processing update sheet: {update_file_path}")

        try:
            update_data = pd.read_excel(update_file_path)
            logger.debug(f"Loaded update sheet: {update_data.shape[0]} rows, {update_data.shape[1]} columns.")
        except Exception as e:
            logger.error(f"Failed to load update sheet: {e}")
            return None

        # Extract headers and subcategories
        headers = self.extract_headers(update_file_path)
        if headers:
            for main_category, subcategories in headers.items():
                logger.debug(f"Main Category: {main_category}, Subcategories: {subcategories}")

        return update_data


    def validate_and_prepare_directories(self):
        os.makedirs(self.split_chunks_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        for file in os.listdir(self.split_chunks_dir):
            os.remove(os.path.join(self.split_chunks_dir, file))

    def load_split_chunks(self):
        chunks = []
        for file in glob.glob(os.path.join(self.split_chunks_dir, "*.xlsx")):
            chunks.append({"file_path": file, "data": pd.read_excel(file)})
        return chunks