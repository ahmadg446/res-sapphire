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
            logger.info(f"Loaded reference data: {reference_data.shape[0]} rows, {reference_data.shape[1]} columns.")
        except Exception as e:
            logger.error(f"Failed to load reference data: {e}")
            return

        chunk_number = 1
        for start_row in range(0, len(reference_data), self.chunk_size):
            chunk = reference_data.iloc[start_row:start_row + self.chunk_size]
            chunk_file = os.path.join(self.split_chunks_dir, f"chunk{chunk_number}.xlsx")
            chunk.to_excel(chunk_file, index=False)
            logger.info(f"Saved chunk {chunk_number}: {chunk.shape[0]} rows to {chunk_file}")
            chunk_number += 1

    def validate_and_prepare_directories(self):
        """
        Ensure the split_chunks and processed directories are prepared, and clear only files.
        """
        os.makedirs(self.split_chunks_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

        # Clear only files, not directories
        for file in os.listdir(self.split_chunks_dir):
            file_path = os.path.join(self.split_chunks_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

    def extract_headers(self, file_path):
        try:
            df = pd.read_excel(file_path, header=None, nrows=2)
            main_categories = df.iloc[0].fillna("Uncategorized").tolist()
            subcategories = df.iloc[1].fillna("Unnamed").tolist()

            headers = defaultdict(list)
            for main, sub in zip(main_categories, subcategories):
                headers[main.strip()].append(sub.strip())

            return headers

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return None

    def load_split_chunks(self):
        chunks = []
        for file in glob.glob(os.path.join(self.split_chunks_dir, "*.xlsx")):
            chunks.append({"file_path": file, "data": pd.read_excel(file)})
        return chunks

    def analyze_reference_columns(self):
        logger.info(f"Analyzing columns in reference file: {self.ref_file}")
        try:
            reference_data = pd.read_excel(self.ref_file, nrows=1, header=None)
            column_headers = [col for col in reference_data.iloc[0] if not pd.isna(col)]
            logger.info(f"Identified {len(column_headers)} columns.")
            return column_headers

        except Exception as e:
            logger.error(f"Error while analyzing reference file: {e}")
            return None

    def update_chunk(self, chunk_data, chunk_file):
        try:
            chunk_data.to_excel(chunk_file, index=False)
            logger.info(f"Updated chunk saved to {chunk_file}")
        except Exception as e:
            logger.error(f"Failed to update chunk {chunk_file}: {e}")
