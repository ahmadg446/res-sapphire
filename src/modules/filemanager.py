import pandas as pd
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import os
import glob
import logging
from src.config import CONFIG

logger = logging.getLogger(__name__)

# def process_chunk_api(chunk, chunk_number): # placeholder for api interaction w chunk data
    # logger.debug(f"Sending chunk {chunk_number} to API.")

class FileManager:
    def __init__(self, assets_dir, chunk_size):
        self.assets_dir = assets_dir
        self.chunk_size = chunk_size
        self.ref_file = CONFIG["input_file_path"]
        self.split_chunks_dir = CONFIG["output_directory"]
        self.processed_dir = CONFIG["processed_directory"]

    def process(self):
        logger.debug("Starting file processing...")
        self.validate_and_prepare_directories() # ensure directories ready and cleared

        try:
            reference_data = pd.read_excel(self.ref_file)
            logger.debug(f"Loaded reference data: {reference_data.shape[0]} rows, {reference_data.shape[1]} columns.")
        except Exception as e:
            logger.error(f"Failed to load reference data: {e}")
            return
        
        total_chunks = (len(reference_data) + self.chunk_size -1) // self.chunk_size
        saved_chunk_files = []
        
        for chunk_number, start_row in enumerate(range(0, len(reference_data), self.chunk_size), start = 1):
            chunk = reference_data.iloc[start_row:start_row + self.chunk_size]
            chunk_file = self.generate_chunk_filename(chunk_number)
            chunk.to_excel(chunk_file, index = False)
            logger.debug(f"Saved chunk {chunk_number}: to {chunk.shape[0]} rows to {chunk_file}")

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.extract_headers, file_path): file_path for file_path in saved_chunk_files}

        extracted_data = {}
        for future in futures:
            file_path = futures[future]

            try:
                headers = future.result()
                if headers:
                    extracted_data[file_path] = headers
                    logger.debug(f"Extracted headers for {file_path}")

            except Exception as e:
                logger.error(f"Failed to extract headers for {file_path}: {e}")
        
        logger.debug(f"File processing completed.")
        return extracted_data
    
    def load_excel(self, file_path): # load excel file, select sheet w most rows

        try:
            workbook = pd.ExcelFile(file_path)
            selected_sheet = max(workbook.sheet_names, key = lambda sheet: pd. read_excel(file_path, sheet_name = sheet).shape[0])
            data = pd.read_excel(file_path, sheet_name = selected_sheet)
            return data
        except Exception as e:
            raise pd.errors.ExcelError(f"Error loading Excel file: {e}")
    

    def validate_and_prepare_directories(self): 
        os.makedirs(self.split_chunks_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        for file in os.listdir(self.split_chunks_dir):
            os.remove(os.path.join(self.split_chunks_dir, file))

    def extract_headers(self, file_path):

        try:
            df = pd.read_excel(file_path)

            headers = {}

            for col_index, title in enumerate(df.iloc[0]):
                if pd.isna(title):
                    break

                title = str(title),strip()
                column_data = df.iloc[1:, col_index].dropna().tolist()

                headers[title] = column_data

                logger.debug(f"Extracted headers: {list(headers.keys())}")
                for title, data in headers.items():
                    logger.debug(f"   () Title: {title} () Rows: {len(data)}")

                return headers
            
        except Exception as e:
            logger.error(f"Failed to extract headers from {file_path}: {e}")
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

    def load_split_chunks(self):
        chunks = []
        for file in glob.glob(os.path.join(self.split_chunks_dir, "*.xlsx")):
            chunks.append({"file_path": file, "data": pd.read_excel(file)})
        return chunks
    
    def process_chunk(self, chunk, chunk_number): # default chunk processing
        try:
            # process_chunk_api(chunk, chunk_number)

            chunk_file = self.generate_chunk_filename(chunk_number)
            chunk.to_excel(chunk_file, index = False)
            logger.debug(f"Saved chunk {chunk_number}: {chunk.shape[0]} cells identified.")
        except Exception as e:
            logger.error(f"Failed to process chunk: {chunk_number}: {e}")

    def generate_chunk_filename(self, chunk_number):
        return os.path.join(self.split_chunks_dir, f"chunk{chunk_number}.xlsx")
    