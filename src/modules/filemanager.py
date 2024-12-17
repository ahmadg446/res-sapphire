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
        logger.info("Processing reference data...")
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
            saved_chunk_files.append(chunk_file)
            logger.debug(f"Saved chunk {chunk_number}: to {chunk.shape[0]} rows to {chunk_file}")

        extracted_headers = {}
        if saved_chunk_files:
            first_chunk = saved_chunk_files[0]
            extracted_headers = self.extract_headers(first_chunk)

            if extracted_headers:  

                logger.debug("Reference data processing completed.")

                update_sheet_headers = self.process_update_sheet()
                if update_sheet_headers:
                    
                    return update_sheet_headers
            return extracted_headers
            
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
            df = pd.read_excel(file_path, header = None)

            headers = {}

            title_row_index = 0

            identified_columns = []

            for col_index in range(df.shape[1]):
                title = df.iloc[title_row_index, col_index]

                if pd.isna(title or title == ""):
                    continue

                title = str(title).strip()
                identified_columns.append(f"'{title}' ({col_index + 1})")
                column_data = df.iloc[title_row_index + 1:, col_index].dropna().tolist()

                headers[title] = column_data

            logger.debug(f"Columns identified: {', '.join(identified_columns)}")

            return headers
            
        except Exception as e:
            logger.error(f"Failed to extract headers from {file_path}: {e}")
            return None

            
    def process_update_sheet(self):
        try: 
            update_sheet_path = CONFIG.get("update_template_path")
            if not update_sheet_path:
                logger.error("Update sheet path not configured.")
                return None, None
            
            workbook = pd.ExcelFile(update_sheet_path)
            sheet_names = workbook.sheet_names
            logger.debug(f"Sheets available in Update Template: {', '.join(sheet_names)}")
            
            # logger.debug(f"Loading sheet: {sheet_names[0]}") [i dont wanna deal with this rn]
            df = pd.read_excel(update_sheet_path, sheet_names[0], header = None)

            headers = {}
            title_row_index = 0

            for col_index in range(df.shape[1]):
                title = df.iloc[title_row_index, col_index]

                if pd.isna(title) or title == "":
                    continue

                title = str(title).strip()
                column_data = df.iloc[title_row_index + 1:, col_index].dropna().tolist()
                headers[title] = column_data

            logger.debug(f"Update Sheet Columns identified: {', '.join([f"'{title}' ({col_index + 1})" for col_index, title, in enumerate(headers.keys())])}")

            return headers
        
        except Exception as e:
            logger.error(f"Failed to process the update sheet: {e}")
            return None
        
    def load_split_chunks(self):
        chunks = []
        for file in glob.glob(os.path.join(self.split_chunks_dir, "*.xlsx")):
            chunks.append({"file_path": file, "data": pd.read_excel(file)})
        return chunks
    
    def process_chunk(self, chunk, chunk_number): # default chunk processing
        try:
            # process_chunk_api(chunk, chunk_number) [uncomment to send info to api right away]

            chunk_file = self.generate_chunk_filename(chunk_number)
            chunk.to_excel(chunk_file, index = False)
            logger.debug(f"Saved chunk {chunk_number}: {chunk.shape[0]} cells identified.")
        except Exception as e:
            logger.error(f"Failed to process chunk: {chunk_number}: {e}")

    def generate_chunk_filename(self, chunk_number):
        return os.path.join(self.split_chunks_dir, f"chunk{chunk_number}.xlsx")
    