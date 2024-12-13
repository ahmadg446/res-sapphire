import sys
import logging
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import CONFIG
from src.modules.filemanager import FileManager
from src.modules.webscraper import WebScraper


def setup_logging():
    log_level_terminal = logging.INFO
    log_level_file = logging.DEBUG

    log_file = CONFIG["log_file"]
    if log_file:
        logs_dir = os.path.dirname(log_file)
        os.makedirs(logs_dir, exist_ok=True)

        with open(log_file, 'w'):
            pass

    terminal_handler = logging.StreamHandler()
    terminal_handler.setLevel(log_level_terminal)
    terminal_formatter = logging.Formatter("%(message)s")
    terminal_handler.setFormatter(terminal_formatter)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level_file)
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[terminal_handler, file_handler])

setup_logging()
logger = logging.getLogger(__name__)

class Main:
    def __init__(self):
        self.assets_dir = os.path.dirname(CONFIG["input_file_path"])
        self.chunk_size = CONFIG["chunk_size"]
        self.file_manager = FileManager(self.assets_dir, self.chunk_size)
        self.web_scraper = WebScraper()

    def orchestrate(self):
        logger.info("Starting directory processing...")
        self.file_manager.process()

        if CONFIG["require_confirmation"]:
            proceed = input("Do you want to continue with web scraping? (Y/N): ").strip().lower()
            if proceed != "y":
                logger.info("Skipping web scraping. Exiting.")
                return
        else:
            logger.info("Web scraping set to run automatically.")

        logger.info("Proceeding with web scraping...")
        chunks = self.file_manager.load_split_chunks()
        self.web_scraper.process_chunks(chunks)

if __name__ == "__main__":
    main = Main()
    main.orchestrate()
