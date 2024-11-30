import sys
import logging
import os
from config import CONFIG
from modules.filemanager import FileManager


def setup_logging():
    # Configures logging to terminal and file
    log_level_terminal = logging.INFO
    log_level_file = logging.DEBUG

    log_file = CONFIG.get("log_file")
    if log_file:
        logs_dir = os.path.dirname(log_file)
        os.makedirs(logs_dir, exist_ok=True)
        with open(log_file, 'w'):
            pass  # Clear log file at the start

    terminal_handler = logging.StreamHandler()
    terminal_handler.setLevel(log_level_terminal)
    terminal_formatter = logging.Formatter("%(message)s")
    terminal_handler.setFormatter(terminal_formatter)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level_file)
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[terminal_handler, file_handler])


def initialize_components():
    # Sets up and returns the file manager
    assets_dir = os.path.dirname(CONFIG["input_file_path"])
    chunk_size = CONFIG["chunk_size"]
    return FileManager(assets_dir, chunk_size)


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Application starting...")

    file_manager = initialize_components()

    logger.info("Starting directory processing...")
    file_manager.process()

    if CONFIG["require_confirmation"]:
        proceed = input("Do you want to continue with the next steps? (Y/N): ").strip().lower()
        if proceed != "y":
            logger.info("Exiting.")
            return
    else:
        logger.info("Continuing automatically.")

    logger.info("Analyzing the reference file...")
    reference_columns = file_manager.analyze_reference_columns()
    if reference_columns:
        logger.debug(f"Reference columns identified: {reference_columns}")

    chunks = file_manager.load_split_chunks()
    logger.info(f"Loaded {len(chunks)} chunks for further processing.")

    logger.info("Application completed successfully.")


if __name__ == "__main__":
    main()
