import logging
from transformers import pipeline
from src.config import CONFIG
from modules.filemanager import FileManager

logger = logging.getLogger(__name__)

class AIHandler:
    def __init__(self):
        # Load model and pipeline from config
        self.model_name = CONFIG.get("ai_handler_model")
        self.max_tokens = CONFIG.get("ai_handler_max_tokens")
        self.temperature = CONFIG.get("ai_handler_temperature")
        self.enable_ai = CONFIG.get("enable_ai_handler")
        
        if self.enable_ai:
            try:
                self.pipeline = pipeline("text-generation", model=self.model_name)
                logger.info(f"AI model '{self.model_name}' loaded successfully.")
            except Exception as e:
                logger.error(f"Failed to load model '{self.model_name}': {e}")
                self.pipeline = None
        else:
            logger.warning("AI Handler is disabled in the configuration.")
            self.pipeline = None


    def api_payload(headers):

        assets_dir = CONFIG.get("input_file_path")
        chunk_size = CONFIG.get("chunk_size")

        file_manager = FileManager(assets_dir, chunk_size)

        headers = file_manager.process()
        if not headers:
            print("Failed to extract headers.")
            return
        
        