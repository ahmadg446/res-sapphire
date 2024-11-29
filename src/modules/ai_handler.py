import logging
from transformers import pipeline
from src.config import CONFIG

logger = logging.getLogger(__name__)

class AIHandler:
    def __init__(self):
        # Load model and pipeline from config
        self.model_name = CONFIG.get("ai_handler_model")
        self.max_tokens = CONFIG.get("ai_handler_max_tokens")
        self.temperature = CONFIG.get("ai_handler_temperature")
        self.enable_ai = CONFIG.get("enable_ai_handler)
        
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

    def select_sheet(self, sheet_names):
        """
        Select the most relevant sheet from a list of sheet names.
        """
        if not self.enable_ai or not self.pipeline:
            logger.warning("AI Handler is disabled or not properly initialized.")
            return None

        prompt = f"Given the following sheet names: {sheet_names}, which sheet contains the relevant data for processing?"
        try:
            response = self.pipeline(prompt, max_length=self.max_tokens, num_return_sequences=1, temperature=self.temperature)
            selected_sheet = response[0]["generated_text"].strip()
            logger.info(f"Selected sheet: {selected_sheet}")
            return selected_sheet
        except Exception as e:
            logger.error(f"Error selecting sheet with AI model: {e}")
            return None

    def fill_additional_info(self, chunk):
        """
        Enrich a chunk of data using the AI model.
        """
        if not self.enable_ai or not self.pipeline:
            logger.warning("AI Handler is disabled or not properly initialized.")
            return chunk

        prompt_template = (
            "Enrich the following data chunk by adding missing information. "
            "Chunk:\n{chunk_data}"
        )

        try:
            for index, row in chunk.iterrows():
                chunk_data = row.to_dict()
                prompt = prompt_template.format(chunk_data=chunk_data)
                response = self.pipeline(prompt, max_length=self.max_tokens, num_return_sequences=1, temperature=self.temperature)
                enriched_data = response[0]["generated_text"].strip()

                # Example of enrichment (modify as needed):
                row["AI_Enriched_Info"] = enriched_data
                logger.info(f"Processed row {index}: {enriched_data}")

        except Exception as e:
            logger.error(f"Error enriching chunk with AI model: {e}")

        return chunk
