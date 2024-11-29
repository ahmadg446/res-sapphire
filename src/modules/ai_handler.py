import openai
import logging
from src.config import CONFIG

logger = logging.getLogger(__name__)

class AIHandler:
    def __init__(self):
        self.api_key = CONFIG.get("openai_api_key")
        if not self.api_key:
            logger.error("OpenAI API key is missing in CONFIG.")
            raise ValueError("OpenAI API key is required for AIHandler.")

        self.model = CONFIG.get("ai_handler_model", "text-davinci-003")
        self.max_tokens = CONFIG.get("ai_handler_max_tokens", 100)
        self.temperature = CONFIG.get("ai_handler_temperature", 0.7)

        # Set the API key for OpenAI
        openai.api_key = self.api_key

    def select_sheet(self, sheet_names):
        """
        Select the appropriate sheet by querying the AI with the list of sheet names.

        Args:
            sheet_names (list): List of sheet names from the Excel file.

        Returns:
            str: The name of the selected sheet.
        """
        if not sheet_names:
            logger.error("Sheet names list is empty. Cannot proceed.")
            return None

        prompt = f"Given the following sheet names: {sheet_names}, which sheet contains the relevant data for processing?"
        try:
            response = openai.Completion.create(
                engine=self.model,
                prompt=prompt,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            selected_sheet = response.choices[0].text.strip()
            logger.info(f"AI selected sheet: {selected_sheet}")
            return selected_sheet
        except Exception as e:
            logger.error(f"Error querying AI: {e}")
            return None

    def fill_additional_info(self, chunk_data):
        """
        Placeholder for AI-based logic to fill additional info in chunks.
        """
        pass
