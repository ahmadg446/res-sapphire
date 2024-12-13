import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG = {

    # General settings
    "require_confirmation": True,  # Set to False to disable user prompts (True for debugging purposes)
    "chunk_size": 10,  # Number of rows per chunk

    # Web scraper settings
    "scraper_base_url": "https://egyptianlinens.com/search?view=ajax&q={sku}&options[prefix]=last&type=product",  # Base URL for scraping
    "max_retries": 3,                              # Max retries for failed requests
    "rate_limit_delay": 1.0,                       # Delay (in seconds) between requests
    "scraper_threads": 5,                          # Number of parallel threads for scraping
    "default_selector": ".item-name",              # Default CSS selector for parsing HTML
    "timeout": 10,                                 # Request timeout in seconds
    "retry_backoff": 2,                            # Backoff time (in seconds) between retries
    "log_scraped_urls": True,                      # Log all scraped URLs for debugging
    "custom_headers": {                            # Headers for web requests
        "User-Agent": "sapphire/1.0 (compatible; res-sapphire; +https://regaledgesolutions.com)",
    },


    # File handling settings
    "input_file_path": os.path.join(PROJECT_ROOT, "assets/ref/reference_data.xlsx"),
    "output_directory": os.path.join(PROJECT_ROOT, "assets/split_chunks"),
    "processed_directory": os.path.join(PROJECT_ROOT, "assets/processed_chunks"),
    "template_directory": os.path.join(PROJECT_ROOT, "assets/template"),
    "update_template_path": os.path.join(PROJECT_ROOT, "assets/template/updatetemplate.xlsx"),
    "update_template_filled_path": os.path.join(PROJECT_ROOT, "assets/template/updatetemplate_filled.xlsx"),
    "log_file": os.path.join(PROJECT_ROOT, "logs/process.log"),
    "chunk_size": 10,
    "log_level": "INFO",

    # Debugging and logging (Terminal output restricted, detailed logs in process_log)
    "log_level": "INFO",  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
    "save_intermediate": True,  # Save intermediate results for debugging

    # AI handler configuration
    "ai_handler_model": "res-sapphire-01", 
    "ai_handler_max_tokens": 100,  # Adjust token limits as needed
    "enable_ai_handler": True,  # Toggle AI handler usage
    "ai_handler_max_tokens": 100,  # Maximum tokens to use in each completion
    "ai_handler_temperature": 0.7,  # Sampling temperature for OpenAI completions
    
}