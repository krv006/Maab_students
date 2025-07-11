import sys
from pathlib import Path

# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

import logging

class ErrorHandler:
    
    def __init__(self):
        try:
            self.log_file = "errors.log"
            self._setup_logging()
        except Exception as e:
            # Fallback to console logging if config fails
            print(f"CRITICAL: Failed to initialize error handler: {str(e)}")
            self.log_file = "fallback_error.log"
            self._setup_logging()
            self.log_error(f"Original config error: {str(e)}")
        
    def _setup_logging(self):

        handler = logging.FileHandler(self.log_file, encoding="utf-8")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', 
            handlers=[handler]
        )

    def log_info(self, info):
        """Log informational messages"""
        logging.info(f"Info: {str(info)}")

    def log_complete(self, info):
        """Log informational messages"""
        logging.info(f"_COMPLETED_: {str(info)}")
        
    def log_error(self, error):
        """Log error messages without traceback"""
        logging.error(f"_ERROR_: {str(error)}")
      
    def log_exception(self, error: Exception):
        """
        Logs the given exception with full traceback.

        Args:
            error (Exception): The exception to log.
        """
        logging.error("Exception occurred _ERROR_: %s", error, exc_info=error)

# Global instance for easy access
error_manager = ErrorHandler()
class ExpectedCustomError(Exception):
    pass