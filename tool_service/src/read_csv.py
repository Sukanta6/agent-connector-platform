
import pandas as pd
from typing import Union, Dict
from tool_service.util.logger import get_logger

logger = get_logger(__name__)


class CSVLoader:
    def __init__(self, logger=None):
        """Initialize CSVLoader with optional logger."""
        self.logger = logger if logger else get_logger(__name__)

    def load_csv(self, file_path: str) -> Union[pd.DataFrame, Dict]:
        """Load data from CSV file and return as DataFrame."""
        self.logger.info(f"CSVLoader started loading")

        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"Successfully read {len(df)} rows from CSV")
            return df
        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {file_path}")
            return {
                "status": "failed",
                "error": f"File not found: {file_path}"
            }
        except Exception as e:
            self.logger.error(f"Error loading CSV file", exc_info=True)
            return {
                "status": "failed",
                "error": str(e)
            }


