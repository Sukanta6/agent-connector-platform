import csv
from typing import List, Dict
from tool_service.util.logger import get_logger

logger = get_logger(__name__)


class CSVLoader:
    def __init__(self, logger=None):
        """Initialize CSVLoader with optional logger."""
        self.logger = logger if logger else get_logger(__name__)

    def load_csv(self, file_path: str) -> List[Dict]:
        """Load data from CSV file."""
        self.logger.info(f"CSVLoader started loading from: {file_path}")

        try:
            data = self._read_csv_file(file_path)
            self.logger.info(f"Successfully loaded {len(data)} rows from CSV")
            return {
                "status": "success",
                "file_path": file_path,
                "row_count": len(data),
                "data": data
            }
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

    def _read_csv_file(self, file_path: str) -> List[Dict]:
        """Read CSV file and return list of dictionaries."""
        rows = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

    def _log_csv_details(self, file_path: str, row_count: int) -> None:
        """Log CSV file details for debugging."""
        self.logger.info(f"CSV file path: {file_path}")
        self.logger.info(f"Total rows loaded: {row_count}")


# Maintain backward compatibility with existing code
def load_csv(file_path: str) -> Dict:
    """Wrapper function for backward compatibility."""
    csv_loader = CSVLoader(logger)
    return csv_loader.load_csv(file_path)
