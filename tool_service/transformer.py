from tool_service.util.logger import get_logger
from tool_service.src.load_csv import CSVLoader

logger = get_logger(__name__)


class DataTransformer:
    def __init__(self):
        self.logger = logger

    def transform_data(self, source: dict, destination: dict) -> dict:
        """Transform data from source to destination."""
        self.logger.info("Transformer started processing")

        source_path = source.get("path")
        table_name = destination.get("table")

        self._log_transform_details(source_path, table_name)
        return self._build_transform_result(source_path, table_name)

    def _log_transform_details(self, source_path: str, table_name: str) -> None:
        """Log transformation details for debugging and save to file."""
        self.logger.info(f"Source path: {source_path}")
        self.logger.info(f"Destination table: {table_name}")
        # Logs are automatically saved to logs/app.log via FileHandler

    def _build_transform_result(self, source_path: str, table_name: str) -> dict:
        """Build transformation result."""
        
        data_csv = CSVLoader(self.logger).load_csv(source_path)
        return {
            "message": f"Would transform data from {source_path} into table {table_name}"
        }


# Maintain backward compatibility with existing code
def transform_data(source: dict, destination: dict) -> dict:
    """Wrapper function for backward compatibility."""
    transformer = DataTransformer()
    return transformer.transform_data(source, destination)