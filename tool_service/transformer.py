from tool_service.util.logger import get_logger
from tool_service.src.read_csv import CSVLoader
from tool_service.src.load_sql import SQLDataFrameLoader as SDFL


logger = get_logger(__name__)


class DataTransformer:
    def __init__(self):
        self.logger = logger

    def transform_data(self, source: dict, destination: dict) -> dict:
        """Transform data from source to destination."""
        self.logger.info("Transformer started processing")

        source_path = source.get("path")
        table_name = destination.get("table")

        data =  self._build_transform_result(source_path)
        SDFL_instance = SDFL(self.logger, destination)
        response = SDFL_instance.test_connection()
        if response.get("success"):
            self.logger.info(f"Connection to destination successful.")
            load_response = SDFL_instance.load_dataframe_to_sql(data, table_name)
            return load_response
        
        
        return response



    def _build_transform_result(self, source_path: str) -> dict:
        """Build transformation result."""
        
        data_csv = CSVLoader(self.logger).load_csv(source_path)
        return data_csv