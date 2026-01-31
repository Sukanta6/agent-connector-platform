from tool_service.util.logger import get_logger

logger = get_logger(__name__)


def transform_data(source: dict, destination: dict) -> dict:
    logger.info("Transformer started processing")

    source_path = source.get("path")
    table_name = destination.get("table")

    logger.info(f"Source path: {source_path}")
    logger.info(f"Destination table: {table_name}")

    return {
        "message": f"Would transform data from {source_path} into table {table_name}"
    }