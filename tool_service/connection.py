from tool_service.transformer import transform_data
from tool_service.util.logger import get_logger

logger = get_logger(__name__)


def handle_request(request: dict) -> dict:
    logger.info("Connection layer received request")

    environment = request.get("environment")
    source = request.get("source", {})
    destination = request.get("destination", {})

    logger.info(f"Environment: {environment}")
    logger.info(f"Source type: {source.get('type')}")
    logger.info(f"Destination type: {destination.get('type')}")

    try:
        result = transform_data(source, destination)

        return {
            "status": "success",
            "environment": environment,
            "result": result
        }

    except Exception as e:
        logger.error("Error in connection layer", exc_info=True)
        return {
            "status": "failed",
            "error": str(e)
        }