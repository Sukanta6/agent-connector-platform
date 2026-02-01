from tool_service.transformer import DataTransformer as DT
from tool_service.util.logger import get_logger

logger = get_logger(__name__)


class ConnectionHandler:
    def __init__(self):
        self.logger = logger

    def handle_request(self, request: dict) -> dict:
        """Handle incoming request with source and destination configuration."""
        self.logger.info("Connection layer received request")

        environment = request.get("environment")
        source = request.get("source", {})
        destination = request.get("destination", {})

        self._log_request_details(environment, source, destination)

        try:
            result = DT().transform_data(source, destination)
            return self._success_response(environment, result)
        except Exception as e:
            self.logger.error("Error in connection layer", exc_info=True)
            return self._error_response(e)

    def _log_request_details(self, environment: str, source: dict, destination: dict) -> None:
        """Log request details for debugging."""
        self.logger.info(f"Environment: {environment}")
        self.logger.info(f"Source type: {source.get('type')}")
        self.logger.info(f"Destination type: {destination.get('type')}")

    def _success_response(self, environment: str, result: dict) -> dict:
        """Build success response."""
        return {
            "status": "success",
            "environment": environment,
            "result": result
        }

    def _error_response(self, error: Exception) -> dict:
        """Build error response."""
        return {
            "status": "failed",
            "error": str(error)
        }
