import json
from tool_service.connection import handle_request
from tool_service.util.logger import save_logs_to_file


class RequestRunner:
    def __init__(self, request_file="mocks/request.json"):
        self.request_file = request_file

    def load_request(self):
        """Load request payload from JSON file."""
        with open(self.request_file) as f:
            return json.load(f)

    def run(self):
        """Execute the request and return result."""
        request_payload = self.load_request()
        result = handle_request(request_payload)
        return result


if __name__ == "__main__":
    runner = RequestRunner()
    result = runner.run()
    print("Final Result:", result)
    
    # Save all logs to file at the end
    save_logs_to_file()