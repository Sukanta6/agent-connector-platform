import json
from tool_service.connection import handle_request


def load_request():
    with open("mocks/request.json") as f:
        return json.load(f)


if __name__ == "__main__":
    request_payload = load_request()
    result = handle_request(request_payload)
    print("Final Result:", result)