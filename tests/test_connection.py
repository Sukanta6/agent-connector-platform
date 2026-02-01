from unittest.mock import patch
from tool_service.connection import ConnectionHandler


@patch("tool_service.connection.DT")
def test_handle_request_success(mock_dt):
    # Mock transformer success
    mock_transformer = mock_dt.return_value
    mock_transformer.transform_data.return_value = {"rows_loaded": 5}

    handler = ConnectionHandler()

    request = {
        "environment": "local",
        "source": {"type": "csv"},
        "destination": {"type": "sql"}
    }

    result = handler.handle_request(request)

    assert result["status"] == "success"
    assert result["environment"] == "local"
    assert result["result"]["rows_loaded"] == 5
    mock_transformer.transform_data.assert_called_once()


@patch("tool_service.connection.DT")
def test_handle_request_failure(mock_dt):
    # Mock transformer raising exception
    mock_transformer = mock_dt.return_value
    mock_transformer.transform_data.side_effect = Exception("Pipeline error")

    handler = ConnectionHandler()

    request = {
        "environment": "local",
        "source": {"type": "csv"},
        "destination": {"type": "sql"}
    }

    result = handler.handle_request(request)

    assert result["status"] == "failed"
    assert "Pipeline error" in result["error"]