from unittest.mock import patch, MagicMock
from tool_service.transformer import DataTransformer


# ---------- Success Path ----------

@patch("tool_service.transformer.SDFL")
@patch("tool_service.transformer.CSVLoader")
def test_transform_data_success(mock_csv_loader, mock_sdfl):
    # Mock CSV read
    mock_csv_loader.return_value.load_csv.return_value = "fake_dataframe"

    # Mock DB loader
    mock_loader_instance = mock_sdfl.return_value
    mock_loader_instance.test_connection.return_value = {"success": True}
    mock_loader_instance.load_dataframe_to_sql.return_value = True

    transformer = DataTransformer()

    source = {"path": "file.csv"}
    destination = {"table": "users"}

    result = transformer.transform_data(source, destination)

    assert result is True
    mock_loader_instance.test_connection.assert_called_once()
    mock_loader_instance.load_dataframe_to_sql.assert_called_once_with("fake_dataframe", "users")


# ---------- Connection Failure Path ----------

@patch("tool_service.transformer.SDFL")
@patch("tool_service.transformer.CSVLoader")
def test_transform_data_connection_failure(mock_csv_loader, mock_sdfl):
    mock_csv_loader.return_value.load_csv.return_value = "fake_dataframe"

    mock_loader_instance = mock_sdfl.return_value
    mock_loader_instance.test_connection.return_value = {
        "success": False,
        "message": "Connection failed"
    }

    transformer = DataTransformer()

    source = {"path": "file.csv"}
    destination = {"table": "users"}

    result = transformer.transform_data(source, destination)

    assert result["success"] is False
    assert result["message"] == "Connection failed"
    mock_loader_instance.load_dataframe_to_sql.assert_not_called()


# ---------- CSV Load Failure Propagates ----------

@patch("tool_service.transformer.CSVLoader")
def test_transform_data_csv_failure(mock_csv_loader):
    mock_csv_loader.return_value.load_csv.side_effect = Exception("CSV error")

    transformer = DataTransformer()

    source = {"path": "file.csv"}
    destination = {"table": "users"}

    try:
        transformer.transform_data(source, destination)
        assert False, "Exception should have been raised"
    except Exception as e:
        assert "CSV error" in str(e)