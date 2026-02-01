import pandas as pd
from tool_service.src.read_csv import CSVLoader
from unittest.mock import patch
from tool_service.src.read_csv import CSVLoader


def test_load_csv_success(tmp_path):
    # Create temporary CSV file
    file_path = tmp_path / "sample.csv"

    df_expected = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [25, 30]
    })
    df_expected.to_csv(file_path, index=False)

    loader = CSVLoader()
    result = loader.load_csv(str(file_path))

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["name", "age"]


def test_load_csv_file_not_found():
    loader = CSVLoader()
    result = loader.load_csv("missing_file.csv")

    assert isinstance(result, dict)
    assert result["status"] == "failed"
    assert "File not found" in result["error"]
    


@patch("tool_service.src.read_csv.pd.read_csv")
def test_load_csv_generic_exception(mock_read_csv):
    mock_read_csv.side_effect = Exception("Unexpected failure")

    loader = CSVLoader()
    result = loader.load_csv("any_file.csv")

    assert isinstance(result, dict)
    assert result["status"] == "failed"
    assert "Unexpected failure" in result["error"]