import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError
from pandas.errors import ParserError

from tool_service.src.load_sql import SQLDataFrameLoader


class DummyLogger:
    def info(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass


# ---------- Engine Initialization ----------

@patch("tool_service.src.load_sql.create_engine")
def test_engine_initialization(mock_create_engine):
    destination = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    SQLDataFrameLoader(DummyLogger(), destination)
    mock_create_engine.assert_called_once()


# ---------- Connection String Branches ----------

@patch("tool_service.src.load_sql.create_engine")
def test_build_connection_string_postgres(mock_engine):
    dest = {
        "db_type": "postgresql",
        "connection": {"user": "u", "password": "p", "host": "h", "port": 5432, "database": "d"}
    }
    loader = SQLDataFrameLoader(DummyLogger(), dest)
    assert "postgresql://" in loader._build_connection_string()


@patch("tool_service.src.load_sql.create_engine")
def test_postgres_connection_string_no_password(mock_engine):
    dest = {
        "db_type": "postgresql",
        "connection": {"user": "u", "host": "h", "port": 5432, "database": "d"}
    }
    loader = SQLDataFrameLoader(DummyLogger(), dest)
    assert "postgresql://u@" in loader._build_connection_string()


@patch("tool_service.src.load_sql.create_engine")
def test_build_connection_string_mssql(mock_engine):
    dest = {
        "db_type": "mssql",
        "connection": {"user": "u", "password": "p", "host": "h", "port": 1433, "database": "d"}
    }
    loader = SQLDataFrameLoader(DummyLogger(), dest)
    assert "mssql+pymssql://" in loader._build_connection_string()


@patch("tool_service.src.load_sql.create_engine")
def test_mssql_connection_string_no_password(mock_engine):
    dest = {
        "db_type": "mssql",
        "connection": {"user": "u", "host": "h", "port": 1433, "database": "d"}
    }
    loader = SQLDataFrameLoader(DummyLogger(), dest)
    assert "mssql+pymssql://" in loader._build_connection_string()


def test_unsupported_db_type():
    dest = {"db_type": "oracle", "connection": {}}
    with pytest.raises(ValueError):
        SQLDataFrameLoader(DummyLogger(), dest)


# ---------- Test Connection ----------

@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.text")
def test_test_connection_success(mock_text, mock_engine):
    mock_conn = mock_engine.return_value.connect.return_value.__enter__.return_value
    mock_conn.execute.return_value = None

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.test_connection()
    assert result["success"] is True


@patch("tool_service.src.load_sql.create_engine")
def test_test_connection_failure(mock_engine):
    mock_engine.return_value.connect.side_effect = SQLAlchemyError("fail")

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.test_connection()
    assert result["success"] is False


@patch("tool_service.src.load_sql.create_engine")
def test_test_connection_unexpected_exception(mock_engine):
    mock_engine.return_value.connect.side_effect = Exception("weird error")

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.test_connection()
    assert result["success"] is False


# ---------- Load DataFrame ----------

@patch("tool_service.src.load_sql.create_engine")
def test_load_dataframe_success(mock_engine):
    df = pd.DataFrame({"id": [1, 2]})
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}, "table": "users"}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    with patch.object(df, "to_sql") as mock_to_sql:
        result = loader.load_dataframe_to_sql(df)

    assert result is True


@patch("tool_service.src.load_sql.create_engine")
def test_load_dataframe_empty(mock_engine):
    df = pd.DataFrame()
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}, "table": "users"}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.load_dataframe_to_sql(df) is False


@patch("tool_service.src.load_sql.create_engine")
def test_load_dataframe_missing_table(mock_engine):
    df = pd.DataFrame({"a": [1]})
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.load_dataframe_to_sql(df) is False


@patch("tool_service.src.load_sql.create_engine")
def test_load_dataframe_sqlalchemy_error(mock_engine):
    df = pd.DataFrame({"id": [1]})
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}, "table": "users"}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    with patch.object(df, "to_sql", side_effect=SQLAlchemyError("sql error")):
        assert loader.load_dataframe_to_sql(df) is False


@patch("tool_service.src.load_sql.create_engine")
def test_load_dataframe_generic_exception(mock_engine):
    df = pd.DataFrame({"id": [1]})
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}, "table": "users"}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    with patch.object(df, "to_sql", side_effect=Exception("boom")):
        assert loader.load_dataframe_to_sql(df) is False


# ---------- CSV Loading ----------

@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.pd.read_csv")
def test_load_csv_to_database_success(mock_read_csv, mock_engine):
    df = pd.DataFrame({"id": [1]})
    mock_read_csv.return_value = df

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}, "table": "users"}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    with patch.object(loader, "load_dataframe_to_sql", return_value=True):
        assert loader.load_csv_to_database("file.csv") is True


@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.pd.read_csv", side_effect=FileNotFoundError)
def test_load_csv_file_not_found(mock_read, mock_engine):
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.load_csv_to_database("missing.csv") is False


@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.pd.read_csv", side_effect=ParserError("bad csv"))
def test_load_csv_parser_error(mock_read, mock_engine):
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.load_csv_to_database("bad.csv") is False


# ---------- Table Inspection ----------

@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.inspect")
def test_get_table_info_missing(mock_inspect, mock_engine):
    mock_inspector = mock_inspect.return_value
    mock_inspector.has_table.return_value = False

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.get_table_info("users") is None


@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.inspect")
def test_get_table_info_success(mock_inspect, mock_engine):
    mock_inspector = mock_inspect.return_value
    mock_inspector.has_table.return_value = True
    mock_inspector.get_columns.return_value = [{"name": "id"}, {"name": "name"}]

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.get_table_info("users")
    assert result["column_count"] == 2


@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.inspect")
def test_list_tables(mock_inspect, mock_engine):
    mock_inspect.return_value.get_table_names.return_value = ["users", "orders"]

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.list_tables() == ["users", "orders"]


@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.inspect", side_effect=Exception("inspect error"))
def test_list_tables_exception(mock_inspect, mock_engine):
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    assert loader.list_tables() == []


# ---------- Close Connection ----------

@patch("tool_service.src.load_sql.create_engine")
def test_close_connection(mock_engine):
    mock_engine.return_value.dispose = MagicMock()

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    loader.close()
    mock_engine.return_value.dispose.assert_called_once()


@patch("tool_service.src.load_sql.create_engine")
def test_close_connection_exception(mock_engine):
    mock_engine.return_value.dispose.side_effect = Exception("close error")

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    loader.close()  # should not raise
    
@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.inspect")
def test_get_table_info_success(mock_inspect, mock_engine):
    mock_inspector = mock_inspect.return_value
    mock_inspector.has_table.return_value = True
    mock_inspector.get_columns.return_value = [{"name": "id"}, {"name": "name"}]

    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.get_table_info("users")
    assert result["column_count"] == 2
    
@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.inspect", side_effect=Exception("inspect fail"))
def test_get_table_info_exception(mock_inspect, mock_engine):
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.get_table_info("users")
    assert result is None
    
@patch("tool_service.src.load_sql.create_engine")
@patch("tool_service.src.load_sql.pd.read_csv", side_effect=Exception("unknown error"))
def test_load_csv_unexpected_exception(mock_read, mock_engine):
    dest = {"db_type": "sqlite", "connection": {"database": "test.db"}}
    loader = SQLDataFrameLoader(DummyLogger(), dest)

    result = loader.load_csv_to_database("file.csv")
    assert result is False