"""Module for loading dataframes to local SQL database."""

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any
from urllib.parse import quote_plus
import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from util.logger import get_logger


class SQLDataFrameLoader:
    """Class to load pandas DataFrames to a local SQL database."""
    
    def __init__(self, logger, destination: Dict[str, Any]):
        """
        Initialize the SQLDataFrameLoader.
        
        Args:
            logger: Logger instance for logging operations
            destination: Destination configuration dict with:
                - db_type: Type of database (sqlite, postgresql, mysql)
                - database_name: Name of the database (optional)
                - connection: Dict with host, port, user, password, database
                - table: Table name (optional, can be provided during load)
                - if_exists: How to behave if table exists ('fail', 'replace', 'append')
        """
        self.logger = logger
        self.destination = destination
        self.db_type = destination.get("db_type", "")
        self.database_name = destination.get("database_name", "")
        self.connection_details = destination.get("connection", {})
        self.engine = None
        self._initialize_engine()
    
    def _build_connection_string(self) -> str:
        """Build connection string based on database type and connection details."""
        conn = self.connection_details
        
        if self.db_type.lower() == "sqlite":
            # For SQLite, use the database filename from connection_details or database_name
            db_file = conn.get("database") or f"{self.database_name}.db"
            return f"sqlite:///{db_file}"
        
        elif self.db_type.lower() == "postgresql":
            user = conn.get("user", "postgres")
            password = conn.get("password", "")
            host = conn.get("host", "localhost")
            port = conn.get("port", 5432)
            database = conn.get("database", self.database_name)
            
            if password:
                return f"postgresql://{user}:{password}@{host}:{port}/{database}"
            else:
                return f"postgresql://{user}@{host}:{port}/{database}"
        
        elif self.db_type.lower() == "mssql":
            user = conn.get("user", "root")
            password = conn.get("password", "")
            host = conn.get("host", "localhost")
            port = conn.get("port", 1433)
            database = conn.get("database", self.database_name)
            
            # Use pymssql driver for SQL Server to avoid requiring ODBC drivers
            # SQLAlchemy connection string for pymssql:
            #   mssql+pymssql://<user>:<password>@<host>:<port>/<database>
            # URL-encode credentials to safely include special characters (e.g. '@')
            user_enc = quote_plus(user) if user else ""
            if password:
                password_enc = quote_plus(password)
                return f"mssql+pymssql://{user_enc}:{password_enc}@{host}:{port}/{database}"
            else:
                return f"mssql+pymssql://{user_enc}@{host}:{port}/{database}"
        
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _initialize_engine(self) -> None:
        """Initialize the database engine based on the database type."""
        try:
            connection_string = self._build_connection_string()
            self.engine = create_engine(connection_string)
            
            self.logger.info(f"Database engine initialized for {self.db_type} database: {self.database_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database engine: {str(e)}")
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the database connection.
        
        Returns:
            dict: Connection test result with 'success' bool and 'message' string
        """
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                self.logger.info(f"Database connection test successful for {self.db_type}")
                return {
                    "success": True,
                    "message": f"Successfully connected to {self.db_type} database: {self.database_name}",
                    "db_type": self.db_type,
                    "database": self.database_name
                }
        except SQLAlchemyError as e:
            error_msg = f"Database connection test failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "db_type": self.db_type,
                "database": self.database_name
            }
        except Exception as e:
            error_msg = f"Unexpected error during connection test: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "db_type": self.db_type,
                "database": self.database_name
            }
    
    def load_dataframe_to_sql(
        self,
        dataframe: pd.DataFrame,
        table_name: Optional[str] = None,
        if_exists: Optional[str] = None,
        index: bool = False,
        chunk_size: Optional[int] = None
    ) -> bool:
        """
        Load a pandas DataFrame to the SQL database.
        
        Args:
            dataframe: The pandas DataFrame to load
            table_name: Name of the table to create/update (if not in destination config)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
                      (if not provided, uses destination config or defaults to 'replace')
            index: Whether to write DataFrame index as a column
            chunk_size: Number of rows to write at a time (None = all at once)
        
        Returns:
            bool: True if loading was successful, False otherwise
        """
        try:
            # Get table name from parameter or destination config
            table = table_name or self.destination.get("table")
            if not table:
                self.logger.error("Table name must be provided in parameter or destination config")
                return False
            
            # Get if_exists from parameter or destination config
            behavior = if_exists or self.destination.get("if_exists", "replace")
            
            if dataframe.empty:
                self.logger.warning(f"DataFrame is empty. No data will be loaded to table '{table}'")
                return False
            
            self.logger.info(f"Starting to load DataFrame to table '{table}' ({len(dataframe)} rows)")
            
            dataframe.to_sql(
                table,
                self.engine,
                if_exists=behavior,
                index=index,
                chunksize=chunk_size
            )
            
            self.logger.info(f"Successfully loaded {len(dataframe)} rows to table '{table}'")
            return True
        
        except SQLAlchemyError as e:
            self.logger.error(f"SQLAlchemy error while loading data: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while loading DataFrame: {str(e)}")
            return False
    
    def load_csv_to_database(
        self,
        csv_file_path: str,
        table_name: Optional[str] = None,
        if_exists: Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Load data from a CSV file to the SQL database.
        
        Args:
            csv_file_path: Path to the CSV file
            table_name: Name of the table to create/update (if not in destination config)
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            **kwargs: Additional arguments to pass to pd.read_csv()
        
        Returns:
            bool: True if loading was successful, False otherwise
        """
        try:
            self.logger.info(f"Reading CSV file: {csv_file_path}")
            dataframe = pd.read_csv(csv_file_path, **kwargs)
            self.logger.info(f"CSV file loaded successfully with {len(dataframe)} rows")
            
            return self.load_dataframe_to_sql(dataframe, table_name, if_exists)
        
        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {csv_file_path}")
            return False
        except pd.errors.ParserError as e:
            self.logger.error(f"Error parsing CSV file '{csv_file_path}': {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error while loading CSV file: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[dict]:
        """
        Get information about a table in the database.
        
        Args:
            table_name: Name of the table to inspect
        
        Returns:
            dict: Information about the table, or None if table doesn't exist
        """
        try:
            inspector = inspect(self.engine)
            
            if not inspector.has_table(table_name):
                self.logger.warning(f"Table '{table_name}' does not exist in the database")
                return None
            
            columns = inspector.get_columns(table_name)
            self.logger.info(f"Table '{table_name}' has {len(columns)} columns")
            
            return {
                "table_name": table_name,
                "columns": [col["name"] for col in columns],
                "column_count": len(columns)
            }
        
        except Exception as e:
            self.logger.error(f"Error inspecting table '{table_name}': {str(e)}")
            return None
    
    def list_tables(self) -> list:
        """
        List all tables in the database.
        
        Returns:
            list: Names of all tables in the database
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            self.logger.info(f"Database contains {len(tables)} table(s): {tables}")
            return tables
        except Exception as e:
            self.logger.error(f"Error listing tables: {str(e)}")
            return []
    
    def close(self) -> None:
        """Close the database connection."""
        try:
            if self.engine:
                self.engine.dispose()
                self.logger.info("Database connection closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {str(e)}")
