# agent-connector-platform

## Setup

Install Python dependencies:

```bash
pip install -r requirements.txt
```

If you only need SQL Server connectivity on macOS without ODBC drivers, the project uses `pymssql` via SQLAlchemy (connection URL format: `mssql+pymssql://user:pass@host:port/db`).