import logging
import platform
import socket
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from src.sources.base import FileLoadLog

logger = logging.getLogger(__name__)

from src.retry import retry


def get_runtime() -> str:
    """Determine the .NET runtime platform identifier.

    Returns platform identifier like 'linux-x64', 'linux-arm64', 'win-x64', 'osx-arm64', etc.
    """
    system = platform.system().lower()  # e.g., 'windows', 'linux', 'darwin'
    architecture = platform.machine().lower()  # e.g., 'x86_64', 'aarch64'

    # Based on https://github.com/pythonnet/pythonnet/discussions/2307
    # Mapping of system and architecture to custom platform names
    if system in ["windows", "windows_nt"] and architecture == "amd64":
        return "win-x64"
    elif system == "linux" and architecture in [
        "arm",
        "arm64",
        "aarch64_b",
        "aarch64",
        "armv8b",
        "armv8l",
    ]:
        return "linux-arm64"
    elif system == "darwin":
        return "osx-arm64"
    elif system == "linux":
        return "linux-x64"
    else:
        return "unknown"


def _ensure_clr_available():
    """Lazily import pythonnet/.NET components, raising a clear error if unavailable."""
    try:
        # Runtime should already be loaded in settings.py at startup
        import clr
        import System  # type: ignore[import-untyped]

        clr.AddReference("System.Data.Common")
        DataTable = System.Data.DataTable
        DBNull = System.DBNull

        runtime = get_runtime()
        dll_dir = (
            Path(__file__).parent.parent / "src" / "net-runtime-specific" / runtime
        )

        if not dll_dir.exists():
            raise ImportError(
                f"Platform-specific DLL folder not found: {dll_dir}. "
                f"Runtime: {runtime}, System: {platform.system()}, Architecture: {platform.machine()}"
            )

        sys.path.insert(0, str(dll_dir))
        clr.AddReference("Microsoft.Data.SqlClient")

        from Microsoft.Data.SqlClient import (  # type: ignore[import-untyped]
            SqlBulkCopy,
            SqlConnection,
            SqlConnectionStringBuilder,
        )

        return DataTable, DBNull, SqlBulkCopy, SqlConnection, SqlConnectionStringBuilder
    except ImportError as e:
        logger.exception(f"Failed to import .NET components: {e}")
        raise ImportError(
            f".NET Framework is required for SQL Server bulk operations. "
            f"Error: {e}. Ensure .NET Framework is available."
        ) from e
    except (SystemError, RuntimeError) as e:
        logger.exception(f"Failed to initialize .NET runtime: {e}")
        raise


def dicts_to_datatable(data: list[dict], table_name: str):
    """Convert list of dicts to .NET DataTable."""
    DataTable, DBNull, _, _, _ = _ensure_clr_available()
    import System  # type: ignore[import-untyped]

    dt = DataTable()

    column_types = {
        "etl_row_hash": System.Array[System.Byte],
        "file_load_log_id": System.Int64,
        "file_row_number": System.Int32,
    }

    for col in data[0].keys():
        if col in column_types:
            column = System.Data.DataColumn(col, column_types[col])
            dt.Columns.Add(column)
        else:
            dt.Columns.Add(col)

    for row in data:
        dr = dt.NewRow()
        for key, value in row.items():
            if value is None:
                dr[key] = DBNull.Value
            elif key in column_types:
                col_type = column_types[key]
                if col_type == System.Array[System.Byte]:
                    # etl_row_hash: convert Python bytes to .NET byte array
                    dr[key] = System.Array[System.Byte](value)
                elif col_type == System.Int64:
                    dr[key] = System.Int64(value)
                elif col_type == System.Int32:
                    dr[key] = System.Int32(value)
                else:
                    dr[key] = value
            else:
                dr[key] = value
        dt.Rows.Add(dr)
    dt.TableName = table_name
    return dt


def _convert_sqlalchemy_to_dotnet_connection_string(
    sqlalchemy_url: str,
) -> tuple[str, object]:
    """Convert SQLAlchemy connection string to .NET SqlConnection format using SqlConnectionStringBuilder.

    Returns: (connection_string, builder) tuple so we can access builder properties for logging
    """
    _, _, _, _, SqlConnectionStringBuilder = _ensure_clr_available()

    url = sqlalchemy_url.replace("mssql+pyodbc://", "mssql://")
    parsed = urlparse(url)

    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or ""
    port = parsed.port or 1433
    database = parsed.path.lstrip("/") if parsed.path else ""

    # Parse query parameters
    query_params = parse_qs(parsed.query)
    trust_cert = query_params.get("TrustServerCertificate", ["no"])[0].lower() == "yes"

    # Resolve hostname to IP to avoid registry alias lookups on Linux
    # Keep original hostname for certificate validation
    hostname_for_cert = host
    try:
        ip_address = socket.gethostbyname(host)
        server_address = f"{ip_address},{port}"
    except (socket.gaierror, OSError) as e:
        logger.exception(f"Failed to resolve hostname '{host}': {e}")
        server_address = f"{host},{port}"

    builder = SqlConnectionStringBuilder()
    builder.DataSource = server_address
    builder.InitialCatalog = database
    builder.UserID = username
    builder.Password = password
    builder["Encrypt"] = "True"
    builder.Pooling = False
    builder.ConnectTimeout = 30

    if trust_cert:
        builder["TrustServerCertificate"] = "True"
        # Set HostNameInCertificate to original hostname to avoid certificate name mismatch
        # when connecting via IP address
        builder["HostNameInCertificate"] = hostname_for_cert

    return builder.ConnectionString, builder


@retry()
def bulk_insert(
    connection_string: str, table_name: str, data_list: list[dict], log: FileLoadLog
) -> None:
    """Bulk insert data into SQL Server using SqlBulkCopy."""
    _, _, SqlBulkCopy, SqlConnection, _ = _ensure_clr_available()

    dotnet_conn_string, _ = _convert_sqlalchemy_to_dotnet_connection_string(
        connection_string
    )

    dt = dicts_to_datatable(data_list, table_name)
    conn = SqlConnection(dotnet_conn_string)
    try:
        conn.Open()
        bulk_copy = SqlBulkCopy(conn)
        bulk_copy.DestinationTableName = table_name

        # Map columns by name to avoid position-based mapping issues
        for col in dt.Columns:
            bulk_copy.ColumnMappings.Add(col.ColumnName, col.ColumnName)

        bulk_copy.WriteToServer(dt)
        logger.debug(
            f"[log_id={log.id}] SqlBulkCopy inserted {len(data_list)} records into {table_name}"
        )
    except Exception as e:
        logger.exception(
            f"[log_id={log.id}] Failed to SqlBulkCopy insert into {table_name}: {e}"
        )
        raise
    finally:
        try:
            if conn.State == 1:  # ConnectionState.Open
                conn.Close()
            conn.Dispose()
        except Exception as cleanup_error:
            logger.warning(
                f"[log_id={log.id}] Error during connection cleanup: {cleanup_error}"
            )
