# mcp_clickhouse/mcp_server.py
"""
This module provides a server for interacting with ClickHouse using the FastMCP framework.
It includes tools for listing databases, tables, and columns, as well as executing SELECT queries.
"""

import logging
import concurrent.futures
import atexit

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.binding import format_query_value
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from functools import lru_cache

from mcp_clickhouse.mcp_env import config


pd.set_option('display.max_columns', 25)


MCP_SERVER_NAME = "mcp-clickhouse"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(MCP_SERVER_NAME)

# Thread pool executor for running queries
QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=10)
atexit.register(lambda: QUERY_EXECUTOR.shutdown(wait=True))
SELECT_QUERY_TIMEOUT_SECS = 30

# Load environment variables from .env file
load_dotenv()

# List of dependencies required by the server
deps = [
    "clickhouse-connect",
    "python-dotenv",
    "uvicorn",
    "pip-system-certs",
]

# Initialize FastMCP server
mcp = FastMCP(MCP_SERVER_NAME, dependencies=deps)


@mcp.tool(
    name="list_databases",
    description="List all databases in the ClickHouse server.",
)
def list_databases():
    """
    List all databases in the ClickHouse server.

    Returns:
        str: JSON string containing the list of databases and their descriptions.
    """
    logger.info("Called tool: list_databases")
    return execute_query("""
        SELECT
             database_name
           , database_description
        FROM assistant.databases
        """
    ).to_markdown(index=False, tablefmt="pipe")


@mcp.tool(
    name="list_database_tables",
    description="List all tables in a specified database."
)
def list_database_tables(database: str):
    """
    List all tables in a specified database.

    Args:
        database (str): The name of the database.

    Returns:
        str: JSON string containing the list of tables and their descriptions.
    """
    logger.info(f"Called tool: list_database_tables with argument database={database}")
    return execute_query(f"""
        SELECT
             table_name
           , table_description
           , table_sorting_key
        FROM assistant.tables
        WHERE database_name = {format_query_value(database)}
        """
    ).to_markdown(index=False, tablefmt="pipe")


@mcp.tool(
    name="list_table_columns",
    description="List all columns in a specified table."
)
def list_table_columns(table_name_with_schema: str):
    """
    List all columns in a specified table.

    Args:
        table_name_with_schema (str): The name of the table with schema.

    Returns:
        str: JSON string containing the list of columns and their descriptions.
    """

    logger.info(
        f"""Called tool: list_table_columns with argument
        table_name_with_schema={table_name_with_schema}"""
    )

    return execute_query(f"""
        SELECT
             column_name
           , column_type
           , column_description
        FROM assistant.columns
        WHERE table_name = {format_query_value(table_name_with_schema)}
        """
    ).to_markdown(index=False, tablefmt="pipe")


@mcp.tool(
    name="get_table_relationships",
    description="Displays the relationship structure for the specified table, including foreign keys, dependencies, and relationships with other tables in the data schema. Allows quick visualization of the data model for optimal query construction."
)
def get_table_relationships(table_name_with_schema: str):
    """
    List all relations in a specified table.

    Args:
        table_name_with_schema (str): The name of the table with schema.

    Returns:
        str: JSON string containing the list of relations and their relationship.
    """

    logger.info(
        f"""Called tool: get_table_relationships with argument
        table_name_with_schema={table_name_with_schema}"""
    )

    return execute_query(f"""
        SELECT
            foreign_column_name
            , related_table_name
            , join_column_name
            , relationship
        FROM assistant.table_relations
        WHERE table_name =  {format_query_value(table_name_with_schema)}
        """
    ).to_markdown(index=False, tablefmt="pipe")


@lru_cache(maxsize=128)
def execute_query(query: str):
    """
    Execute a query on the ClickHouse server.

    Args:
        query (str): The SQL query to execute.

    Returns:
        str: JSON string containing the query results.
    """
    client = create_clickhouse_client()
    try:
        return client.query_df(query, settings={"readonly": 1})
    except Exception as err:
        logger.error(f"Error executing query: {err}")
        return f"error running query: {err}"


@mcp.tool(
    name="run_select_query",
    description="Run a SELECT query asynchronously with a timeout."
)
def run_select_query(query: str):
    """
    Run a SELECT query asynchronously with a timeout.

    Args:
        query (str): The SQL query to execute.

    Returns:
        str: JSON string containing the query results or an error message if the query times out.
    """
    logger.info(f"Executing SELECT query: {query}")
    future = QUERY_EXECUTOR.submit(execute_query, query)
    try:
        result = future.result(timeout=SELECT_QUERY_TIMEOUT_SECS)
        return result
    except concurrent.futures.TimeoutError:
        logger.warning(f"Query timed out after {SELECT_QUERY_TIMEOUT_SECS} seconds: {query}")
        future.cancel()
        return f"Queries taking longer than {SELECT_QUERY_TIMEOUT_SECS} seconds are currently not supported."


def create_clickhouse_client():
    """
    Create a ClickHouse client using the configuration from the environment.

    Returns:
        clickhouse_connect.Client: The ClickHouse client.

    Raises:
        Exception: If the connection to ClickHouse fails.
    """
    client_config = config.get_client_config()
    logger.info(
        f"Creating ClickHouse client connection to {client_config['host']}:{client_config['port']} "
        f"as {client_config['username']} "
        f"(secure={client_config['secure']}, verify={client_config['verify']}, "
        f"connect_timeout={client_config['connect_timeout']}s, "
        f"send_receive_timeout={client_config['send_receive_timeout']}s)"
    )

    try:
        client = clickhouse_connect.get_client(
            compression=True,
            **client_config
        )
        # Test the connection
        version = client.server_version
        logger.info(f"Successfully connected to ClickHouse server version {version}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {str(e)}")
        raise
