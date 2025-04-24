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

    sql = """
        SELECT
             database_name
           , database_description
        FROM assistant.databases
        """

    if not check_table_exists("assistant.databases"):
        sql = """
        SELECT 
            name as database_name
          , comment as database_description
        FROM system.databases
        """
    logger.info("Called tool: list_databases")
    return execute_query(sql).to_markdown(index=False, tablefmt="pipe")


def format_list_for_sql(values: list[str]) -> str:
    """
    Formats a list of strings for use in SQL IN clause.
    Each element is wrapped in single quotes.
    
    Args:
        values (list[str]): List of string values to format
        
    Returns:
        str: Comma-separated string of quoted values for SQL
    """
    return ', '.join([f"'{value}'" for value in values])


@mcp.tool(
    name="list_database_tables",
    description="List tables in specified database(s). (supports multiple databases)"
)
def list_database_tables(databases: list[str]):
    """
    List all tables in specified databases.

    Args:
        databases (list[str]): The list of database names.

    Returns:
        str: JSON string containing the list of tables and their descriptions.
    """
    logger.info(f"Called tool: list_database_tables with argument databases={databases}")

    databases_str = format_list_for_sql(databases)
    
    sql = f"""
        SELECT
             table_name
           , table_description
           , table_sorting_key
        FROM assistant.tables
        WHERE database_name IN ({databases_str})
        """
    if not check_table_exists("assistant.tables"):
        sql = f"""
        SELECT
             database || '.' || name as table_name
           , comment as table_description
           , sorting_key as table_sorting_key
        FROM system.tables
        WHERE database IN ({databases_str})
        """

    return execute_query(sql).to_markdown(index=False, tablefmt="pipe")


@mcp.tool(
    name="list_table_columns",
    description="List all columns in specified tables. (supports multiple tables)"
)
def list_table_columns(table_names_with_schema: list[str]):
    """
    List all columns in specified tables.

    Args:
        table_names_with_schema (list[str]): The names of the tables with schema.

    Returns:
        str: JSON string containing the list of columns and their descriptions.
    """

    logger.info(
        f"""Called tool: list_table_columns with argument
        table_names_with_schema={table_names_with_schema}"""
    )
    
    tables_str = format_list_for_sql(table_names_with_schema)
    
    sql = f"""
        SELECT
             column_name
           , column_type
           , column_description
        FROM assistant.columns
        WHERE table_name IN ({tables_str})
    """
    if not check_table_exists("assistant.columns"):
        sql = f"""
        SELECT
             name as column_name
           , type as column_type
           , comment as column_description
        FROM system.columns
        WHERE database || '.' || table IN ({tables_str})
        """
    return execute_query(sql).to_markdown(index=False, tablefmt="pipe")


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
    sql = f"""
        SELECT
              foreign_column_name
            , related_table_name
            , join_column_name
            , relationship
        FROM assistant.table_relations
        WHERE table_name =  {format_query_value(table_name_with_schema)}
        """
    if not check_table_exists("assistant.table_relations"):
        sql = """
        SELECT
              '' as foreign_column_name
            , '' as related_table_name
            , '' as join_column_name
            , '' as relationship
        """
    return execute_query(sql).to_markdown(index=False, tablefmt="pipe")


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
    sql = f"""
    -- MCP CLICKHOUSE QUERY 
    {query}
    -- END MCP CLICKHOUSE QUERY
    """
    try:
        return client.query_df(sql, settings={"readonly": 1})
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


def check_table_exists(table_name: str) -> bool:
    """
    Check if a table exists in the ClickHouse database.
    
    Args:
        table_name (str): Name of the table to check
        
    Returns:
        bool: True if the table exists, False otherwise
    """
    result = execute_query(f"exists {table_name}")
    return bool(result["result"][0])