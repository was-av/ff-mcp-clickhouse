from mcp_clickhouse.mcp_server import execute_query



print(execute_query("""
    SELECT 
         database_name
       , database_description 
    FROM assistant.databases
    """
))
