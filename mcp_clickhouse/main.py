# mcp_clickhouse/main.py
from mcp_clickhouse.mcp_server import mcp


def main():
    mcp.run()


if __name__ == "__main__":
    main()
