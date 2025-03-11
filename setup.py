from setuptools import setup, find_packages

setup(
    name="ff-mcp-clickhouse",
    version="0.1.0",
    description="MCP integration for ClickHouse",
    author="Illia",
    author_email="dp020891miv@gmail.com",
    packages=find_packages(),
    install_requires=[
        "mcp[cli]>=1.3.0",
        "python-dotenv>=1.0.1",
        "uvicorn>=0.34.0",
        "clickhouse-connect>=0.8.0",
        "pip-system-certs>=4.0",
    ],

)
