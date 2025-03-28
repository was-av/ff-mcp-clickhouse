import unittest

from dotenv import load_dotenv
import pandas as pd

from mcp_clickhouse import create_clickhouse_client, run_select_query

load_dotenv()


class TestClickhouseTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the environment before tests."""
        cls.client = create_clickhouse_client()

        # Prepare test database and table
        cls.test_db = "test_tool_db"
        cls.test_table = "test_table"
        cls.client.command(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")

        # Drop table if exists to ensure clean state
        cls.client.command(f"DROP TABLE IF EXISTS {cls.test_db}.{cls.test_table}")

        # Create table with comments
        cls.client.command(f"""
            CREATE TABLE {cls.test_db}.{cls.test_table} (
                id UInt32 COMMENT 'Primary identifier',
                name String COMMENT 'User name field'
            ) ENGINE = MergeTree()
            ORDER BY id
            COMMENT 'Test table for unit testing'
        """)
        cls.client.command(f"""
            INSERT INTO {cls.test_db}.{cls.test_table} (id, name) VALUES (1, 'Alice'), (2, 'Bob')
        """)

    @classmethod
    def tearDownClass(cls):
        """Clean up the environment after tests."""
        cls.client.command(f"DROP DATABASE IF EXISTS {cls.test_db}")


    # @unittest.skip("in complete")
    def test_run_select_query_success(self):
        """Test running a SELECT query successfully."""
        query = f"SELECT * FROM {self.test_db}.{self.test_table}"
        result = run_select_query(query)
        # restore basic return type for test
        if isinstance(result, pd.DataFrame):
            result = result.to_dict(orient='records')

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")


    def test_run_select_query_failure(self):
        """Test running a SELECT query with an error."""
        query = f"SELECT * FROM {self.test_db}.non_existent_table"
        result = run_select_query(query)
        self.assertIsInstance(result, str)
        self.assertIn("error running query", result)


if __name__ == "__main__":
    unittest.main()
