import unittest

from mcp_clickhouse.mcp_server import check_table_exists


class TestTableExists(unittest.TestCase):
    """Test cases for the check_table_exists function."""

    def test_existing_table(self):
        """Test checking for an existing table."""
        result = check_table_exists("system.tables")
        print('********', result)
        self.assertTrue(result)

    def test_non_existing_table(self):
        """Test checking for a non-existing table."""   
        result = check_table_exists("system.non_existing_table")
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main() 