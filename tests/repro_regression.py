import unittest
from schemaforge.parsers.snowflake import SnowflakeParser
from schemaforge.comparator import Comparator

class TestRegression(unittest.TestCase):
    def test_name_error(self):
        print("\n--- Test: Regression NameError 't' ---")
        # Scenario: Compare two simple tables (Add Default, Add Table, etc.)
        sql1 = "CREATE TABLE T1 (ID INT);"
        sql2 = "CREATE TABLE T1 (ID INT DEFAULT 0);"
        
        parser = SnowflakeParser()
        comparator = Comparator()
        
        s1 = parser.parse(sql1)
        s2 = parser.parse(sql2)
        
        # This triggered the error in the report
        plan = comparator.compare(s1, s2)
        
        print("Comparison successful")

if __name__ == '__main__':
    unittest.main()
