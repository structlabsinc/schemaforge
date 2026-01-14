import pytest
from schemaforge.parsers.sqlglot_adapter import SqlglotParser
from schemaforge.models import Table, Column

class TestSqlglotPoc:
    def test_basic_create_table(self):
        sql = """
        CREATE TABLE users (
            id INT PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email VARCHAR(100)
        );
        """
        parser = SqlglotParser()
        schema = parser.parse(sql)
        
        assert len(schema.tables) == 1
        table = schema.tables[0]
        assert table.name == "users"
        assert len(table.columns) == 3
        
        # Check columns
        col_id = table.get_column("id")
        assert col_id.data_type == "INT"
        assert col_id.is_primary_key is True
        
        col_user = table.get_column("username")
        assert col_user.data_type == "VARCHAR(50)"
        assert col_user.is_nullable is False
        
        col_email = table.get_column("email")
        assert col_email.data_type == "VARCHAR(100)"
        assert col_email.is_nullable is True

    def test_transpilation_types(self):
        # This tests if sqlglot parses types correctly even if they are dialect specific
        # For this generic adapter we just check if it extracts the type string representation
        sql = "CREATE TABLE products (id SERIAL, name TEXT, price MONEY(10,2));"
        parser = SqlglotParser(dialect='postgres')
        schema = parser.parse(sql)
        tables = schema.tables
        
        col_price = schema.get_table("products").get_column("price")
        # should exist
        assert col_price is not None
        assert col_price.data_type == "MONEY(10, 2)"

        col_id = schema.get_table("products").get_column("id")
        assert col_id.data_type == "SERIAL"

        col_name = schema.get_table("products").get_column("name")
        assert col_name.data_type == "TEXT"

        if parser.dialect == 'tsql':
             # T-SQL types
             assert "INT" in str(col_id.data_type).upper() or "INTEGER" in str(col_id.data_type).upper()
             assert "VARCHAR" in str(col_name.data_type).upper()
             assert "DECIMAL" in str(col_price.data_type).upper()

