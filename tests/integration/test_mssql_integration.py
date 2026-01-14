import pytest
from schemaforge.parsers.mssql import MSSQLParser
from schemaforge.generators.mssql import MSSQLGenerator
# SchemaCompare might be in comparator.py or similar
from schemaforge.comparator import Comparator
from schemaforge.models import Schema

class TestMSSQLIntegration:
    def test_full_cycle(self):
        sql = """
        CREATE TABLE [dbo].[Customers] (
            [CustomerID] INT IDENTITY(1,1) PRIMARY KEY,
            [FullName] NVARCHAR(200) NOT NULL,
            [CreditLimit] MONEY DEFAULT 0
        );
        CREATE CLUSTERED INDEX [IX_Customers_Name] ON [Customers] ([FullName]);
        GO
        CREATE TABLE [dbo].[Orders] (
            [OrderID] INT IDENTITY(1,1) PRIMARY KEY,
            [CustomerID] INT NOT NULL,
            [OrderDate] DATETIME2 DEFAULT GETDATE(),
            CONSTRAINT [FK_Orders_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Customers]([CustomerID])
        );
        """
        
        # 1. Parse
        parser = MSSQLParser()
        schema = parser.parse(sql)
        
        assert len(schema.tables) == 2
        cust = schema.get_table('dbo.customers')
        assert cust is not None
        assert len(cust.columns) == 3
        # Verify columns (sqlglot might include precision in type)
        # CreditLimit is MONEY in source sql
        assert any(c.name == 'customerid' and c.is_primary_key for c in cust.columns)
        assert any(c.name == 'creditlimit' and (c.data_type.startswith('DECIMAL') or c.data_type == 'MONEY') for c in cust.columns)
        
        # 2. Generate
        gen = MSSQLGenerator()
        ddl = gen.create_table(cust)
        assert "CREATE TABLE [dbo].[customers]" in ddl
        assert "[creditlimit] MONEY" in ddl or "[creditlimit] DECIMAL" in ddl
        
        # 3. Compare (No change)
        diff = Comparator().compare(schema, schema)
        assert not diff.new_tables
        assert not diff.dropped_tables
        assert not diff.modified_tables
        
    def test_migration_generation(self):
        # Scenario: Add column, change type
        sql_v1 = "CREATE TABLE [Users] ([Id] INT PRIMARY KEY, [Name] VARCHAR(50))"
        sql_v2 = "CREATE TABLE [Users] ([Id] INT PRIMARY KEY, [Name] NVARCHAR(100), [Email] VARCHAR(50))"
        
        parser = MSSQLParser()
        s1 = parser.parse(sql_v1)
        s2 = parser.parse(sql_v2)
        
        diff = Comparator().compare(s1, s2)
        assert len(diff.modified_tables) == 1
        
        gen = MSSQLGenerator()
        migration = gen.generate_migration(diff)
        
        # Verify T-SQL specific ALTER syntax
        # Expect: ALTER TABLE [Users] ADD [Email] VARCHAR(50) NULL;
        # Expect: ALTER TABLE [Users] ALTER COLUMN [Name] NVARCHAR(100) NULL;
        
        assert "ALTER TABLE [users] ADD [email] VARCHAR(50)" in migration
        assert "ALTER TABLE [users] ALTER COLUMN [name] NVARCHAR(100)" in migration
