import pytest
from schemaforge.parsers.mssql import MSSQLParser
from schemaforge.generators.mssql import MSSQLGenerator
from schemaforge.models import Table, Column

class TestMSSQLParser:
    def test_basic_table_parsing(self):
        sql = """
        CREATE TABLE [dbo].[Users] (
            [UserID] INT IDENTITY(1,1) PRIMARY KEY,
            [UserName] NVARCHAR(100) NOT NULL,
            [Email] VARCHAR(255) NULL
        );
        """
        parser = MSSQLParser()
        schema = parser.parse(sql)
        
        # Expect unquoted name
        assert len(schema.tables) == 1
        table = schema.get_table('dbo.Users')
        
        assert len(table.columns) == 3
        # ID
        col1 = table.columns[0]
        assert col1.name == 'UserID'
        assert col1.data_type == 'INT'
        assert col1.is_primary_key is True
        
        # UserName
        col2 = table.columns[1]
        assert col2.name == 'UserName' 
        assert col2.data_type == 'NVARCHAR(100)'
        assert col2.is_nullable is False
        
        # Email
        col3 = table.columns[2]
        assert col3.name == 'Email'
        assert col3.data_type == 'VARCHAR(255)'
        assert col3.is_nullable is True  # Explicit NULL

    def test_types_and_go(self):
        sql = """
        CREATE TABLE [data] (
            [val] NVARCHAR(MAX),
            [ts] DATETIME2
        )
        GO
        CREATE TABLE [metadata] (
            [id] BIGINT
        )
        """
        parser = MSSQLParser()
        schema = parser.parse(sql)
        
        assert len(schema.tables) == 2
        
        t1 = schema.get_table('data') # Unquoted
        assert t1.columns[0].data_type == 'TEXT' # Mapped from NVARCHAR(MAX)
        assert t1.columns[1].data_type == 'DATETIME' # Mapped from DATETIME2

    def test_identity_not_pk(self):
        sql = "CREATE TABLE [Log] ([Id] INT IDENTITY(10,1), [Msg] VARCHAR(MAX))"
        parser = MSSQLParser()
        schema = parser.parse(sql)
        
        t = schema.get_table('Log')
        assert t.columns[0].name == 'Id'
        # We don't have explicit identity property in model yet, but ensure it parses without error
        assert t.columns[0].data_type == 'INT'
    def test_advanced_features(self):
        sql = """
        CREATE TABLE [Sales] (
            [OrderID] INT IDENTITY(1,1) PRIMARY KEY,
            [LineTotal] AS ([UnitPrice] * [Qty]),
            [Info] XML,
            [Node] HIERARCHYID,
            [Qty] INT CHECK ([Qty] > 0)
        );
        CREATE CLUSTERED INDEX [IX_Sales_Date] ON [Sales] ([OrderDate]);
        """
        parser = MSSQLParser()
        schema = parser.parse(sql)
        
        table = schema.get_table('Sales')
        assert len(table.columns) == 5
        
        # Computed column
        col2 = table.columns[1]
        assert col2.name == 'LineTotal'
        # Ideally we capture that it is computed, but for now just ensure it parses
        
        # XML check
        col3 = table.columns[2]
        assert col3.data_type == 'XML'
        
        # HierarchyID
        col4 = table.columns[3]
        assert col4.data_type == 'HIERARCHYID'
        
        # Index check
        assert len(table.indexes) == 1
        idx = table.indexes[0]
        assert idx.name == 'IX_Sales_Date'

class TestMSSQLGenerator:
    def test_create_table(self):
        t = Table('Users')
        t.columns.append(Column('Id', 'INT', is_primary_key=True, is_nullable=False))
        t.columns.append(Column('Name', 'NVARCHAR(50)', is_nullable=False))
        
        gen = MSSQLGenerator()
        sql = gen.create_table(t)
        
        assert "CREATE TABLE [Users]" in sql
        assert "[Id] INT NOT NULL" in sql
        assert "CONSTRAINT [PK_Users] PRIMARY KEY ([Id])" in sql

    def test_alter_column(self):
        gen = MSSQLGenerator()
        sql = gen.alter_column('Users', 'Email', 'VARCHAR(200)', new_nullability=True)
        assert sql == "ALTER TABLE [Users] ALTER COLUMN [Email] VARCHAR(200) NULL;"

    def test_rename(self):
        gen = MSSQLGenerator()
        sql = gen.rename_table('Old', 'New')
        assert sql == "EXEC sp_rename 'Old', 'New';"
