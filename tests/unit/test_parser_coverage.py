"""
Tests for SQL parsers - increasing coverage.
"""
import pytest
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.oracle import OracleParser


class TestPostgresParserCoverage:
    """Additional PostgreSQL parser tests for coverage."""
    
    def test_create_table_with_constraints(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_id INT NOT NULL,
                total DECIMAL(10,2) CHECK (total >= 0),
                CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(id)
            );
        ''')
        
        assert len(schema.tables) == 1
        table = schema.tables[0]
        assert table.name == 'orders'
        
    def test_create_index_unique(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT, email VARCHAR(255));
            CREATE UNIQUE INDEX idx_email ON users(email);
        ''')
        
        table = schema.get_table('users')
        assert any(idx.is_unique for idx in table.indexes)
        
    def test_create_table_with_default(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE logs (
                id SERIAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level VARCHAR(10) DEFAULT 'INFO'
            );
        ''')
        
        table = schema.tables[0]
        assert len(table.columns) >= 2
        
    def test_comment_on_table(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT);
            COMMENT ON TABLE users IS 'User accounts';
        ''')
        
        table = schema.get_table('users')
        assert table is not None
        
    def test_comment_on_column(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT, email VARCHAR(255));
            COMMENT ON COLUMN users.email IS 'Primary email address';
        ''')
        
        table = schema.get_table('users')
        assert table is not None
        
    def test_partition_table(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE events (
                id INT,
                event_date DATE
            ) PARTITION BY RANGE (event_date);
        ''')
        
        assert len(schema.tables) == 1
        
    def test_unlogged_table(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE UNLOGGED TABLE temp_data (id INT);
        ''')
        
        table = schema.tables[0]
        assert table.is_unlogged == True
        
    def test_inherits(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE child_table (extra_col INT) INHERITS (parent_table);
        ''')
        
        table = schema.tables[0]
        assert 'parent_table' in (table.inherits or '')


class TestMySQLParserCoverage:
    """Additional MySQL parser tests for coverage."""
    
    def test_create_table_with_engine(self):
        parser = MySQLParser()
        schema = parser.parse('''
            CREATE TABLE users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100)
            ) ENGINE=InnoDB;
        ''')
        
        assert len(schema.tables) == 1
        
    def test_create_table_with_charset(self):
        parser = MySQLParser()
        schema = parser.parse('''
            CREATE TABLE messages (
                id INT,
                content TEXT
            ) DEFAULT CHARSET=utf8mb4;
        ''')
        
        assert len(schema.tables) == 1
        
    def test_create_index(self):
        parser = MySQLParser()
        schema = parser.parse('''
            CREATE TABLE products (id INT, sku VARCHAR(50));
            CREATE INDEX idx_sku ON products(sku);
        ''')
        
        table = schema.get_table('products') or schema.get_table('PRODUCTS')
        assert table is not None
        
    def test_fulltext_index(self):
        parser = MySQLParser()
        schema = parser.parse('''
            CREATE TABLE articles (id INT, content TEXT);
            CREATE FULLTEXT INDEX ft_content ON articles(content);
        ''')
        
        assert len(schema.tables) >= 1


class TestSQLiteParserCoverage:
    """Additional SQLite parser tests for coverage."""
    
    def test_create_table_without_rowid(self):
        parser = SQLiteParser()
        schema = parser.parse('''
            CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value BLOB
            ) WITHOUT ROWID;
        ''')
        
        table = schema.tables[0]
        assert table.without_rowid == True
        
    def test_create_table_strict(self):
        parser = SQLiteParser()
        schema = parser.parse('''
            CREATE TABLE strict_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            ) STRICT;
        ''')
        
        table = schema.tables[0]
        assert table.is_strict == True
        
    def test_create_index(self):
        parser = SQLiteParser()
        schema = parser.parse('''
            CREATE TABLE items (id INTEGER, name TEXT);
            CREATE INDEX idx_name ON items(name);
        ''')
        
        table = schema.get_table('items')
        assert table is not None


class TestOracleParserCoverage:
    """Additional Oracle parser tests for coverage."""
    
    def test_create_table_with_tablespace(self):
        parser = OracleParser()
        schema = parser.parse('''
            CREATE TABLE employees (
                id NUMBER PRIMARY KEY,
                name VARCHAR2(100)
            ) TABLESPACE users_ts;
        ''')
        
        assert len(schema.tables) == 1
        table = schema.tables[0]
        assert table.tablespace is not None
        
    def test_create_sequence(self):
        parser = OracleParser()
        schema = parser.parse('''
            CREATE TABLE test (id NUMBER);
            CREATE SEQUENCE test_seq START WITH 1;
        ''')
        
        # Should parse table at minimum
        assert len(schema.tables) >= 1
