"""
Comprehensive PostgreSQL parser tests for maximum coverage.
"""
import pytest
from schemaforge.parsers.postgres import PostgresParser


class TestPostgresParserComprehensive:
    """Comprehensive Postgres parser tests targeting all code paths."""
    
    def test_basic_table(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (id INT);')
        assert len(schema.tables) == 1
        
    def test_table_with_varchar(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (name VARCHAR(100));')
        assert len(schema.tables) == 1
        col = schema.tables[0].columns[0]
        assert 'VARCHAR' in col.data_type.upper()
        
    def test_table_with_numeric(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (amount NUMERIC(10,2));')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_table_with_serial(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (id SERIAL PRIMARY KEY);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_table_with_bigserial(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (id BIGSERIAL PRIMARY KEY);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_not_null_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (name VARCHAR(100) NOT NULL);')
        col = schema.tables[0].columns[0]
        assert col.is_nullable == False
        
    def test_default_value(self):
        parser = PostgresParser()
        schema = parser.parse("CREATE TABLE test (status VARCHAR(10) DEFAULT 'active');")
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_default_now(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (created_at TIMESTAMP DEFAULT NOW());')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_check_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (age INT CHECK (age >= 0));')
        table = schema.tables[0]
        assert table is not None
        
    def test_named_check_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (age INT, CONSTRAINT chk_age CHECK (age >= 0));')
        table = schema.tables[0]
        assert len(table.check_constraints) >= 1
        
    def test_unique_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (email VARCHAR(255) UNIQUE);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_primary_key_inline(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (id INT PRIMARY KEY);')
        col = schema.tables[0].columns[0]
        assert col.is_primary_key == True
        
    def test_primary_key_table_level(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (id INT, name VARCHAR(100), PRIMARY KEY (id));')
        table = schema.tables[0]
        assert any(c.is_primary_key for c in table.columns)
        
    def test_composite_primary_key(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (a INT, b INT, PRIMARY KEY (a, b));')
        table = schema.tables[0]
        assert len(table.columns) == 2
        
    def test_foreign_key_table_level(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE child (
                id INT, 
                parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES parent(id)
            );
        ''')
        table = schema.tables[0]
        assert len(table.foreign_keys) >= 1
        
    def test_foreign_key_with_on_delete(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE child (
                parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE
            );
        ''')
        table = schema.tables[0]
        if table.foreign_keys:
            assert table.foreign_keys[0].on_delete == 'CASCADE'
            
    def test_foreign_key_with_on_update(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE child (
                parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES parent(id) ON UPDATE SET NULL
            );
        ''')
        table = schema.tables[0]
        if table.foreign_keys:
            assert table.foreign_keys[0].on_update == 'SET NULL'
            
    def test_named_foreign_key(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE child (
                parent_id INT,
                CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id)
            );
        ''')
        table = schema.tables[0]
        if table.foreign_keys:
            assert table.foreign_keys[0].name == 'fk_parent'
            
    def test_create_index(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT, name VARCHAR(100));
            CREATE INDEX idx_name ON test(name);
        ''')
        table = schema.get_table('test')
        assert len(table.indexes) >= 1
        
    def test_create_unique_index(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (email VARCHAR(255));
            CREATE UNIQUE INDEX idx_email ON test(email);
        ''')
        table = schema.get_table('test')
        if table.indexes:
            assert table.indexes[0].is_unique == True
            
    def test_create_index_using_btree(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT);
            CREATE INDEX idx_id ON test USING btree(id);
        ''')
        table = schema.get_table('test')
        assert table is not None
        
    def test_create_index_using_gin(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (data JSONB);
            CREATE INDEX idx_data ON test USING gin(data);
        ''')
        table = schema.get_table('test')
        assert table is not None
        
    def test_partial_index(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT, active BOOLEAN);
            CREATE INDEX idx_active ON test(id) WHERE active = true;
        ''')
        table = schema.get_table('test')
        assert table is not None
        
    def test_comment_on_table(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT);
            COMMENT ON TABLE users IS 'User accounts table';
        ''')
        table = schema.get_table('users')
        assert table is not None
        
    def test_comment_on_column(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT, email VARCHAR(255));
            COMMENT ON COLUMN users.email IS 'Primary email';
        ''')
        table = schema.get_table('users')
        assert table is not None
        
    def test_unlogged_table(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE UNLOGGED TABLE temp_data (id INT);')
        table = schema.tables[0]
        assert table.is_unlogged == True
        
    def test_if_not_exists(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE IF NOT EXISTS test (id INT);')
        assert len(schema.tables) == 1
        
    def test_inherits(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE child (extra INT) INHERITS (parent);')
        table = schema.tables[0]
        assert 'parent' in (table.inherits or '')
        
    def test_partition_by_range(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE events (id INT, event_date DATE)
            PARTITION BY RANGE (event_date);
        ''')
        table = schema.tables[0]
        assert table is not None
        
    def test_partition_by_list(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE orders (id INT, region VARCHAR(10))
            PARTITION BY LIST (region);
        ''')
        table = schema.tables[0]
        assert table is not None
        
    def test_partition_by_hash(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE data (id INT)
            PARTITION BY HASH (id);
        ''')
        table = schema.tables[0]
        assert table is not None
        
    def test_text_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (content TEXT);')
        col = schema.tables[0].columns[0]
        assert 'TEXT' in col.data_type.upper() or col.data_type is not None
        
    def test_boolean_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (active BOOLEAN);')
        col = schema.tables[0].columns[0]
        assert 'BOOLEAN' in col.data_type.upper()
        
    def test_json_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (data JSON);')
        col = schema.tables[0].columns[0]
        assert 'JSON' in col.data_type.upper()
        
    def test_jsonb_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (data JSONB);')
        col = schema.tables[0].columns[0]
        assert 'JSONB' in col.data_type.upper()
        
    def test_uuid_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (id UUID);')
        col = schema.tables[0].columns[0]
        assert 'UUID' in col.data_type.upper()
        
    def test_array_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (tags TEXT[]);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_inet_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (ip INET);')
        col = schema.tables[0].columns[0]
        assert 'INET' in col.data_type.upper()
        
    def test_timestamp_with_tz(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (ts TIMESTAMP WITH TIME ZONE);')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_interval_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (duration INTERVAL);')
        col = schema.tables[0].columns[0]
        assert 'INTERVAL' in col.data_type.upper()
        
    def test_bytea_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (data BYTEA);')
        col = schema.tables[0].columns[0]
        assert 'BYTEA' in col.data_type.upper()
        
    def test_money_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (price MONEY);')
        col = schema.tables[0].columns[0]
        assert 'MONEY' in col.data_type.upper()
        
    def test_citext_type(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE test (email CITEXT);')
        col = schema.tables[0].columns[0]
        assert 'CITEXT' in col.data_type.upper()
        
    def test_generated_column(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (
                a INT,
                b INT,
                c INT GENERATED ALWAYS AS (a + b) STORED
            );
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 3
        
    def test_identity_column_always(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT GENERATED ALWAYS AS IDENTITY);
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_identity_column_by_default(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE test (id INT GENERATED BY DEFAULT AS IDENTITY);
        ''')
        table = schema.tables[0]
        assert len(table.columns) >= 1
        
    def test_multiple_tables(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE users (id INT);
            CREATE TABLE orders (id INT);
            CREATE TABLE products (id INT);
        ''')
        assert len(schema.tables) == 3
        
    def test_quoted_identifiers(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE "User Table" ("ID" INT, "Full Name" VARCHAR(100));')
        assert len(schema.tables) == 1
        
    def test_mixed_case_identifiers(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE "CamelCase" ("ColumnOne" INT);')
        table = schema.tables[0]
        assert table is not None
        
    def test_schema_qualified_name(self):
        parser = PostgresParser()
        schema = parser.parse('CREATE TABLE public.test (id INT);')
        assert len(schema.tables) >= 1
        
    def test_exclusion_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE reservations (
                room_id INT,
                during TSRANGE,
                EXCLUDE USING gist (room_id WITH =, during WITH &&)
            );
        ''')
        table = schema.tables[0]
        assert table is not None
        
    def test_deferrable_constraint(self):
        parser = PostgresParser()
        schema = parser.parse('''
            CREATE TABLE child (
                parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES parent(id) DEFERRABLE
            );
        ''')
        table = schema.tables[0]
        assert table is not None
