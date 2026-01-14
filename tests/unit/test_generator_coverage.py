"""
Coverage tests for Generators (Postgres, MySQL, SQLite, etc.)
Targeting specific logic paths in generate_migration and create_table_sql.
"""
import pytest
from schemaforge.models import (
    Schema, Table, Column, Index, ForeignKey, 
    CustomObject, CheckConstraint
)
from schemaforge.comparator import MigrationPlan, TableDiff

from schemaforge.generators.postgres import PostgresGenerator
from schemaforge.generators.mysql import MySQLGenerator
from schemaforge.generators.sqlite import SQLiteGenerator
from schemaforge.generators.oracle import OracleGenerator
from schemaforge.generators.db2 import DB2Generator
from schemaforge.generators.snowflake import SnowflakeGenerator

class TestPostgresGeneratorCoverage:
    def test_custom_objects(self):
        gen = PostgresGenerator()
        plan = MigrationPlan()
        plan.new_custom_objects.append(CustomObject('TYPE', 'status', properties={'raw_sql': "CREATE TYPE status AS ENUM ('active')" }))
        plan.new_custom_objects.append(CustomObject('VIEW', 'v_users')) # No raw_sql, comment only
        plan.dropped_custom_objects.append(CustomObject('FUNCTION', 'old_func'))
        plan.modified_custom_objects.append((CustomObject('VIEW', 'v1'), CustomObject('VIEW', 'v1', properties={'raw_sql': "CREATE OR REPLACE VIEW v1..."})))
        
        sql = gen.generate_migration(plan)
        # print(sql)
        assert "CREATE TYPE status" in sql
        assert "-- Create VIEW v_users" in sql
        assert "DROP FUNCTION old_func;" in sql
        assert "DROP VIEW v1;" in sql
        assert "CREATE OR REPLACE VIEW v1..." in sql

    def test_create_table_options(self):
        gen = PostgresGenerator()
        t = Table('logs')
        t.is_unlogged = True
        t.partition_by = "RANGE (created_at)"
        t.columns.append(Column('id', 'INT', is_primary_key=True))
        t.columns.append(Column('created_at', 'TIMESTAMP'))
        
        sql = gen.create_table_sql(t)
        assert "CREATE UNLOGGED TABLE" in sql
        assert "PARTITION BY RANGE (created_at)" in sql

    def test_create_index_options(self):
        gen = PostgresGenerator()
        idx = Index('idx_gist', ['data'], method='gist')
        sql = gen.create_index_sql(idx, 't')
        assert "USING gist" in sql

    def test_modified_table_columns(self):
        gen = PostgresGenerator()
        plan = MigrationPlan()
        diff = TableDiff('users')
        
        # Added column
        diff.added_columns.append(Column('new_col', 'INT'))
        
        # Dropped column
        diff.dropped_columns.append(Column('old_col', 'TEXT'))
        
        # Modified columns
        # 1. Type change
        c1_old = Column('c1', 'INT')
        c1_new = Column('c1', 'TEXT')
        diff.modified_columns.append((c1_old, c1_new))
        
        # 2. Nullability (SET NOT NULL)
        c2_old = Column('c2', 'INT', is_nullable=True)
        c2_new = Column('c2', 'INT', is_nullable=False)
        diff.modified_columns.append((c2_old, c2_new))
        
        # 3. Nullability (DROP NOT NULL)
        c3_old = Column('c3', 'INT', is_nullable=False)
        c3_new = Column('c3', 'INT', is_nullable=True)
        diff.modified_columns.append((c3_old, c3_new))
        
        # 4. Default Set
        c4_old = Column('c4', 'INT')
        c4_new = Column('c4', 'INT', default_value='0')
        diff.modified_columns.append((c4_old, c4_new))
        
        # 5. Default Drop
        c5_old = Column('c5', 'INT', default_value='1')
        c5_new = Column('c5', 'INT')
        diff.modified_columns.append((c5_old, c5_new))
        
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        
        assert "ADD COLUMN \"new_col\" INT" in sql
        assert "DROP COLUMN \"old_col\"" in sql
        assert "TYPE TEXT USING \"c1\"::TEXT" in sql
        assert "ALTER COLUMN \"c2\" SET NOT NULL" in sql
        assert "ALTER COLUMN \"c3\" DROP NOT NULL" in sql
        assert "ALTER COLUMN \"c4\" SET DEFAULT 0" in sql
        assert "ALTER COLUMN \"c5\" DROP DEFAULT" in sql

    def test_modified_table_constraints(self):
        gen = PostgresGenerator()
        plan = MigrationPlan()
        diff = TableDiff('orders')
        
        # FKs
        fk_old = ForeignKey('fk_1', ['u_id'], 'users', ['id'])
        diff.dropped_fks.append(fk_old)
        
        fk_new = ForeignKey('fk_2', ['p_id'], 'products', ['id'], on_delete='CASCADE', on_update='NO ACTION')
        diff.added_fks.append(fk_new)
        
        # Modified FK
        fk_mod_old = ForeignKey('fk_3', ['x'], 't', ['x'])
        fk_mod_new = ForeignKey('fk_3', ['x'], 't', ['x'], on_delete='SET NULL')
        diff.modified_fks.append((fk_mod_old, fk_mod_new))
        
        # Indexes
        idx_old = Index('idx_1', ['a'])
        diff.dropped_indexes.append(idx_old)
        
        idx_new = Index('idx_2', ['b'], is_unique=True)
        diff.added_indexes.append(idx_new)
        
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        
        assert "DROP CONSTRAINT \"fk_1\"" in sql
        assert "ADD CONSTRAINT \"fk_2\" FOREIGN KEY (\"p_id\") REFERENCES \"products\"(\"id\") ON DELETE CASCADE ON UPDATE NO ACTION" in sql
        assert "DROP CONSTRAINT \"fk_3\"" in sql # Modified means drop...
        assert "ADD CONSTRAINT \"fk_3\" ... ON DELETE SET NULL" # ...and add
        assert "DROP INDEX idx_1;" in sql
        assert "CREATE UNIQUE INDEX \"idx_2\"" in sql

class TestMySQLGeneratorCoverage:
    def test_mysql_features(self):
        gen = MySQLGenerator()
        
        # Fulltext index
        idx = Index('idx_ft', ['desc'], method='fulltext')
        sql = gen.create_index_sql(idx, 'articles')
        assert "FULLTEXT INDEX `idx_ft`" in sql
        
        # Partition
        t = Table('logs')
        t.partition_by = "HASH(year)"
        t.columns.append(Column('year', 'INT'))
        sql_create = gen.create_table_sql(t)
        assert "PARTITION BY HASH(year)" in sql_create

    def test_mysql_alters(self):
        gen = MySQLGenerator()
        plan = MigrationPlan()
        diff = TableDiff('users')
        
        # Modify column
        c_old = Column('email', 'VARCHAR(50)')
        c_new = Column('email', 'VARCHAR(100)', is_nullable=False)
        diff.modified_columns.append((c_old, c_new))
        
        # Drop Index
        diff.dropped_indexes.append(Index('idx_old', ['email']))
        
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        
        assert "MODIFY COLUMN `email` VARCHAR(100) NOT NULL" in sql
        assert "DROP INDEX idx_old ON users" in sql

class TestSQLiteGeneratorCoverage:
    def test_sqlite_alters(self):
        gen = SQLiteGenerator()
        plan = MigrationPlan()
        
        # SQLite ADD COLUMN
        diff = TableDiff('t')
        diff.added_columns.append(Column('c', 'INT'))
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        assert 'ALTER TABLE "t" ADD COLUMN "c" INT' in sql

    def test_sqlite_create(self):
        gen = SQLiteGenerator()
        t = Table('strict_t')
        t.is_strict = True # If supported?
        t.without_rowid = True
        t.columns.append(Column('id', 'INT', is_primary_key=True))
        
        sql = gen.create_table_sql(t)
        assert "WITHOUT ROWID" in sql

class TestOracleGeneratorCoverage:
    def test_oracle_options(self):
        gen = OracleGenerator()
        t = Table('t_ora')
        t.tablespace = 'USERS'
        t.columns.append(Column('id', 'NUMBER'))
        
        sql = gen.create_table_sql(t)
        assert "TABLESPACE USERS" in sql
        
        # Partition
        t.partition_by = "RANGE (id)"
        sql2 = gen.create_table_sql(t)
        assert "PARTITION BY RANGE (id)" in sql2

    def test_oracle_migration(self):
        gen = OracleGenerator()
        plan = MigrationPlan()
        diff = TableDiff('users')
        
        # Add column
        diff.added_columns.append(Column('email', 'VARCHAR2(100)'))
        # Drop column
        diff.dropped_columns.append(Column('old', 'INT'))
        # Modify column (type)
        diff.modified_columns.append((Column('c', 'CHAR(1)'), Column('c', 'CHAR(2)')))
        
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        assert "ADD \"email\" VARCHAR2(100)" in sql
        assert "DROP COLUMN \"old\"" in sql
        assert "MODIFY \"c\" CHAR(2)" in sql

class TestDB2GeneratorCoverage:
    def test_db2_options(self):
        gen = DB2Generator()
        t = Table('t_db2')
        t.partition_by = "RANGE (id)"
        t.columns.append(Column('id', 'INT'))
        
        sql = gen.create_table_sql(t)
        assert "PARTITION BY RANGE (id)" in sql
        
    def test_db2_migration(self):
        gen = DB2Generator()
        plan = MigrationPlan()
        diff = TableDiff('t')
        diff.added_columns.append(Column('c', 'INT'))
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        assert "ADD COLUMN \"c\" INT" in sql

class TestSnowflakeGeneratorCoverage:
    def test_snowflake_identity(self):
        gen = SnowflakeGenerator()
        t = Table('t_identity')
        c = Column('id', 'INT')
        c.is_identity = True
        c.identity_start = 1
        c.identity_step = 1
        t.columns.append(c)
        
        sql = gen.create_table_sql(t)
        assert "IDENTITY(1, 1)" in sql

    def test_snowflake_pk_change(self):
        gen = SnowflakeGenerator()
        plan = MigrationPlan()
        diff = TableDiff('users')
        
        # Scenario: PK status changed for a column
        c_old = Column('id', 'INT', is_primary_key=False)
        c_new = Column('id', 'INT', is_primary_key=True)
        diff.modified_columns.append((c_old, c_new))
        
        # Need new_table_obj to be set for the generator to retrieve PK name/cols for ADD logic
        new_table = Table('users')
        new_table.columns.append(c_new)
        new_table.primary_key_name = 'pk_users'
        diff.new_table_obj = new_table
        
        plan.modified_tables.append(diff)
        
        sql = gen.generate_migration(plan)
        assert "DROP PRIMARY KEY" in sql
        assert "ADD CONSTRAINT \"pk_users\" PRIMARY KEY (\"id\")" in sql


