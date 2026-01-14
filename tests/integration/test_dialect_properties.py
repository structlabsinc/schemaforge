import pytest
from schemaforge.parsers.oracle import OracleParser
from schemaforge.parsers.db2 import DB2Parser
from schemaforge.parsers.mssql import MSSQLParser

def test_oracle_storage_parameters():
    """Oracle storage parameters: PCTFREE, STORAGE, IOT, TABLESPACE"""
    sql = """
    CREATE TABLE employees (
        id NUMBER PRIMARY KEY
    ) 
    ORGANIZATION INDEX
    PCTFREE 10
    STORAGE (INITIAL 64K NEXT 1M)
    TABLESPACE users;
    """
    parser = OracleParser()
    schema = parser.parse(sql)
    
    table = schema.get_table('employees')
    assert table.storage_parameters.get('pctfree') == 10
    assert 'INITIAL 64K' in table.storage_parameters.get('storage')
    assert table.storage_parameters.get('iot') is True
    assert table.tablespace == 'users'

def test_db2_zos_properties():
    """DB2 z/OS properties: STOGROUP, PRIQTY, SECQTY, CCSID, AUDIT, IN DATABASE"""
    sql = """
    CREATE TABLE my_table (
        id INT PRIMARY KEY
    )
    IN DATABASE my_db.my_ts
    USING STOGROUP my_sg
    PRIQTY 100
    SECQTY 50
    ERASE NO
    AUDIT CHANGES
    CCSID UNICODE;
    """
    parser = DB2Parser()
    schema = parser.parse(sql)
    
    table = schema.get_table('my_table')
    assert table.database_name == 'my_db'
    assert table.tablespace == 'my_ts'
    assert table.stogroup == 'my_sg'
    assert table.priqty == 100
    assert table.secqty == 50
    assert table.audit == 'changes'
    assert table.ccsid == 'unicode'


def test_mssql_clustered_pk():
    """MSSQL PRIMARY KEY CLUSTERED handling (should not fail parse)"""
    sql = "CREATE TABLE t1 (id INT PRIMARY KEY CLUSTERED);"
    parser = MSSQLParser()
    schema = parser.parse(sql)
    
    table = schema.get_table('t1')
    assert table is not None
    assert table.columns[0].is_primary_key is True

    sql2 = "CREATE TABLE t2 (id INT, CONSTRAINT pk_t2 PRIMARY KEY CLUSTERED (id));"
    schema2 = parser.parse(sql2)
    table2 = schema2.get_table('t2')
    assert table2 is not None
    assert table2.columns[0].is_primary_key is True
