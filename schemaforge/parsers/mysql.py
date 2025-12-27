from schemaforge.parsers.generic_sql import GenericSQLParser

from schemaforge.models import Table, CustomObject, Schema, Column, Index
from schemaforge.parsers.utils import normalize_sql
import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, DML, DDL, Name
import re

class MySQLParser(GenericSQLParser):
    def parse(self, sql_content):
        self.schema = Schema()
        sql_content = self._strip_comments(sql_content)
        parsed = sqlparse.parse(sql_content)
        
        for statement in parsed:
            if statement.get_type() in ('CREATE', 'CREATE OR REPLACE'):
                self._process_create(statement)
            elif statement.get_type() == 'UNKNOWN':
                first_token = statement.token_first()
                if first_token and first_token.match(DDL, 'CREATE'):
                    self._process_create(statement)
                    
        return self.schema

    def _process_create(self, statement):
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        stmt_str = str(statement).upper()
        
        # Check for Custom Objects
        for kw in ('PROCEDURE', 'FUNCTION', 'TRIGGER', 'VIEW'):
            if re.search(rf'CREATE\s+(?:OR\s+REPLACE\s+)?{kw}\s+', stmt_str):
                # Extract name
                match = re.search(rf'CREATE\s+(?:OR\s+REPLACE\s+)?{kw}\s+([a-zA-Z0-9_`]+)', stmt_str)
                obj_name = "unknown"
                if match:
                    obj_name = self._clean_name(match.group(1))
                
                self.schema.custom_objects.append(CustomObject(
                    obj_type=kw,
                    name=obj_name,
                    properties={'raw_sql': normalize_sql(str(statement))}
                ))
                return

        # Check for INDEX (MySQL allows CREATE INDEX, but usually it's inside CREATE TABLE or ALTER TABLE)
        # But we support CREATE INDEX statements too.
        if 'INDEX' in stmt_str and 'ON' in stmt_str:
            self._process_mysql_index(statement)
            return

        # Check for TABLE
        if 'TABLE' in stmt_str:
            self._process_mysql_table(statement)
            return

    def _process_mysql_table(self, statement):
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        stmt_str = str(statement).upper()
        
        table_name = None
        
        # Extract table name
        match = re.search(r'TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_`]+)', stmt_str)
        if match:
            table_name = self._clean_name(match.group(1))
            
        if not table_name:
            return
            
        table = Table(name=table_name)
        
        # Columns
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                self._parse_columns_and_constraints(token, table)
                break
                
        # Partitioning
        match_part = re.search(r'PARTITION\s+BY\s+(.*?)(?:$|;)', stmt_str, re.IGNORECASE | re.DOTALL)
        if match_part:
            table.partition_by = match_part.group(1).strip()
            
        self.schema.add_table(table)

    def _process_mysql_index(self, statement):
        # CREATE [UNIQUE|FULLTEXT] INDEX name ON table (cols)
        stmt_str = str(statement).upper()
        
        # Extract name
        match_name = re.search(r'INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_`]+)', stmt_str)
        if not match_name: return
        idx_name = self._clean_name(match_name.group(1))
        
        # Extract table
        match_table = re.search(r'ON\s+([a-zA-Z0-9_`]+)', stmt_str)
        if not match_table: return
        table_name = self._clean_name(match_table.group(1))
        
        # Extract method/type
        method = 'btree' # default
        if 'FULLTEXT' in stmt_str:
            method = 'fulltext'
            
        # Extract columns
        match_cols = re.search(r'\((.*?)\)', stmt_str)
        if not match_cols: return
        cols = [self._clean_name(c.strip()) for c in match_cols.group(1).split(',')]
        
        is_unique = 'UNIQUE' in stmt_str
        
        idx = Index(name=idx_name, columns=cols, is_unique=is_unique, method=method)
        
        # Find table and attach
        table = self.schema.get_table(table_name)
        if table:
            table.indexes.append(idx)
            
    def _clean_name(self, name):
        return name.strip('`" ')
