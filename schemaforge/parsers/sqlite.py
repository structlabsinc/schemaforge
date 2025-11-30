from schemaforge.parsers.generic_sql import GenericSQLParser

from schemaforge.models import Table, CustomObject, Schema, Column, Index
import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, DML, DDL, Name
import re

class SQLiteParser(GenericSQLParser):
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
        for kw in ('TRIGGER', 'VIEW', 'VIRTUAL TABLE'):
            if re.search(rf'CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?{kw}\s+', stmt_str):
                # Extract name
                match = re.search(rf'CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?{kw}\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_"]+)', stmt_str)
                obj_name = "unknown"
                if match:
                    obj_name = self._clean_name(match.group(1))
                
                self.schema.custom_objects.append(CustomObject(
                    obj_type=kw,
                    name=obj_name,
                    properties={'raw_sql': str(statement).strip()}
                ))
                return

        # Check for INDEX
        if 'INDEX' in stmt_str and 'ON' in stmt_str:
            self._process_sqlite_index(statement)
            return

        # Check for TABLE
        if 'TABLE' in stmt_str:
            self._process_sqlite_table(statement)
            return

    def _process_sqlite_table(self, statement):
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        stmt_str = str(statement).upper()
        
        table_name = None
        
        # Extract table name
        match = re.search(r'TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_"]+)', stmt_str)
        if match:
            table_name = self._clean_name(match.group(1))
            
        if not table_name:
            return
            
        table = Table(name=table_name)
        
        # Check for STRICT and WITHOUT ROWID
        if 'STRICT' in stmt_str:
            table.is_strict = True
        if 'WITHOUT ROWID' in stmt_str:
            table.without_rowid = True
        
        # Columns
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                self._parse_columns_and_constraints(token, table)
                break
                
        self.schema.tables.append(table)

    def _process_sqlite_index(self, statement):
        # CREATE [UNIQUE] INDEX name ON table (cols)
        stmt_str = str(statement).upper()
        
        # Extract name
        match_name = re.search(r'INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_"]+)', stmt_str)
        if not match_name: return
        idx_name = self._clean_name(match_name.group(1))
        
        # Extract table
        match_table = re.search(r'ON\s+([a-zA-Z0-9_"]+)', stmt_str)
        if not match_table: return
        table_name = self._clean_name(match_table.group(1))
        
        # Extract columns
        match_cols = re.search(r'\((.*?)\)', stmt_str)
        if not match_cols: return
        cols = [self._clean_name(c.strip()) for c in match_cols.group(1).split(',')]
        
        is_unique = 'UNIQUE' in stmt_str
        
        idx = Index(name=idx_name, columns=cols, is_unique=is_unique)
        
        # Find table and attach
        table = self.schema.get_table(table_name)
        if table:
            table.indexes.append(idx)
