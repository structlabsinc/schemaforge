from schemaforge.parsers.generic_sql import GenericSQLParser

from schemaforge.models import Table, CustomObject, Schema, Column
from schemaforge.parsers.utils import normalize_sql
import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, DML, DDL, Name
import re

class OracleParser(GenericSQLParser):
    def parse(self, sql_content):
        self.schema = Schema()
        # Oracle often uses / as delimiter for PL/SQL
        # We might need custom splitting if sqlparse fails on /
        # But for now let's try standard parse
        
        # Pre-process: Remove / on its own line if it's just a delimiter
        # Actually sqlparse might treat / as an operator or error.
        # Let's strip it for now or handle it.
        
        sql_content = self._strip_comments(sql_content)
        parsed = sqlparse.parse(sql_content)
        
        for statement in parsed:
            if statement.get_type() in ('CREATE', 'CREATE OR REPLACE'):
                self._process_create(statement)
            elif statement.get_type() == 'UNKNOWN':
                first_token = statement.token_first()
                if first_token and first_token.match(DDL, 'CREATE'):
                    self._process_create(statement)
                elif first_token and first_token.value.upper() == 'CREATE':
                    self._process_create(statement)
                    
        return self.schema

    def _process_create(self, statement):
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        
        obj_type = None
        obj_name = None
        
        # Check for PL/SQL objects first
        stmt_str = str(statement).upper()
        
        for kw in ('FUNCTION', 'PROCEDURE', 'PACKAGE BODY', 'PACKAGE', 'TRIGGER', 'SEQUENCE', 'SYNONYM'):
            # Check if CREATE [OR REPLACE] KW ...
            # Regex is safer for multi-token keywords
            # For PACKAGE BODY, we need to be careful not to match PACKAGE if PACKAGE BODY is present
            if re.search(rf'CREATE\s+(?:OR\s+REPLACE\s+)?{kw}\s+', stmt_str):
                obj_type = kw
                # Extract name
                match = re.search(rf'CREATE\s+(?:OR\s+REPLACE\s+)?{kw}\s+([a-zA-Z0-9_"]+)', stmt_str)
                if match:
                    obj_name = self._clean_name(match.group(1))
                break
                
        if obj_type:
            self.schema.custom_objects.append(CustomObject(
                obj_type=obj_type,
                name=obj_name,
                properties={'raw_sql': normalize_sql(str(statement))}
            ))
            return

        # Check for TABLE
        for i, token in enumerate(tokens):
            if token.value.upper() == 'TABLE':
                self._process_oracle_table(statement)
                return

    def _process_oracle_table(self, statement):
        # Manual parsing for table name and partitioning
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        
        table_name = None
        for i, token in enumerate(tokens):
            if token.value.upper() == 'TABLE':
                if i + 1 < len(tokens):
                    table_name = self._clean_name(tokens[i+1].value)
                break
                
        if not table_name:
            return
            
        table = Table(name=table_name)
        
        # Columns
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                self._parse_columns_and_constraints(token, table)
                break
                
        # Partitioning
        stmt_str = str(statement).upper()
        match_part = re.search(r'PARTITION\s+BY\s+(.*?)(?:$|;)', stmt_str, re.IGNORECASE | re.DOTALL)
        if match_part:
            table.partition_by = match_part.group(1).strip()
            
        # Tablespace
        match_ts = re.search(r'\sTABLESPACE\s+([a-zA-Z0-9_"]+)', stmt_str)
        if match_ts:
            table.tablespace = self._clean_name(match_ts.group(1))

        self.schema.add_table(table)
