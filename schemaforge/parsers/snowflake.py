
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.models import Schema, Table, Column, CustomObject
from schemaforge.parsers.base import BaseParser
from schemaforge.parsers.utils import normalize_sql
import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, DML, DDL, Name
import sys

class SnowflakeParser(GenericSQLParser):
    def _clean_type(self, type_str):
        type_str = type_str.upper().strip()
        # Snowflake specific types
        if type_str in ('VARIANT', 'OBJECT', 'ARRAY', 'GEOGRAPHY', 'GEOMETRY'):
            return type_str
        return super()._clean_type(type_str)

    def parse(self, sql_content):
        self.schema = Schema()
        # Pre-process using parent method (strips comments)
        sql_content = self._strip_comments(sql_content)
        
        parsed = sqlparse.parse(sql_content)
        
        for statement in parsed:
            
            if statement.get_type() in ('CREATE', 'CREATE OR REPLACE'):
                self._process_create(statement)
            elif statement.get_type() == 'UNKNOWN':
                # sqlparse might not recognize some Snowflake commands as CREATE
                # Check first token
                first_token = statement.token_first()
                if first_token and first_token.match(DDL, 'CREATE'):
                    self._process_create(statement)
                # Also check if it starts with CREATE but sqlparse missed it (e.g. CREATE OR REPLACE)
                elif first_token and first_token.value.upper() == 'CREATE':
                     self._process_create(statement)
                    
        return self.schema

    def _process_create(self, statement):
        # Check for CREATE [OR REPLACE] [TRANSIENT] TABLE
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        
        obj_type = None
        obj_name = None
        is_transient = False
        
        # Helper to check if token value matches or starts with keyword
        def check_keyword(token_val, keyword):
            return token_val == keyword or token_val.startswith(keyword + ' ')
            
        for i, token in enumerate(tokens):
            val = token.value.upper()
            
            if val == 'TRANSIENT':
                is_transient = True
                continue
                
            # Standard Objects (usually separate tokens)
            if val in ('TABLE', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'STREAM'):
                obj_type = val
                # Name is next token
                if i + 1 < len(tokens):
                    name_token = tokens[i+1]
                    # Handle Function/Identifier wrappers
                    if isinstance(name_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
                        obj_name = name_token.get_real_name()
                    else:
                        obj_name = name_token.value
                break
                
            # Objects that might be grouped (STAGE name, PIPE name, TASK name, STREAM name)
            for kw in ('STAGE', 'PIPE', 'TASK', 'STREAM'):
                if check_keyword(val, kw):
                    obj_type = kw
                    if val == kw:
                        # Separate token
                        if i + 1 < len(tokens):
                            obj_name = tokens[i+1].value
                    else:
                        # Grouped token (STAGE my_stage)
                        parts = token.value.split(maxsplit=1)
                        if len(parts) > 1:
                            obj_name = parts[1]
                    break
            if obj_type:
                break
                
            # FILE FORMAT
            if val == 'FILE':
                if i + 1 < len(tokens):
                    next_token = tokens[i+1]
                    next_val = next_token.value.upper()
                    if check_keyword(next_val, 'FORMAT'):
                        obj_type = 'FILE FORMAT'
                        if next_val == 'FORMAT':
                            if i + 2 < len(tokens):
                                obj_name = tokens[i+2].value
                        else:
                            # Grouped (FORMAT my_format)
                            parts = next_token.value.split(maxsplit=1)
                            if len(parts) > 1:
                                obj_name = parts[1]
                        break
        
        if not obj_type:
            return

        if obj_type == 'TABLE':
            self._process_snowflake_table(statement, is_transient)
        else:
            # Custom Object
            if obj_name:
                # Normalize SQL for robust comparison
                raw_sql = str(statement)
                normalized_sql = normalize_sql(raw_sql)
                self.schema.custom_objects.append(CustomObject(
                    obj_type=obj_type,
                    name=obj_name,
                    properties={'raw_sql': normalized_sql} 
                ))

    def _process_snowflake_table(self, statement, is_transient):
        # Use generic parser logic to get basic table structure
        # But we need to handle Snowflake specific syntax
        
        # Find table name
        name_idx = -1
        for i, token in enumerate([]): # Wait, tokens is not defined here! It was defined in _process_create
             # I need to re-extract tokens or pass them?
             # _process_snowflake_table uses 'statement'
             pass
             
        # Re-extract tokens for this method
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        
        name_idx = -1
        for i, token in enumerate(tokens):
            if token.value.upper() == 'TABLE':
                name_idx = i + 1
                break
        
        if name_idx == -1 or name_idx >= len(tokens):
            return

        name_token = tokens[name_idx]
        if isinstance(name_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
            table_name = name_token.get_real_name()
            # Check if original was quoted
            # This is tricky with sqlparse, get_real_name strips quotes
            # We need to check the raw value of the identifier
            raw_val = name_token.value
            if not (raw_val.startswith('"') and raw_val.endswith('"')):
                table_name = table_name.upper()
        else:
            table_name = name_token.value.upper() # Keyword or simple name, default to upper
            
        table = Table(name=table_name, is_transient=is_transient)
        
        # Find columns
        # If table name was a Function, columns are inside it
        columns_found = False
        if isinstance(name_token, sqlparse.sql.Function):
             for sub in name_token.tokens:
                if isinstance(sub, sqlparse.sql.Parenthesis):
                    self._parse_columns_and_constraints(sub, table)
                    columns_found = True
                    break
        
        if not columns_found:
            # Find columns (Parenthesis group) in main tokens
            for token in statement.tokens:
                if isinstance(token, sqlparse.sql.Parenthesis):
                    self._parse_columns_and_constraints(token, table)
                    break
        
        # Parse Advanced Properties (CLUSTER BY, RETENTION, COMMENT)
        stmt_str = str(statement).upper()
        
        # Cluster By
        if 'CLUSTER BY' in stmt_str:
            import re
            match = re.search(r'CLUSTER\s+BY\s*\((.*?)\)', stmt_str, re.IGNORECASE)
            if match:
                cols = [self._clean_name(c.strip()) for c in match.group(1).split(',')]
                table.cluster_by = cols
                
        # Retention
        if 'DATA_RETENTION_TIME_IN_DAYS' in stmt_str:
            import re
            match = re.search(r'DATA_RETENTION_TIME_IN_DAYS\s*=\s*(\d+)', stmt_str)
            if match:
                table.retention_days = int(match.group(1))
                
        # Comment
        if 'COMMENT' in stmt_str:
            import re
            match = re.search(r"COMMENT\s*=\s*'([^']*)'", stmt_str)
            if match:
                table.comment = match.group(1)

        self.schema.tables.append(table)
