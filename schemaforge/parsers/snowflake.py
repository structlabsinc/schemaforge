
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.models import Schema, Table, Column, CustomObject
from schemaforge.parsers.base import BaseParser
from schemaforge.parsers.utils import normalize_sql
import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, DML, DDL, Name
import sys

class SnowflakeParser(GenericSQLParser):
    def _get_next_token(self, tokens, start_idx):
        for i in range(start_idx, len(tokens)):
            if not tokens[i].is_whitespace:
                return i, tokens[i]
        return None, None

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
            elif statement.get_type() == 'ALTER':
                self._process_alter(statement)
            elif statement.get_type() == 'UNKNOWN':
                # sqlparse might not recognize some Snowflake commands as CREATE
                # Check first token
                first_token = statement.token_first()
                if first_token and first_token.match(DDL, 'CREATE'):
                    self._process_create(statement)
                elif first_token and first_token.match(DDL, 'ALTER'):
                    self._process_alter(statement)
                # Also check if it starts with CREATE but sqlparse missed it (e.g. CREATE OR REPLACE)
                elif first_token and first_token.value.upper() == 'CREATE':
                     self._process_create(statement)
                elif first_token and first_token.value.upper() == 'ALTER':
                     self._process_alter(statement)
                elif first_token and first_token.value.upper() in ('GRANT', 'REVOKE'):
                     self._process_grant_revoke(statement)
            
            elif statement.get_type() in ('GRANT', 'REVOKE'):
                self._process_grant_revoke(statement)
                    
        return self.schema

    def _process_grant_revoke(self, statement):
        # Parse GRANT/REVOKE as CustomObject
        stmt_str = str(statement)
        # Extract object name if possible, or just use the whole statement as name/type
        # GRANT SELECT ON TABLE T TO ROLE R
        # Type: GRANT
        # Name: SELECT ON TABLE T TO ROLE R
        
        tokens = statement.tokens
        command = tokens[0].value.upper()
        
        # Normalize SQL
        normalized_sql = normalize_sql(stmt_str)
        
        self.schema.custom_objects.append(CustomObject(
            obj_type=command,
            name=normalized_sql, # Use full SQL as name for now to ensure uniqueness and visibility
            properties={'raw_sql': normalized_sql}
        ))

    def _process_create(self, statement):
        # Check for CREATE [OR REPLACE] [TRANSIENT] TABLE
        tokens = statement.tokens
        
        obj_type = None
        obj_name = None
        is_transient = False
        is_secure = False
        is_external = False
        
        # Helper to check if token value matches or starts with keyword
        def check_keyword(token_val, keyword):
            return token_val == keyword or token_val.startswith(keyword + ' ')
            
        for i, token in enumerate(tokens):
            if token.is_whitespace:
                continue
            
            val = token.value.upper()
            
            # Track modifiers
            if val == 'TRANSIENT':
                is_transient = True
                continue
            if val == 'SECURE':
                is_secure = True
                continue
            if val == 'EXTERNAL':
                is_external = True
                continue
                
            # Check for Modern Table Types (ICEBERG, HYBRID, EVENT, DYNAMIC)
            if val in ('ICEBERG', 'HYBRID', 'EVENT', 'DYNAMIC'):
                # Check if next token is TABLE
                idx, next_token = self._get_next_token(tokens, i + 1)
                if next_token and next_token.value.upper() == 'TABLE':
                    obj_type = 'TABLE' 
                    # Store the specific type
                    # We need to pass this to _process_snowflake_table or set it on the table object
                    # But _process_snowflake_table creates the Table object.
                    # We should pass it as an argument.
                    specific_type = f"{val} TABLE".title() # e.g. "Iceberg Table"
                    
                    # Advance 'i' to the 'TABLE' token so the subsequent name extraction works correctly
                    i = idx
                    
                    # We need to store this specific type to pass to _process_snowflake_table
                    # But the loop continues.
                    # Let's change _process_snowflake_table signature.
                    self._process_snowflake_table(statement, is_transient, table_type=specific_type)
                    return # We handled it
            
            # Standard Objects (usually separate tokens)
            if val in ('TABLE', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'STREAM', 'SCHEMA'):
                obj_type = val
                # Apply modifiers
                if val == 'VIEW' and is_secure:
                    obj_type = 'SECURE VIEW'
                if val == 'TABLE' and is_external:
                    # EXTERNAL TABLE - parse as table with external type
                    self._process_snowflake_table(statement, is_transient, table_type='External Table')
                    return
                # Name is next token
                idx, name_token = self._get_next_token(tokens, i + 1)
                if name_token:
                    # Handle Function/Identifier wrappers
                    if isinstance(name_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
                        real_name = name_token.get_real_name()
                        # Check if quoted in value to preserve case
                        if f'"{real_name}"' in name_token.value or f'`{real_name}`' in name_token.value or f'[{real_name}]' in name_token.value:
                            obj_name = f'"{real_name}"'
                        else:
                            obj_name = real_name
                    else:
                        obj_name = name_token.value
                    
                    obj_name = self._clean_name(obj_name)
                break
            
            # MATERIALIZED VIEW
            if val == 'MATERIALIZED':
                idx, next_token = self._get_next_token(tokens, i + 1)
                if next_token and next_token.value.upper() == 'VIEW':
                    obj_type = 'MATERIALIZED VIEW'
                    idx2, name_token = self._get_next_token(tokens, idx + 1)
                    if name_token:
                        obj_name = self._clean_name(name_token.value)
                    break

            # Objects that might be grouped (STAGE name, PIPE name, TASK name, STREAM name, TAG name, FUNCTION name, ALERT name)
            for kw in ('STAGE', 'PIPE', 'TASK', 'STREAM', 'TAG', 'FUNCTION', 'ALERT'):
                if check_keyword(val, kw):
                    obj_type = kw
                    # Apply modifiers for FUNCTION
                    if kw == 'FUNCTION' and is_external:
                        obj_type = 'EXTERNAL FUNCTION'
                    if val == kw:
                        # Separate token
                        idx, name_token = self._get_next_token(tokens, i + 1)
                        if name_token:
                            obj_name = self._clean_name(name_token.value)
                    else:
                        # Grouped token (STAGE my_stage)
                        parts = token.value.split(maxsplit=1)
                        if len(parts) > 1:
                            obj_name = self._clean_name(parts[1])
                    
                    # Handle Function signature (remove parens)
                    if kw == 'FUNCTION' and obj_name and '(' in obj_name:
                        obj_name = obj_name.split('(')[0]
                    elif kw == 'FUNCTION' and obj_name and i + 2 < len(tokens) and tokens[i+2].value == '(':
                         # FUNCTION name ( args )
                         pass 
                    break
            
            # DATABASE ROLE (CREATE DATABASE ROLE name)
            if val == 'DATABASE':
                idx, next_token = self._get_next_token(tokens, i + 1)
                if next_token and next_token.value.upper() == 'ROLE':
                    obj_type = 'DATABASE ROLE'
                    idx2, name_token = self._get_next_token(tokens, idx + 1)
                    if name_token:
                        obj_name = self._clean_name(name_token.value)
                    break
            
            # MASKING POLICY / ROW ACCESS POLICY
            if val == 'MASKING' or val == 'MASKING POLICY':
                obj_type = 'MASKING POLICY'
                if val == 'MASKING':
                    # Check next token
                    idx, next_t = self._get_next_token(tokens, i + 1)
                    if next_t and next_t.value.upper() == 'POLICY':
                        # Get name
                        idx2, name_t = self._get_next_token(tokens, idx + 1)
                        if name_t:
                            obj_name = self._clean_name(name_t.value)
                        break
                else:
                    # Single token MASKING POLICY
                    idx, name_t = self._get_next_token(tokens, i + 1)
                    if name_t:
                        obj_name = self._clean_name(name_t.value)
                    break
            
            if val == 'ROW' or val == 'ROW ACCESS POLICY':
                obj_type = 'ROW ACCESS POLICY'
                if val == 'ROW':
                    # Check ACCESS
                    idx, next_t = self._get_next_token(tokens, i + 1)
                    if next_t and next_t.value.upper() == 'ACCESS':
                        # Check POLICY
                        idx2, next_t2 = self._get_next_token(tokens, idx + 1)
                        if next_t2:
                            val2 = next_t2.value.upper()
                            if val2 == 'POLICY':
                                # Separate token
                                idx3, name_t = self._get_next_token(tokens, idx2 + 1)
                                if name_t:
                                    obj_name = self._clean_name(name_t.value)
                                break
                            elif val2.startswith('POLICY '):
                                # Grouped token: POLICY name ...
                                parts = next_t2.value.split(maxsplit=2)
                                if len(parts) > 1:
                                    obj_name = self._clean_name(parts[1])
                                break
                else:
                    # Single token ROW ACCESS POLICY
                    idx, name_t = self._get_next_token(tokens, i + 1)
                    if name_t:
                        obj_name = self._clean_name(name_t.value)
                    break

            if obj_type:
                break
                
            # FILE FORMAT
            if val == 'FILE':
                idx, next_token = self._get_next_token(tokens, i + 1)
                if next_token:
                    next_val = next_token.value.upper()
                    if check_keyword(next_val, 'FORMAT'):
                        obj_type = 'FILE FORMAT'
                        if next_val == 'FORMAT':
                            idx2, name_token = self._get_next_token(tokens, idx + 1)
                            if name_token:
                                obj_name = self._clean_name(name_token.value)
                        else:
                            # Grouped (FORMAT my_format)
                            parts = next_token.value.split(maxsplit=1)
                            if len(parts) > 1:
                                obj_name = self._clean_name(parts[1])
                        break
        
        if not obj_type:
            return

        if obj_type == 'TABLE':
            self._process_snowflake_table(statement, is_transient, table_type="Table")
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

    def _process_snowflake_table(self, statement, is_transient, table_type="Table"):
        # Use generic parser logic to get basic table structure
        # But we need to handle Snowflake specific syntax
        
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
            real_name = name_token.get_real_name()
            # Check if quoted in value to preserve case
            if f'"{real_name}"' in name_token.value or f'`{real_name}`' in name_token.value or f'[{real_name}]' in name_token.value:
                table_name = f'"{real_name}"'
            else:
                table_name = real_name
        else:
            table_name = name_token.value
            
        table = Table(name=self._clean_name(table_name), is_transient=is_transient, table_type=table_type)
        
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
        # Cluster By
        if 'CLUSTER BY' in stmt_str:
            # Use token iteration to find CLUSTER BY and the following Parenthesis
            for i, token in enumerate(statement.tokens):
                if token.value.upper() == 'CLUSTER':
                    # Check next token is BY
                    # Need to skip whitespace
                    next_idx = i + 1
                    while next_idx < len(statement.tokens) and statement.tokens[next_idx].is_whitespace:
                        next_idx += 1
                    
                    if next_idx < len(statement.tokens) and statement.tokens[next_idx].value.upper() == 'BY':
                        # Find next Parenthesis
                        paren_idx = next_idx + 1
                        while paren_idx < len(statement.tokens):
                            if isinstance(statement.tokens[paren_idx], sqlparse.sql.Parenthesis):
                                # Found it!
                                cluster_content = statement.tokens[paren_idx].value[1:-1] # Strip outer ()
                                
                                # Use simple parenthesis-aware splitter
                                cols = []
                                current = []
                                paren_level = 0
                                for char in cluster_content:
                                    if char == ',' and paren_level == 0:
                                        cols.append("".join(current).strip())
                                        current = []
                                    else:
                                        if char == '(': paren_level += 1
                                        if char == ')': paren_level -= 1
                                        current.append(char)
                                if current:
                                    cols.append("".join(current).strip())
                                
                                table.cluster_by = [self._clean_name(c) for c in cols]
                                break
                            paren_idx += 1
                    break
                

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

    def _process_alter(self, statement):
        # Handle ALTER TABLE for Policies and Tags
        import re
        stmt_str = str(statement).upper()
        
        # Extract Table Name
        # ALTER TABLE name ...
        match_table = re.search(r'ALTER\s+TABLE\s+(\w+)', stmt_str, re.IGNORECASE)
        if not match_table:
            return
            
        table_name = self._clean_name(match_table.group(1))
        table = self.schema.get_table(table_name)
        if not table:
            # Table might not be in schema if we are parsing partial DDL or if it was created in another file
            # For now, we only support modifying tables defined in the same parsing context or previously parsed
            return

        # Masking Policy
        # ALTER TABLE t MODIFY COLUMN c SET MASKING POLICY p
        # ALTER TABLE t MODIFY COLUMN c UNSET MASKING POLICY
        if 'MASKING POLICY' in stmt_str:
            # SET
            match_mp = re.search(r'MODIFY\s+COLUMN\s+(\w+)\s+SET\s+MASKING\s+POLICY\s+(\w+)', stmt_str, re.IGNORECASE)
            if match_mp:
                col_name = match_mp.group(1)
                policy_name = match_mp.group(2)
                table.policies.append(f"MASKING POLICY {policy_name} ON {col_name}")
            
            # UNSET
            match_mp_unset = re.search(r'MODIFY\s+COLUMN\s+(\w+)\s+UNSET\s+MASKING\s+POLICY', stmt_str, re.IGNORECASE)
            if match_mp_unset:
                col_name = match_mp_unset.group(1)
                # Remove any masking policy on this column
                # Format: "MASKING POLICY {policy_name} ON {col_name}"
                # We need to find and remove it.
                # Since policy name is unknown in UNSET, we filter by suffix.
                suffix = f" ON {col_name}"
                table.policies = [p for p in table.policies if not (p.startswith("MASKING POLICY") and p.endswith(suffix))]

        # Row Access Policy
        # ALTER TABLE t ADD ROW ACCESS POLICY p ON (c)
        # ALTER TABLE t DROP ROW ACCESS POLICY p
        if 'ROW ACCESS POLICY' in stmt_str:
            # ADD
            match_rap = re.search(r'ADD\s+ROW\s+ACCESS\s+POLICY\s+(\w+)\s+ON\s*\((.*?)\)', stmt_str, re.IGNORECASE)
            if match_rap:
                policy_name = match_rap.group(1)
                cols = match_rap.group(2)
                table.policies.append(f"ROW ACCESS POLICY {policy_name} ON ({cols})")
            
            # DROP
            match_rap_drop = re.search(r'DROP\s+ROW\s+ACCESS\s+POLICY\s+(\w+)', stmt_str, re.IGNORECASE)
            if match_rap_drop:
                policy_name = match_rap_drop.group(1)
                # Remove by policy name prefix
                prefix = f"ROW ACCESS POLICY {policy_name}"
                table.policies = [p for p in table.policies if not p.startswith(prefix)]
                
        # Tags
        # ALTER TABLE t SET TAG tag1 = 'val1', tag2 = 'val2'
        # ALTER TABLE t UNSET TAG tag1, tag2
        if 'TAG' in stmt_str:
            # SET
            if 'SET TAG' in stmt_str:
                match_tag = re.search(r'SET\s+TAG\s+(.*)', stmt_str, re.IGNORECASE)
                if match_tag:
                    tags_content = match_tag.group(1).rstrip(';') # Remove trailing semicolon
                    tag_assignments = tags_content.split(',')
                    for tag_assign in tag_assignments:
                        if '=' in tag_assign:
                            k, v = tag_assign.split('=', 1)
                            clean_v = v.strip().strip("'").strip()
                            table.tags[self._clean_name(k.strip())] = clean_v
            
            # UNSET
            if 'UNSET TAG' in stmt_str:
                match_tag_unset = re.search(r'UNSET\s+TAG\s+(.*)', stmt_str, re.IGNORECASE)
                if match_tag_unset:
                    tags_content = match_tag_unset.group(1).rstrip(';')
                    tag_names = tags_content.split(',')
                    for tag_name in tag_names:
                        clean_tag = self._clean_name(tag_name.strip())
                        if clean_tag in table.tags:
                            del table.tags[clean_tag]
