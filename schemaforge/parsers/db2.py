from schemaforge.parsers.generic_sql import GenericSQLParser

from schemaforge.models import Table, CustomObject, Schema, Column
from schemaforge.parsers.utils import normalize_sql
import sqlparse
from sqlparse.sql import Statement, Token
from sqlparse.sql import Statement, Token
from sqlparse.tokens import Keyword, DML, DDL, Name
import re

class DB2Parser(GenericSQLParser):
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
        
        obj_type = None
        obj_name = None
        
        for i, token in enumerate(tokens):
            val = token.value.upper()
            
            if val == 'TABLE':
                self._process_db2_table(statement)
                return
                
            if val == 'ALIAS':
                obj_type = 'ALIAS'
                if i + 1 < len(tokens):
                    # ALIAS FOR ...
                    # CREATE ALIAS name FOR target
                    # Name is at i+1
                    obj_name = tokens[i+1].value
                break
                
            if val == 'INDEX':
                 self._extract_create_index(statement, self.schema)
                 return
                 
            if val == 'UNIQUE' and i+1 < len(tokens) and tokens[i+1].value.upper() == 'INDEX':
                 self._extract_create_index(statement, self.schema, is_unique=True)
                 return

            if val == 'VIEW':
                 self._extract_create_view(statement, self.schema)
                 return
                
        if obj_type == 'ALIAS' and obj_name:
             self.schema.custom_objects.append(CustomObject(
                obj_type='ALIAS',
                name=obj_name,
                properties={'raw_sql': normalize_sql(str(statement))}
            ))

    def _process_db2_table(self, statement):
        # Use generic logic for basic structure
        # But we need to extract DB2 specific properties
        stmt_str = str(statement) # Keep case for extraction
        
        # 1. Extract Name and Columns using generic logic
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
        
        # Check for AUX TABLE
        if 'AUX' in stmt_str.upper():
             match_aux = re.search(r'CREATE\s+AUX\s+TABLE', stmt_str, re.IGNORECASE)
             if match_aux:
                 table.table_type = "AUX TABLE"
                 
        # Check for STORES clause (for AUX tables)
        match_stores = re.search(r'STORES\s+([a-zA-Z0-9_"]+)\s+COLUMN\s+([a-zA-Z0-9_"]+)', stmt_str, re.IGNORECASE)
        if match_stores:
             table.tags['aux_stores_table'] = self._clean_name(match_stores.group(1))
             table.tags['aux_stores_col'] = self._clean_name(match_stores.group(2))
        
        # Columns
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                self._parse_columns_and_constraints(token, table)
                break
                
        # Post-process columns for IDENTITY
        # Generic parser might miss "GENERATED ALWAYS AS IDENTITY" as it's complex
        # We might need to refine column parsing.
        
        # Tablespace: IN "tablespace_name" or IN DATABASE "db"."ts"
        
        # IN clause (DB.TS or just TS)
        # Matches: IN DB.TS or IN TS or IN DATABASE DB.TS
        match_in = re.search(r'\sIN\s+(?:DATABASE\s+)?([a-zA-Z0-9_".]+)', stmt_str, re.IGNORECASE)
        if match_in:
            val = match_in.group(1)
            # Check if it has DB part
            parts = val.split('.')
            if len(parts) == 2:
                table.database_name = self._clean_name(parts[0])
                table.tablespace = self._clean_name(parts[1])
            else:
                table.tablespace = self._clean_name(val)
                
        # STOGROUP
        match_sto = re.search(r'USING\s+STOGROUP\s+([a-zA-Z0-9_"]+)', stmt_str, re.IGNORECASE)
        if match_sto:
            table.stogroup = self._clean_name(match_sto.group(1))
            
        # PRIQTY
        match_pri = re.search(r'PRIQTY\s+(\d+)', stmt_str, re.IGNORECASE)
        if match_pri:
            table.priqty = int(match_pri.group(1))
            
        # SECQTY
        match_sec = re.search(r'SECQTY\s+(\d+)', stmt_str, re.IGNORECASE)
        if match_sec:
            table.secqty = int(match_sec.group(1))
            
        # AUDIT
        match_audit = re.search(r'AUDIT\s+(NONE|CHANGES|ALL)', stmt_str, re.IGNORECASE)
        if match_audit:
            table.audit = match_audit.group(1).upper() # Normalize to upper
            
        # CCSID
        match_ccsid = re.search(r'CCSID\s+(EBCDIC|ASCII|UNICODE)', stmt_str, re.IGNORECASE)
        if match_ccsid:
            table.ccsid = match_ccsid.group(1).upper() # Normalize to upper
            
        # Partitioning: PARTITION BY ...
        # Match from PARTITION BY until standard end or next major clause
        match_part = re.search(r'PARTITION\s+BY\s+(.*?)(?=\)|;|$)', stmt_str, re.IGNORECASE | re.DOTALL)
        if match_part:
            idx = stmt_str.upper().find("PARTITION BY")
            if idx != -1:
                part_def = stmt_str[idx:].strip().rstrip(';')
                table.partition_by = part_def[len("PARTITION BY"):].strip()
            
        # PERIOD FOR (Temporal)
        match_period = re.search(r'PERIOD\s+FOR\s+([a-zA-Z0-9_]+\s*\(.*?\))', stmt_str, re.IGNORECASE)
        if match_period:
             table.period_for = match_period.group(1).strip()
             # Remove potential ghost column "PERIOD" created by generic parser
             table.columns = [c for c in table.columns if c.name.upper() != 'PERIOD']
            
        self.schema.tables.append(table)

    def _parse_column_def(self, token):
        # Override to handle IDENTITY and Special Clauses
        
        raw = str(token).upper().strip()
        
        # Filter out PERIOD FOR SYSTEM_TIME (DB2 Temporal)
        if raw.startswith('PERIOD FOR'):
            # Extract content "SYSTEM_TIME (start, end)"
            # Token is usually "PERIOD FOR SYSTEM_TIME (SYS_START, SYS_END)"
            # Regex or split
            match_period = re.search(r'PERIOD\s+FOR\s+(.+)', raw, re.IGNORECASE)
            if match_period:
                # We need to access the current table object being parsed.
                # But _parse_column_def doesn't support writing to the table :(.
                # It returns a Column or None.
                # WORKAROUND: The caller `_parse_columns_and_constraints` (generic) ignores return value None.
                # But it also doesn't pass the table to `_parse_column_def`.
                # Checks generic_sql.py: `self._parse_column_def(token)` is called.
                
                # I need to modify `GenericSQLParser._parse_columns_and_constraints` to allow custom line handlers?
                # OR, I iterate tokens in `_process_db2_table` and extract PERIOD manually BEFORE calling regex/generic.
                pass
            return None
            
        col = super()._parse_column_def(token)
        if col:
             # Check for IDENTITY in the raw token
            if 'GENERATED ALWAYS AS IDENTITY' in raw or 'GENERATED BY DEFAULT AS IDENTITY' in raw:
                col.is_identity = True
            
            # Check for ROW BEGIN/END (Temporal)
            if 'GENERATED ALWAYS AS ROW BEGIN' in raw:
                # Add metadata? For now just ensure type is correct
                pass
                
        return col

    def _extract_create_view(self, statement, schema: Schema):
        # Treat as CustomObject with raw_sql
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        # Get Name
        name = "unknown_view"
        for i, token in enumerate(tokens):
            if token.value.upper() == 'VIEW':
                if i+1 < len(tokens):
                    name = self._clean_name(tokens[i+1].get_real_name())
                break
        
        view = CustomObject(name=name, obj_type="VIEW", properties={'raw_sql': normalize_sql(str(statement))})
        schema.custom_objects.append(view)

    def _extract_create_index(self, statement, schema: Schema, is_unique: bool = False):
        # Override to capture INCLUDE, CLUSTER, PARTITIONED
        super()._extract_create_index(statement, schema, is_unique)
        
        # Now find the just added index and augment it
        tokens = [t for t in statement.tokens if not t.is_whitespace and not isinstance(t, sqlparse.sql.Comment)]
        idx_name = None
        on_index = -1
        for i, token in enumerate(tokens):
            if token.value.upper() == 'ON':
                on_index = i
                break
        
        if on_index > 0:
            idx_name_token = tokens[on_index-1]
            if isinstance(idx_name_token, sqlparse.sql.Identifier):
                idx_name = idx_name_token.get_real_name()
            else:
                idx_name = idx_name_token.value
            
            idx_name = self._clean_name(idx_name)
            
            # Find the table name to locate the table object
            table_name = None
            if on_index + 1 < len(tokens):
                t_token = tokens[on_index+1]
                if isinstance(t_token, sqlparse.sql.Identifier):
                    table_name = t_token.get_real_name()
                elif isinstance(t_token, sqlparse.sql.Function):
                     table_name = t_token.get_real_name()
                else:
                    table_name = t_token.value
            
            if table_name:
                table = schema.get_table(self._clean_name(table_name))
                if table:
                    # Find index by name
                    target_index = None
                    for idx in table.indexes:
                        if idx.name == idx_name:
                            target_index = idx
                            break
                    
                    if target_index:
                        stmt_str = str(statement).upper()
                        # Check properties
                        if 'CLUSTER' in stmt_str:
                            target_index.properties['cluster'] = True
                        if 'PARTITIONED' in stmt_str:
                             target_index.properties['partitioned'] = True
                        # INCLUDE
                        match_include = re.search(r'INCLUDE\s*\((.*?)\)', str(statement), re.IGNORECASE)
                        if match_include:
                            target_index.properties['include_columns'] = [c.strip() for c in match_include.group(1).split(',')]
