import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML, DDL
from typing import List, Optional
from schemaforge.models import Schema, Table, Column, Index, ForeignKey
from schemaforge.parsers.base import BaseParser

class GenericSQLParser(BaseParser):
    def parse(self, sql_content: str) -> Schema:
        # Preprocess to strip nested comments which sqlparse doesn't handle well
        sql_content = self._strip_comments(sql_content)
        
        schema = Schema()
        parsed = sqlparse.parse(sql_content)
        
        import logging
        logger = logging.getLogger('schemaforge')
        
        for statement in parsed:
            processed = False
            stmt_type = statement.get_type()
            if stmt_type == 'CREATE':
                # Check if it is CREATE TABLE or CREATE INDEX
                # Filter out whitespace AND comments to find the real keywords
                token_list = [t for t in statement.tokens if not t.is_whitespace and not isinstance(t, sqlparse.sql.Comment)]
                
                # token_list[0] should be CREATE
                if len(token_list) > 1 and token_list[1].value.upper() == 'TABLE':
                    table = self._extract_create_table(statement)
                    if table:
                        schema.add_table(table)
                        processed = True
                elif len(token_list) > 1 and token_list[1].value.upper() == 'INDEX':
                    self._extract_create_index(statement, schema)
                    processed = True
                elif len(token_list) > 2 and token_list[1].value.upper() == 'UNIQUE' and token_list[2].value.upper() == 'INDEX':
                     self._extract_create_index(statement, schema, is_unique=True)
                     processed = True
            
            elif stmt_type == 'UNKNOWN':
                 # Handle COMMENT ON (sqlparse might return UNKNOWN or just handle keywords)
                 # Actually sqlparse often treats COMMENT ON as valid statement but maybe not a type it recognizes easily
                 first_token = statement.token_first()
                 if first_token and first_token.value.upper() == 'COMMENT':
                     self._process_comment(statement, schema)
                     
            if not processed:
                stmt_str = str(statement).strip()
                # Check for meaningful content (not just comments/whitespace)
                has_content = any(not (t.is_whitespace or isinstance(t, sqlparse.sql.Comment)) for t in statement.tokens)
                if not has_content: continue
                
                upper_stmt = stmt_str.upper()
                if any(upper_stmt.startswith(k) for k in ('SET', 'USE', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'PRAGMA', 'BEGIN', 'END')):
                     continue
                     
                if upper_stmt.startswith('CREATE'):
                     logger.error(f"Failed to parse statement: {stmt_str[:100]}...")
                else:
                     logger.warning(f"Ignored statement (not CREATE/COMMENT): {stmt_str[:50]}...")

        return schema

    def _extract_create_index(self, statement, schema: Schema, is_unique: bool = False):
        # CREATE [UNIQUE] INDEX index_name ON table_name (col1, col2)
        tokens = [t for t in statement.tokens if not t.is_whitespace and not isinstance(t, sqlparse.sql.Comment)]
        
        idx_name = None
        table_name = None
        columns = []
        
        # Simple iteration to find ON keyword
        on_index = -1
        for i, token in enumerate(tokens):
            if token.value.upper() == 'ON':
                on_index = i
                break
        
        if on_index > 0:
            # Name is usually before ON
            idx_name_token = tokens[on_index-1]
            if isinstance(idx_name_token, Identifier):
                idx_name = idx_name_token.get_real_name()
            else:
                idx_name = idx_name_token.value
                
            # Table is after ON
            if on_index + 1 < len(tokens):
                table_token = tokens[on_index+1]
                if isinstance(table_token, Identifier):
                    table_name = table_token.get_real_name()
                elif isinstance(table_token, sqlparse.sql.Function):
                     table_name = table_token.get_real_name()
                     # Extract columns from parameters
                     for param in table_token.get_parameters():
                         columns.append(str(param))
                else:
                    table_name = table_token.value
            
            # Columns are in parenthesis after table
            # sqlparse might group table and columns in an Identifier or separate
            # We look for Parenthesis
            _, parenthesis = statement.token_next_by(i=sqlparse.sql.Parenthesis)
            if parenthesis:
                 # Extract columns
                 content_tokens = parenthesis.tokens[1:-1]
                 for token in content_tokens:
                     if isinstance(token, IdentifierList):
                         for id_token in token.get_identifiers():
                             columns.append(id_token.get_real_name())
                     elif isinstance(token, Identifier):
                         columns.append(token.get_real_name())
                     elif token.ttype is sqlparse.tokens.Name:
                         columns.append(token.value)
        
        # print(f"DEBUG: Index={idx_name}, Table={table_name}, Cols={columns}")
        if idx_name and table_name and columns:
            table = schema.get_table(self._clean_name(table_name))
            if table:
                index = Index(name=self._clean_name(idx_name), columns=[self._clean_name(c) for c in columns], is_unique=is_unique)
                table.indexes.append(index)
            # else:
            #    print(f"DEBUG: Table {table_name} not found for index {idx_name}")

    def _extract_create_table(self, statement) -> Optional[Table]:
        table_name = None
        
        # Find TABLE keyword index
        table_keyword_idx = -1
        for i, token in enumerate(statement.tokens):
            if token.value.upper() == 'TABLE':
                table_keyword_idx = i
                break
        
        # If TABLE found, look for identifier after it
        if table_keyword_idx != -1:
            for i in range(table_keyword_idx + 1, len(statement.tokens)):
                token = statement.tokens[i]
                if isinstance(token, Identifier):
                    real_name = token.get_real_name()
                    # Check if quoted in value to preserve case
                    if f'"{real_name}"' in token.value or f'`{real_name}`' in token.value or f'[{real_name}]' in token.value:
                        table_name = f'"{real_name}"'
                    else:
                        table_name = real_name
                    break
        else:
            # Fallback
            for token in statement.tokens:
                if isinstance(token, Identifier):
                    real_name = token.get_real_name()
                    # Check if quoted in value to preserve case
                    if f'"{real_name}"' in token.value or f'`{real_name}`' in token.value or f'[{real_name}]' in token.value:
                        table_name = f'"{real_name}"'
                    else:
                        table_name = real_name
                    break
                
        if not table_name:
            return None
            
        from schemaforge.logging_config import get_logger
        logger = get_logger("parser")
        logger.debug(f"Extracted Table Name: {table_name}, Cleaned: {self._clean_name(table_name)}")
        table = Table(name=self._clean_name(table_name))
        
        _, parenthesis = statement.token_next_by(i=sqlparse.sql.Parenthesis)
        
        if parenthesis:
            self._parse_columns_and_constraints(parenthesis, table)
        else:
            logger.warning(f"Failed to parse columns for table {table_name}: No parenthesis found. Possible syntax error (e.g. unclosed parenthesis).")
            
        return table

    def _flatten_tokens(self, tokens):
        flat = []
        for token in tokens:
            if isinstance(token, IdentifierList):
                flat.extend(self._flatten_tokens(token.tokens))
            elif isinstance(token, Identifier):
                flat.extend(self._flatten_tokens(token.tokens))
            else:
                flat.append(token)
        return flat

    def _parse_columns_and_constraints(self, parenthesis, table: Table):
        # Extract tokens from Parenthesis object if needed
        if isinstance(parenthesis, sqlparse.sql.Parenthesis):
            tokens = parenthesis.tokens[1:-1] # Strip ( and )
        else:
            tokens = parenthesis
            
        # Filter out whitespace and comments first? No, we need them for some context
        # But we definitely want to strip comments from the token stream we process
        content_tokens = [t for t in tokens if not isinstance(t, sqlparse.sql.Comment)]
        
        # Flatten IdentifierList and Identifier recursively
        flat_tokens = self._flatten_tokens(content_tokens)
        
        # Split by comma, respecting parenthesis
        current_tokens = []
        parenthesis_level = 0
        for token in flat_tokens:
            if token.value == ',' and parenthesis_level == 0:
                self._process_definition(current_tokens, table)
                current_tokens = []
            elif isinstance(token, sqlparse.sql.Comment):
                continue
            elif not token.is_whitespace:
                current_tokens.append(token)
                if token.value == '(':
                    parenthesis_level += 1
                elif token.value == ')':
                    parenthesis_level -= 1
                # if token.is_group:
                #     # This is naive, sqlparse might not give us easy parenthesis level
                #     # But flat_tokens shouldn't have groups except maybe functions?
                #     # Actually we flattened IdentifierList and Identifier.
                #     # Parenthesis group is still a group.
                #     if isinstance(token, sqlparse.sql.Parenthesis):
                #         pass # We treat parenthesis as a single token here
                
        if current_tokens:
            self._process_definition(current_tokens, table)

    def _process_definition(self, tokens, table: Table):
        if not tokens:
            return
            
        first_word = tokens[0].value.upper()
        
        # Ignore common table options that might appear inside parenthesis or be misparsed
        # Note: 'COMMENT' as a standalone word could be a column name, only skip 'COMMENT ON'
        if first_word in ('ORGANIZE', 'CLUSTER', 'ENGINE', 'WITH', 'PARTITION'):
            return
        # Skip 'COMMENT ON' statements (table/column metadata) but not columns named 'comment'
        if first_word == 'COMMENT' and len(tokens) > 1 and tokens[1].value.upper() == 'ON':
            return

        if first_word == 'CONSTRAINT':
            self._process_constraint(tokens, table)
        elif first_word in ('PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'INDEX', 'KEY') or 'PRIMARY KEY' in first_word:
             # Inline constraint without CONSTRAINT keyword (e.g. PRIMARY KEY (id))
             
             is_pk = False
             content_token = None
             
             if first_word == 'PRIMARY' and len(tokens) > 2 and tokens[1].value.upper() == 'KEY':
                 is_pk = True
                 content_token = tokens[2]
             elif 'PRIMARY KEY' in first_word:
                 is_pk = True
                 if len(tokens) > 1:
                     content_token = tokens[1]
             
             if is_pk and content_token and isinstance(content_token, sqlparse.sql.Parenthesis):
                 self._extract_pk_cols(content_token, table)
                 return
                 
             if is_pk and content_token and isinstance(content_token, sqlparse.sql.Parenthesis):
                 self._extract_pk_cols(content_token, table)
                 return
                 
             # Handle UNIQUE (cols)
             if first_word == 'UNIQUE':
                 # UNIQUE (cols)
                 if len(tokens) > 1 and isinstance(tokens[1], sqlparse.sql.Parenthesis):
                     # Generate a name if not provided (inline unique usually doesn't have name unless CONSTRAINT used)
                     # But here we are in _process_definition without CONSTRAINT
                     self._extract_unique_constraint(tokens[1], table, f"uk_{table.name}_{len(table.indexes)}")
                     return

             # Handle FOREIGN KEY (cols) REFERENCES ref_table (ref_cols)
             if first_word == 'FOREIGN' and len(tokens) > 1 and tokens[1].value.upper() == 'KEY':
                 # FOREIGN KEY (cols) REFERENCES ...
                 if len(tokens) > 2 and isinstance(tokens[2], sqlparse.sql.Parenthesis):
                     # We need to pass all tokens to _extract_foreign_key or just the relevant ones?
                     # _extract_foreign_key expects the full token stream starting from FOREIGN KEY usually?
                     # No, it iterates tokens.
                     # Let's pass the whole stream.
                     self._extract_foreign_key(tokens, table, f"fk_{table.name}_{len(table.foreign_keys)}")
                     return
             
             # Handle CHECK (expr)
             if first_word == 'CHECK':
                 if len(tokens) > 1 and isinstance(tokens[1], sqlparse.sql.Parenthesis):
                     self._extract_check_constraint(tokens[1], table, f"ck_{table.name}_{len(table.check_constraints)}")
                     return

        else:
            self._process_column(tokens, table)

    def _process_constraint(self, tokens, table: Table):
        # CONSTRAINT name FOREIGN KEY (cols) REFERENCES ref_table(ref_cols)
        # CONSTRAINT name PRIMARY KEY (cols)
        
        constraint_name = tokens[1].value
        if isinstance(tokens[1], Identifier):
            constraint_name = tokens[1].get_real_name()
            
        type_idx = 2
        if len(tokens) <= type_idx:
             return

        constraint_type = tokens[type_idx].value.upper()
        
        if constraint_type == 'FOREIGN':
            # Expect FOREIGN KEY
            if len(tokens) > type_idx + 1 and tokens[type_idx+1].value.upper() == 'KEY':
                self._extract_foreign_key(tokens, table, constraint_name)
        elif constraint_type == 'PRIMARY' or constraint_type == 'PRIMARY KEY':
            # Expect PRIMARY KEY
            # Case 1: PRIMARY KEY as separate tokens
            if constraint_type == 'PRIMARY' and len(tokens) > type_idx + 1 and tokens[type_idx+1].value.upper() == 'KEY':
                # Extract cols from parenthesis
                # CONSTRAINT name PRIMARY KEY (cols)
                # tokens: CONSTRAINT, name, PRIMARY, KEY, (cols)
                if len(tokens) > type_idx + 2 and isinstance(tokens[type_idx+2], sqlparse.sql.Parenthesis):
                    self._extract_pk_cols(tokens[type_idx+2], table, constraint_name)
            # Case 2: PRIMARY KEY as single token
            elif constraint_type == 'PRIMARY KEY':
                # CONSTRAINT name PRIMARY KEY (cols)
                # tokens: CONSTRAINT, name, PRIMARY KEY, (cols)
                if len(tokens) > type_idx + 1 and isinstance(tokens[type_idx+1], sqlparse.sql.Parenthesis):
                    self._extract_pk_cols(tokens[type_idx+1], table, constraint_name)
        
        elif constraint_type == 'UNIQUE':
            # CONSTRAINT name UNIQUE (cols)
            if len(tokens) > type_idx + 1 and isinstance(tokens[type_idx+1], sqlparse.sql.Parenthesis):
                self._extract_unique_constraint(tokens[type_idx+1], table, constraint_name)
                
        elif constraint_type == 'CHECK':
            # CONSTRAINT name CHECK (expr)
            if len(tokens) > type_idx + 1 and isinstance(tokens[type_idx+1], sqlparse.sql.Parenthesis):
                self._extract_check_constraint(tokens[type_idx+1], table, constraint_name)

    def _extract_pk_cols(self, parenthesis, table: Table, name: Optional[str] = None):
        content = parenthesis.tokens[1:-1]
        for t in content:
            col_names = []
            if isinstance(t, IdentifierList):
                for id_token in t.get_identifiers():
                    col_names.append(id_token.get_real_name())
            elif isinstance(t, Identifier):
                col_names.append(t.get_real_name())
            elif t.ttype is sqlparse.tokens.Name:
                col_names.append(t.value)
            
            for col_name in col_names:
                if col_name:
                    clean_col_name = self._clean_name(col_name)
                    for col in table.columns:
                        if col.name == clean_col_name:
                            col.is_primary_key = True
                            break
        
        if name:
            table.primary_key_name = self._clean_name(name)

    def _extract_foreign_key(self, tokens, table: Table, name: str):
        # Find columns
        cols = []
        ref_table = None
        ref_cols = []
        on_delete = None
        on_update = None
        
        # Scan for parenthesis (cols) and REFERENCES
        current_phase = 'cols' # cols -> references -> ref_cols -> actions
        
        # Scan for parenthesis (cols) and REFERENCES
        current_phase = 'cols' # cols -> references -> ref_cols -> actions
        
        i = 0
        while i < len(tokens):
            token = tokens[i]
            
            # Check for ON DELETE / ON UPDATE
            # ... (omitted)
            
            # (Loop logic continues...)
            
            if isinstance(token, sqlparse.sql.Parenthesis):
                # ...
                pass
            elif token.value.upper() == 'REFERENCES':
                current_phase = 'ref_table'
            elif current_phase == 'ref_table':
                # Handle schema.table or just table
                if isinstance(token, Identifier):
                    # Identifier might contain schema.table
                    # sqlparse handles this well usually
                    ref_table = token.get_real_name()
                    # Check if it has a parent (schema)
                    if token.get_parent_name():
                        ref_table = f"{token.get_parent_name()}.{ref_table}"
                    
                    current_phase = 'ref_cols'
                elif isinstance(token, sqlparse.sql.Function):
                    ref_table = token.get_real_name()
                    for param in token.get_parameters():
                        ref_cols.append(str(param))
                    # If we found cols in Function, we are done with ref_cols
                    current_phase = 'actions'
                elif token.ttype is sqlparse.tokens.Name or token.ttype is sqlparse.tokens.String.Symbol:
                    ref_table = token.value
                    current_phase = 'ref_cols'
                # Handle generic unknown tokens if needed
            
            # Check for ON DELETE / ON UPDATE
            # These are multi-token usually: ON then DELETE/UPDATE
            if token.value.upper() == 'ON':
                if i + 2 < len(tokens):
                    action_type = tokens[i+1].value.upper()
                    if action_type in ('DELETE', 'UPDATE'):
                        # Get action (CASCADE, SET NULL, etc)
                        # Action might be 1 or 2 tokens (SET NULL)
                        action_val = tokens[i+2].value.upper()
                        offset = 3
                        
                        if action_val == 'SET' and i + 3 < len(tokens) and tokens[i+3].value.upper() in ('NULL', 'DEFAULT'):
                            action_val += f" {tokens[i+3].value.upper()}"
                            offset = 4
                        elif action_val == 'NO' and i + 3 < len(tokens) and tokens[i+3].value.upper() == 'ACTION':
                            action_val += " ACTION"
                            offset = 4
                            
                        if action_type == 'DELETE':
                            on_delete = action_val
                        else:
                            on_update = action_val
                            
                        i += offset
                        continue
            
            if isinstance(token, sqlparse.sql.Parenthesis):
                # Extract names from parenthesis
                names = []
                # Remove parens
                content_tokens_p = [t for t in token.tokens if not t.is_whitespace and not t.value in ('(', ')')]
                # Usually sqlparse leaves ( and ) in tokens list of Parenthesis
                
                # Use flatten approach or specific iteration
                for t in content_tokens_p:
                    if isinstance(t, IdentifierList):
                         for id_token in t.get_identifiers():
                             names.append(id_token.get_real_name())
                    elif isinstance(t, Identifier):
                        names.append(t.get_real_name())
                    elif t.ttype is sqlparse.tokens.Name:
                        names.append(t.value)
                    elif t.ttype is sqlparse.tokens.String.Symbol: # "quoted"
                         names.append(t.value)
                
                if current_phase == 'cols':
                    cols = names
                    current_phase = 'references'
                elif current_phase == 'ref_cols':
                    ref_cols = names
                    current_phase = 'actions' # explicit actions next
                    
            elif token.value.upper() == 'REFERENCES':
                current_phase = 'ref_table'
            elif current_phase == 'ref_table':
                # Handle schema.table or just table
                if isinstance(token, Identifier):
                    # Identifier might contain schema.table
                    # sqlparse handles this well usually
                    ref_table = token.get_real_name()
                    # Check if it has a parent (schema)
                    if token.get_parent_name():
                        ref_table = f"{token.get_parent_name()}.{ref_table}"
                    
                    current_phase = 'ref_cols'
                elif isinstance(token, sqlparse.sql.Function):
                    ref_table = token.get_real_name()
                    for param in token.get_parameters():
                        ref_cols.append(str(param))
                    # If we found cols in Function, we are done with ref_cols
                    current_phase = 'actions'
                elif token.ttype is sqlparse.tokens.Name or token.ttype is sqlparse.tokens.String.Symbol:
                    ref_table = token.value
                    current_phase = 'ref_cols'
                # Handle simple dot notation if sqlparse didn't group it (rare but possible)
                # e.g. schema . table
                # This logic is complex to do in a simple loop without lookahead
                # But Identifier usually catches it.
            
            i += 1

        if cols and ref_table:
            # If ref_cols is empty, it implies PK of ref_table, but we can't know that here easily without looking up ref_table
            # For now, we store what we found.
            fk = ForeignKey(
                name=self._clean_name(name),
                column_names=[self._clean_name(c) for c in cols],
                ref_table=self._clean_name(ref_table),
                ref_column_names=[self._clean_name(c) for c in ref_cols],
                on_delete=on_delete,
                on_update=on_update
            )
            table.foreign_keys.append(fk)

    def _extract_unique_constraint(self, parenthesis, table: Table, name: str):
        content = parenthesis.tokens[1:-1]
        cols = []
        for t in content:
            col_names = []
            if isinstance(t, IdentifierList):
                for id_token in t.get_identifiers():
                    col_names.append(id_token.get_real_name())
            elif isinstance(t, Identifier):
                col_names.append(t.get_real_name())
            elif t.ttype is sqlparse.tokens.Name:
                col_names.append(t.value)
            
            for col_name in col_names:
                if col_name:
                    cols.append(self._clean_name(col_name))
        
        if cols:
            # Add as Unique Index
            index = Index(name=self._clean_name(name), columns=cols, is_unique=True)
            table.indexes.append(index)

    def _extract_check_constraint(self, parenthesis, table: Table, name: str):
        # Extract expression inside parenthesis
        # parenthesis.value includes outer parens, so strip them
        expr = parenthesis.value[1:-1].strip()
        
        # Add Check Constraint
        from schemaforge.models import CheckConstraint
        check = CheckConstraint(name=self._clean_name(name), expression=expr)
        table.check_constraints.append(check)

    def _process_column(self, tokens, table: Table):
        # Filter out whitespace tokens for easier processing
        filtered_tokens = [t for t in tokens if not t.is_whitespace]
        
        if not filtered_tokens:
            return

        col_name = filtered_tokens[0].value
        if isinstance(filtered_tokens[0], Identifier):
             col_name = filtered_tokens[0].get_real_name()
             
        if len(filtered_tokens) > 1:
            data_type = filtered_tokens[1].value
            next_idx = 2
            
            # Handle multi-word types like CHARACTER VARYING, DOUBLE PRECISION
            if len(filtered_tokens) > 2 and not isinstance(filtered_tokens[2], sqlparse.sql.Parenthesis):
                 second_token_val = filtered_tokens[2].value.upper()
                 combined = data_type.upper() + " " + second_token_val
                 if data_type.upper() == 'CHARACTER' and second_token_val.startswith('VARYING'):
                     data_type = 'VARCHAR'
                     next_idx = 3
                 elif data_type.upper() == 'DOUBLE' and second_token_val == 'PRECISION':
                     data_type = 'DOUBLE PRECISION'
                     next_idx = 3
                 # ARRAY types (e.g. TEXT [])
                 elif second_token_val == '[]' or second_token_val == 'ARRAY':
                     data_type = data_type + '[]'
                     next_idx = 3

            # Check for qualified types (e.g. schema.type)
            while next_idx < len(filtered_tokens):
                curr = filtered_tokens[next_idx]
                if curr.value == '.':
                    data_type += '.'
                    next_idx += 1
                    if next_idx < len(filtered_tokens):
                         data_type += filtered_tokens[next_idx].value
                         next_idx += 1
                else:
                    break

            # Check for ARRAY keyword or [] suffix attached or tokenized
            # sqlparse might produce "TEXT" then "[" "]"
            if next_idx < len(filtered_tokens) and filtered_tokens[next_idx].value == '[':
                 if next_idx + 1 < len(filtered_tokens) and filtered_tokens[next_idx+1].value == ']':
                      data_type += '[]'
                      next_idx += 2
            
            # Handle type parameters like VARCHAR(100)
            if len(filtered_tokens) > next_idx and isinstance(filtered_tokens[next_idx], sqlparse.sql.Parenthesis):
                 data_type += filtered_tokens[next_idx].value
                 next_idx += 1 # Increment next_idx after consuming parenthesis
            
            # Use raw data type for Arrays if detected, to avoid cleaning TEXT[] -> VARCHAR
            # Use raw data type for Arrays if detected, to avoid cleaning TEXT[] -> VARCHAR
            if '[]' in data_type:
                final_data_type = data_type.upper()
            else:
                final_data_type = self._clean_type(data_type)
            
            column = Column(name=self._clean_name(col_name), data_type=final_data_type)
            
            full_def = " ".join([t.value for t in tokens]).upper()
            # Normalize whitespace to handle "PRIMARY  KEY"
            full_def = " ".join(full_def.split())
            
            if 'NOT NULL' in full_def:
                column.is_nullable = False
            if 'PRIMARY KEY' in full_def:
                column.is_primary_key = True
                
            # Parse DEFAULT, COMMENT, COLLATE, UNIQUE, REFERENCES
            for i, token in enumerate(filtered_tokens):
                val_upper = token.value.upper()
                
                # Check for DEFAULT
                if val_upper == 'DEFAULT':
                    # Collect tokens until end or next constraint keyword
                    default_val_tokens = []
                    next_idx_default = i + 1 # Use a separate index for default value parsing
                    while next_idx_default < len(filtered_tokens):
                        next_token = filtered_tokens[next_idx_default]
                        next_val_upper = next_token.value.upper()
                        # Keywords that might terminate a DEFAULT clause
                        if next_val_upper in ('NOT', 'NULL', 'PRIMARY', 'KEY', 'UNIQUE', 'CHECK', 'REFERENCES', 'CONSTRAINT', 'GENERATED', 'AUTO_INCREMENT', 'COMMENT', 'COLLATE', 'WITH', 'MASKING'):
                            # Special handling for 'NOT NULL' and 'PRIMARY KEY' as they are multi-word
                            if next_val_upper == 'NOT' and next_idx_default + 1 < len(filtered_tokens) and filtered_tokens[next_idx_default+1].value.upper() == 'NULL':
                                break
                            if next_val_upper == 'PRIMARY' and next_idx_default + 1 < len(filtered_tokens) and filtered_tokens[next_idx_default+1].value.upper() == 'KEY':
                                break
                            # For other single keywords, break
                            if next_val_upper in ('UNIQUE', 'CHECK', 'REFERENCES', 'CONSTRAINT', 'GENERATED', 'AUTO_INCREMENT', 'COMMENT', 'COLLATE', 'WITH', 'MASKING'):
                                break
                        
                        default_val_tokens.append(next_token.value)
                        next_idx_default += 1
                    
                    if default_val_tokens:
                        column.default_value = " ".join(default_val_tokens)
                
                # Check for UNIQUE
                if val_upper == 'UNIQUE':
                    # Add unique index for this column
                    idx_name = f"uk_{table.name}_{column.name}"
                    index = Index(name=self._clean_name(idx_name), columns=[column.name], is_unique=True)
                    table.indexes.append(index)

                # Check for REFERENCES (Foreign Key)
                if val_upper == 'REFERENCES':
                    # REFERENCES ref_table (ref_col)
                    if i + 1 < len(filtered_tokens):
                        ref_table_token = filtered_tokens[i+1]
                        ref_table_name = ref_table_token.value
                        ref_col_name = None
                        
                        if isinstance(ref_table_token, Identifier):
                            # Check if Identifier contains parenthesis (e.g. schema.table(col))
                            token_val = ref_table_token.value
                            if '(' in token_val and token_val.endswith(')'):
                                # Extract table and col
                                parts = token_val.split('(')
                                ref_table_name = parts[0]
                                ref_col_name = parts[1][:-1] # Remove trailing )
                                
                                # Clean table name (handle schema)
                                # We can rely on sqlparse for the base name but we need to handle the schema part manually if we split the string
                                # Actually, let's just use the string splitting as it's more reliable for this specific case where sqlparse grouped it all
                                pass
                            else:
                                ref_table_name = ref_table_token.get_real_name()
                                if ref_table_token.get_parent_name():
                                    ref_table_name = f"{ref_table_token.get_parent_name()}.{ref_table_name}"
                        
                        # Check for ref_col in separate parenthesis if not found in Identifier
                        if not ref_col_name and i + 2 < len(filtered_tokens) and isinstance(filtered_tokens[i+2], sqlparse.sql.Parenthesis):
                            # Extract column
                            content = filtered_tokens[i+2].value[1:-1]
                            ref_col_name = self._clean_name(content)
                        
                        if ref_table_name:
                             fk_name = f"fk_{table.name}_{column.name}"
                             ref_cols = [self._clean_name(ref_col_name)] if ref_col_name else [] 
                             
                             fk = ForeignKey(
                                name=self._clean_name(fk_name),
                                column_names=[column.name],
                                ref_table=self._clean_name(ref_table_name),
                                ref_column_names=ref_cols
                             )
                             table.foreign_keys.append(fk)

                # Check for COMMENT
                if val_upper == 'COMMENT':
                    if i + 1 < len(filtered_tokens):
                        # Comments can be single quoted strings or dollar-quoted strings
                        comment_token = filtered_tokens[i+1]
                        if comment_token.ttype is sqlparse.tokens.String.Single:
                            column.comment = comment_token.value.strip("'")
                        elif comment_token.ttype is sqlparse.tokens.String.Dollar:
                            column.comment = comment_token.value.strip("$$")
                        else:
                            column.comment = comment_token.value # Fallback for unquoted comments if any
                        
                # Check for COLLATE
                if val_upper == 'COLLATE':
                    if i + 1 < len(filtered_tokens):
                        column.collation = filtered_tokens[i+1].value.strip("'")

                # Check for MASKING POLICY (Snowflake)
                # Syntax: [WITH] MASKING POLICY policy_name
                if val_upper == 'MASKING':
                    # Check if next is POLICY
                    if i + 1 < len(filtered_tokens) and filtered_tokens[i+1].value.upper() == 'POLICY':
                        if i + 2 < len(filtered_tokens):
                            column.masking_policy = filtered_tokens[i+2].value
                
                # Check for WITH MASKING POLICY
                if val_upper == 'WITH':
                    if i + 1 < len(filtered_tokens) and filtered_tokens[i+1].value.upper() == 'MASKING':
                         if i + 2 < len(filtered_tokens) and filtered_tokens[i+2].value.upper() == 'POLICY':
                             if i + 3 < len(filtered_tokens):
                                 column.masking_policy = filtered_tokens[i+3].value

                if val_upper.startswith('IDENTITY') or val_upper.startswith('AUTOINCREMENT'):
                    column.is_identity = True
                    # Consume optional (start, inc)
                    if i + 1 < len(filtered_tokens) and isinstance(filtered_tokens[i+1], sqlparse.sql.Parenthesis):
                        # Parse start/inc
                        content = filtered_tokens[i+1].value.strip('()')
                        parts = [p.strip() for p in content.split(',')]
                        if len(parts) >= 1 and parts[0].isdigit():
                            column.identity_start = int(parts[0])
                        if len(parts) >= 2 and parts[1].isdigit():
                            column.identity_step = int(parts[1])

                # Check for inline CHECK (expr)
                if val_upper == 'CHECK':
                    if i + 1 < len(filtered_tokens) and isinstance(filtered_tokens[i+1], sqlparse.sql.Parenthesis):
                        expr = filtered_tokens[i+1].value[1:-1].strip()
                        from schemaforge.models import CheckConstraint
                        # We attribute this check to the table, but it's defined inline
                        check_name = f"ck_{table.name}_{column.name}_{len(table.check_constraints)}"
                        table.check_constraints.append(CheckConstraint(name=check_name, expression=expr))

            table.columns.append(column)

    def _clean_name(self, name: str) -> str:
        # Normalize to lower case and strip quotes for comparison
        # Also strip invisible characters like Zero Width Space (U+200B)
        name = name.replace(chr(0x200b), '')
        
        # If quoted, preserve case but strip quotes
        if name.startswith('"') and name.endswith('"'):
            return name[1:-1]
        
        return name.strip('`"[] ').lower()

    def _clean_type(self, data_type: str) -> str:
        # Remove invisible characters
        data_type = data_type.replace(chr(0x200b), '')
        
        # Handle $$ quoting for defaults or types (Snowflake/Postgres)
        if data_type.startswith('$$') and data_type.endswith('$$'):
            data_type = data_type[2:-2]
            
        # Remove whitespace inside type definition e.g. VARCHAR ( 100 ) -> VARCHAR(100)
        # This is a simple regex replacement or string manipulation
        dt = data_type.upper()
        # Remove all whitespace
        dt = "".join(dt.split())
        
        # Extract base type and parameters
        base_type = dt.split('(')[0]
        suffix = ""
        if '(' in dt:
            suffix = dt[dt.find('('):]
            
        # Common Aliases
        aliases = {
            'INTEGER': 'INT',
            'STRING': 'VARCHAR',
            'CHARACTERVARYING': 'VARCHAR', # Whitespace removed
            'DECIMAL': 'NUMERIC',
            'BOOL': 'TINYINT(1)', # MySQL
            'BOOLEAN': 'TINYINT(1)', # MySQL
            'TEXT': 'VARCHAR', # Snowflake/Postgres
            'FLOAT8': 'DOUBLEPRECISION', # Postgres
            'REAL': 'FLOAT',
            'BYTEINT': 'TINYINT', # Snowflake
            'BOOLISH': 'TINYINT(1)', # Custom alias for God Mode (maps to BOOLEAN -> TINYINT(1))
            'DOUBLEPRECISION': 'FLOAT', # Normalize to FLOAT
        }
        
        # Oracle specific: INT -> NUMBER(38)
        # This is tricky because INT is generic. 
        # For now, if we see INT, we map to INT. 
        # But if we compare INT vs NUMBER(38), we need them to match.
        # Let's map INT to NUMBER(38) only if we are sure? 
        # Or map NUMBER(38) to INT? 
        # NUMBER(38) is very specific. INT is generic.
        # Let's map NUMBER(38) to INT for normalization if suffix is (38)
        
        if base_type == 'NUMBER' and suffix == '(38)':
             return 'INT'
        
        if base_type in aliases:
            return aliases[base_type] + suffix
            
        return dt

    def _strip_comments(self, sql: str) -> str:
        result = []
        i = 0
        n = len(sql)
        nesting = 0
        in_quote = False
        quote_char = None
        in_dollar_quote = False
        in_line_comment = False
        
        while i < n:
            char = sql[i]
            next_char = sql[i+1] if i + 1 < n else ''
            
            if in_line_comment:
                if char == '\n':
                    in_line_comment = False
                    result.append(char)
                i += 1
                continue
                
            if in_quote:
                result.append(char)
                if char == quote_char:
                    if quote_char == "'" and next_char == "'":
                        result.append(next_char)
                        i += 2
                        continue
                    in_quote = False
                i += 1
                continue
                
            if in_dollar_quote:
                result.append(char)
                if char == '$' and next_char == '$':
                    result.append(next_char)
                    in_dollar_quote = False
                    i += 2
                    continue
                i += 1
                continue
                
            # Check for start of string
            if char in ("'", '"', '`'):
                in_quote = True
                quote_char = char
                result.append(char)
                i += 1
                continue
                
            # Check for $$
            if char == '$' and next_char == '$':
                in_dollar_quote = True
                result.append(char)
                result.append(next_char)
                i += 2
                continue
                
            # Check for line comment
            if char == '-' and next_char == '-':
                in_line_comment = True
                i += 2
                continue
                
            # Check for block comment start
            if char == '/' and next_char == '*':
                nesting += 1
                i += 2
                continue
                
            # Check for block comment end
            if char == '*' and next_char == '/':
                if nesting > 0:
                    nesting -= 1
                    i += 2
                    continue
                
            if nesting > 0:
                i += 1
                continue
                
            result.append(char)
            i += 1
            
        return "".join(result)

    def _process_comment(self, statement, schema: Schema):
        # COMMENT ON (TABLE|COLUMN|DATABASE|INDEX|CONSTRAINT) name IS 'comment'
        tokens = [t for t in statement.tokens if not t.is_whitespace and not isinstance(t, sqlparse.sql.Comment)]
        if len(tokens) < 6: return
        
        target_type = tokens[2].value.upper()
        # Use .value to preserve schema qualification (e.g. titan_db_core.foo)
        target_name = tokens[3].value
        
        # Determine comment text
        # usually IS 'text'
        comment_text = None
        for i, t in enumerate(tokens):
            if t.value.upper() == 'IS':
                if i + 1 < len(tokens):
                    comment_text = tokens[i+1].value.strip("'")
                break
                
        if not comment_text: return
        
        target_name = self._clean_name(target_name)
        
        if target_type == 'TABLE':
            table = schema.get_table(target_name)
            if table:
                table.comment = comment_text
        elif target_type == 'DATABASE':
            # Schema doesn't have a database comment field on root usually.
            # But we can store it in custom objects as a "Database Property"?
            # Or just ignore gracefully?
            # Test expects "Comment" in output.
            # We can use a CustomObject for DATABASE properties or add to Schema
            from schemaforge.models import CustomObject
            schema.custom_objects.append(CustomObject(obj_type='COMMENT', name=target_name, properties={'comment': comment_text}))
        elif target_type == 'CONSTRAINT':
            # Constraints might be on a table. But finding the constraint object requires searching all tables?
            # Or assume schema.table.constraint? Usually just constraint_name on table_name.
            # "COMMENT ON CONSTRAINT name ON table IS ..."
            # My tokens check is simple. If tokens[4] == ON, tokens[5] == table.
            table_name = None
            con_name = target_name
            if len(tokens) > 5 and tokens[4].value.upper() == 'ON':
                 table_name = tokens[5].value
            
            if table_name:
                 # Resolve table
                 table = schema.get_table(table_name)
                 if table:
                     # Check all constraints
                     for c in table.check_constraints:
                         if c.name == con_name: c.comment = comment_text; break
                     for c in table.exclusion_constraints:
                         if c.name == con_name: c.comment = comment_text; break
                     # Foreign Keys? Primary Key? Unique?
                     # Models for FK/PK need comment field too? FK has it.
                     for c in table.foreign_keys:
                         if c.name == con_name: c.comment = comment_text; break
                         
        elif target_type == 'INDEX':
             # Index name might be schema qualified
             # Try exact match first
             for table in schema.tables:
                 for idx in table.indexes:
                     if idx.name == target_name:
                         idx.comment = comment_text
                         return

             # Try suffix match
             parts = target_name.split('.')
             idx_name = parts[-1]
             # Search all tables for this index
             for table in schema.tables:
                 for idx in table.indexes:
                     if idx.name == idx_name or idx.name.endswith(f".{idx_name}"):
                         idx.comment = comment_text
                         return
        elif target_type == 'COLUMN':
            # name format schema.table.col or table.col
            parts = target_name.split('.')
            if len(parts) >= 2:
                col_name = parts[-1]
                table_name = ".".join(parts[:-1]) # Handle schema.table correctly
                table = schema.get_table(table_name)
                if table:
                    for col in table.columns:
                        if col.name == col_name:
                            col.comment = comment_text
                            break
