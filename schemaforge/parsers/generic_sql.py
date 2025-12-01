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
        
        for statement in parsed:
            stmt_type = statement.get_type()
            if stmt_type == 'CREATE':
                # Check if it is CREATE TABLE or CREATE INDEX
                # Filter out whitespace AND comments to find the real keywords
                token_list = [t for t in statement.tokens if not t.is_whitespace and not isinstance(t, sqlparse.sql.Comment)]
                
                # token_list[0] should be CREATE
                if len(token_list) > 1 and token_list[1].value.upper() == 'TABLE':
                    table = self._extract_create_table(statement)
                    if table:
                        schema.tables.append(table)
                elif len(token_list) > 1 and token_list[1].value.upper() == 'INDEX':
                    self._extract_create_index(statement, schema)
                elif len(token_list) > 2 and token_list[1].value.upper() == 'UNIQUE' and token_list[2].value.upper() == 'INDEX':
                     self._extract_create_index(statement, schema, is_unique=True)
                     
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
        
        for token in statement.tokens:
            if isinstance(token, Identifier):
                table_name = token.get_real_name()
                break
                
        if not table_name:
            return None
            
        print(f"DEBUG: Extracted Table Name: {table_name}, Cleaned: {self._clean_name(table_name)}")
        table = Table(name=self._clean_name(table_name))
        
        _, parenthesis = statement.token_next_by(i=sqlparse.sql.Parenthesis)
        
        if parenthesis:
            self._parse_columns_and_constraints(parenthesis, table)
            
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
        if first_word in ('ORGANIZE', 'CLUSTER', 'ENGINE', 'COMMENT', 'WITH', 'PARTITION'):
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
                 content = content_token.tokens[1:-1]
                 for t in content:
                     col_name = None
                     if isinstance(t, Identifier):
                         col_name = t.get_real_name()
                     elif t.ttype is sqlparse.tokens.Name:
                         col_name = t.value
                     
                     if col_name:
                         # Find column and mark as PK
                         clean_col_name = self._clean_name(col_name)
                         for col in table.columns:
                             if col.name == clean_col_name:
                                 col.is_primary_key = True
                                 break
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
                    self._extract_pk_cols(tokens[type_idx+2], table)
            # Case 2: PRIMARY KEY as single token
            elif constraint_type == 'PRIMARY KEY':
                # CONSTRAINT name PRIMARY KEY (cols)
                # tokens: CONSTRAINT, name, PRIMARY KEY, (cols)
                if len(tokens) > type_idx + 1 and isinstance(tokens[type_idx+1], sqlparse.sql.Parenthesis):
                    self._extract_pk_cols(tokens[type_idx+1], table)
        
        elif constraint_type == 'UNIQUE':
            # CONSTRAINT name UNIQUE (cols)
            if len(tokens) > type_idx + 1 and isinstance(tokens[type_idx+1], sqlparse.sql.Parenthesis):
                self._extract_unique_constraint(tokens[type_idx+1], table, constraint_name)
                
        elif constraint_type == 'CHECK':
            # CONSTRAINT name CHECK (expr)
            if len(tokens) > type_idx + 1 and isinstance(tokens[type_idx+1], sqlparse.sql.Parenthesis):
                self._extract_check_constraint(tokens[type_idx+1], table, constraint_name)

    def _extract_pk_cols(self, parenthesis, table: Table):
        content = parenthesis.tokens[1:-1]
        for t in content:
            col_name = None
            if isinstance(t, Identifier):
                col_name = t.get_real_name()
            elif t.ttype is sqlparse.tokens.Name:
                col_name = t.value
            
            if col_name:
                clean_col_name = self._clean_name(col_name)
                for col in table.columns:
                    if col.name == clean_col_name:
                        col.is_primary_key = True
                        break

    def _extract_foreign_key(self, tokens, table: Table, name: str):
        # Find columns
        cols = []
        ref_table = None
        ref_cols = []
        
        # Scan for parenthesis (cols) and REFERENCES
        current_phase = 'cols' # cols -> references -> ref_cols
        
        for token in tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                # Extract names from parenthesis
                names = []
                content = token.tokens[1:-1]
                for t in content:
                    if isinstance(t, IdentifierList):
                         for id_token in t.get_identifiers():
                             names.append(id_token.get_real_name())
                    elif isinstance(t, Identifier):
                        names.append(t.get_real_name())
                    elif t.ttype is sqlparse.tokens.Name:
                        names.append(t.value)
                
                if current_phase == 'cols':
                    cols = names
                    current_phase = 'references'
                elif current_phase == 'ref_cols':
                    ref_cols = names
                    
            elif token.value.upper() == 'REFERENCES':
                current_phase = 'ref_table'
            elif current_phase == 'ref_table':
                if isinstance(token, Identifier):
                    ref_table = token.get_real_name()
                    current_phase = 'ref_cols'
                elif isinstance(token, sqlparse.sql.Function):
                    ref_table = token.get_real_name()
                    for param in token.get_parameters():
                        ref_cols.append(str(param))
                    # If we found cols in Function, we are done with ref_cols
                    current_phase = 'done' 
                elif token.ttype is sqlparse.tokens.Name:
                    ref_table = token.value
                    current_phase = 'ref_cols'

        if cols and ref_table and ref_cols:
            fk = ForeignKey(
                name=self._clean_name(name),
                column_names=[self._clean_name(c) for c in cols],
                ref_table=self._clean_name(ref_table),
                ref_column_names=[self._clean_name(c) for c in ref_cols]
            )
            table.foreign_keys.append(fk)

    def _extract_unique_constraint(self, parenthesis, table: Table, name: str):
        content = parenthesis.tokens[1:-1]
        cols = []
        for t in content:
            col_name = None
            if isinstance(t, Identifier):
                col_name = t.get_real_name()
            elif t.ttype is sqlparse.tokens.Name:
                col_name = t.value
            
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
                     data_type = data_type + " " + filtered_tokens[2].value
                     next_idx = 3
                 elif data_type.upper() == 'DOUBLE' and second_token_val.startswith('PRECISION'):
                     data_type = data_type + " " + filtered_tokens[2].value
                     next_idx = 3
            
            if len(filtered_tokens) > next_idx and isinstance(filtered_tokens[next_idx], sqlparse.sql.Parenthesis):
                 data_type += filtered_tokens[next_idx].value
                 
            column = Column(name=self._clean_name(col_name), data_type=self._clean_type(data_type))
            
            full_def = " ".join([t.value for t in tokens]).upper()
            # Normalize whitespace to handle "PRIMARY  KEY"
            full_def = " ".join(full_def.split())
            
            if 'NOT NULL' in full_def:
                column.is_nullable = False
            if 'PRIMARY KEY' in full_def:
                column.is_primary_key = True
                
            # Parse DEFAULT value and COMMENT
            for i, token in enumerate(filtered_tokens):
                # Check for DEFAULT
                if token.value.upper() == 'DEFAULT':
                    # Collect tokens until end or next constraint keyword
                    default_val_tokens = []
                    next_idx = i + 1
                    while next_idx < len(filtered_tokens):
                        next_token = filtered_tokens[next_idx]
                        val_upper = next_token.value.upper()
                        # Keywords that might terminate a DEFAULT clause
                        if val_upper in ('NOT', 'NULL', 'PRIMARY', 'KEY', 'UNIQUE', 'CHECK', 'REFERENCES', 'CONSTRAINT', 'GENERATED', 'AUTO_INCREMENT', 'COMMENT'):
                            # Special handling for 'NOT NULL' and 'PRIMARY KEY' as they are multi-word
                            if val_upper == 'NOT' and next_idx + 1 < len(filtered_tokens) and filtered_tokens[next_idx+1].value.upper() == 'NULL':
                                break
                            if val_upper == 'PRIMARY' and next_idx + 1 < len(filtered_tokens) and filtered_tokens[next_idx+1].value.upper() == 'KEY':
                                break
                            # For other single keywords, break
                            if val_upper in ('UNIQUE', 'CHECK', 'REFERENCES', 'CONSTRAINT', 'GENERATED', 'AUTO_INCREMENT', 'COMMENT'):
                                break
                        
                        if not t.is_whitespace:
                            default_tokens.append(t.value)
                    
                    if default_tokens:
                        column.default_value = " ".join(default_tokens)
                
                # Check for COMMENT
                if token.value.upper() == 'COMMENT':
                    if i + 1 < len(filtered_tokens):
                        column.comment = filtered_tokens[i+1].value.strip("'")
                        
                # Check for COLLATE
                if token.value.upper() == 'COLLATE':
                    if i + 1 < len(filtered_tokens):
                        column.collation = filtered_tokens[i+1].value.strip("'")

            table.columns.append(column)

    def _clean_name(self, name: str) -> str:
        # Normalize to lower case and strip quotes for comparison
        # Also strip invisible characters like Zero Width Space (U+200B)
        name = name.replace(chr(0x200b), '')
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
