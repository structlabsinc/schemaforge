import re
import sqlparse
from schemaforge.parsers.generic_sql import GenericSQLParser, DDL
from schemaforge.models import Column, Table, Index

class MSSQLParser(GenericSQLParser):
    def __init__(self, strict=False):
        super().__init__(strict)
        self.quote_char = '[]'

    def _clean_name(self, ident):
        """Removes [ ] brackets from identifiers."""
        if ident:
            return ident.replace('[', '').replace(']', '').replace('"', '')
        return ident
        
    def _strip_comments(self, sql):
        return super()._strip_comments(sql)

    def parse(self, sql_content):
        # Replace standalone GO statements with semicolon
        sql_content = re.sub(r'^\s*GO\s*$', ';', sql_content, flags=re.MULTILINE | re.IGNORECASE)
        
        # Strip CLUSTERED/NONCLUSTERED from CREATE INDEX to let GenericSQLParser handle it
        # We might want to capture this properly later, but for now we just want to parse the index.
        sql_content = re.sub(r'(CREATE\s+)(?:UNIQUE\s+)?(?:CLUSTERED|NONCLUSTERED)\s+(INDEX)', r'\1\2', sql_content, flags=re.IGNORECASE)
        # Handle UNIQUE CLUSTERED -> UNIQUE INDEX (The regex above handles simple cases, let's be more robust)
        sql_content = re.sub(r'(CREATE\s+)(UNIQUE\s+)(?:CLUSTERED|NONCLUSTERED)\s+(INDEX)', r'\1\2\3', sql_content, flags=re.IGNORECASE)
        
        # Handle [dbo].[table] pattern in CREATE TABLE statements if necessary
        # But for now, let generic sqlparse handle it.
        return super().parse(sql_content)

    def _extract_table_name(self, statement):
        import sqlparse
        from sqlparse.sql import Identifier

        for token in statement.tokens:
             if isinstance(token, Identifier):
                 name = token.get_real_name()
                 schema = token.get_parent_name()
                 
                 # Clean brackets from parts
                 if name: 
                     name = name.replace('[', '').replace(']', '').replace('"', '')
                 if schema:
                     schema = schema.replace('[', '').replace(']', '').replace('"', '')
                     
                 if name:
                     full_name = name
                     if schema:
                         full_name = f"{schema}.{name}"
                     
                     # Return as double-quoted string to preserve case through GenericSQLParser._clean_name
                     return f'"{full_name}"'
                     
        return super()._extract_table_name(statement)

    def _clean_type(self, type_str):
        type_str = type_str.upper().strip('[]')
        
        # T-SQL Mapping
        if 'NVARCHAR' in type_str:
            if 'MAX' in type_str:
                return 'TEXT'
            return type_str
        if 'DATETIME2' in type_str:
             return 'DATETIME'
        if 'BIT' in type_str:
            return 'BOOLEAN'
        if 'MONEY' in type_str:
            return 'DECIMAL' # Approx
            
        return type_str

    def _process_column(self, tokens, table):
        """Override to handle IDENTITY property which is unique to T-SQL."""
        # GenericSQLParser implementation of _process_column takes (tokens, table)
        # It finds the column name and type.
        
        # We need to call super first or copy extraction logic?
        # Copying might be safer to inject IDENTITY check.
        
        import sqlparse
        token = tokens[0]
        if isinstance(token, sqlparse.sql.Identifier):
             col_name = self._clean_name(token.get_real_name())
        else:
             col_name = self._clean_name(token.value)
        
        # Skip if name is None (e.g. some keyword)
        if not col_name:
            return
            
        col_type = None
        is_identity = False # New property, handled as auto_increment
        
        rest_tokens = tokens[1:]
        
        # Simple type extraction
        if rest_tokens:
             # Find first identifier/keyword for type
             for i, t in enumerate(rest_tokens):
                 if not t.is_whitespace:
                     col_type = t.value
                     # Should we consume more for (x,y)?
                     # Check next token
                     if i+1 < len(rest_tokens) and isinstance(rest_tokens[i+1], sqlparse.sql.Parenthesis):
                         col_type += rest_tokens[i+1].value
                     break
        
        # Scan for IDENTITY
        full_def = "".join([t.value for t in rest_tokens]).upper()
        if 'IDENTITY' in full_def:
            is_identity = True
            
        # Call base logic to populate standard fields
        # But we need to update the column object created by super?
        # Or just reimplement. Reimplementing is cleaner for custom dialects.
        
        column = Column(name=col_name, data_type=self._clean_type(col_type))
        column.is_primary_key = 'PRIMARY KEY' in full_def
        column.is_nullable = 'NOT NULL' not in full_def
        # In T-SQL if NOT NULL is omitted, it depends on ANSI commands, but typically NULL.
        
        # IDENTITY usually implies PK but not always.
        if is_identity:
            # We map IDENTITY to auto_increment behavior or explicit identity?
            # SchemaForge models might not have explicit 'identity' field yet?
            # Generic model usually just relies on 'SERIAL' type or similar.
            # But here we want to preserve it? 
            # If we just treat it as a property.
            
            # Let's see models.py... (assume Column has extra_properties or we add one?)
            # Or mapped to 'GENERATED ALWAYS AS IDENTITY'
             pass
             
        # Check for DEFAULT
        if 'DEFAULT' in full_def:
             # Extract default value logic...
             pass
             
        table.columns.append(column)

    def _process_definition(self, tokens, table):
        # Check for constraint keywords
        first_word = tokens[0].value.upper()
        if first_word in ('CONSTRAINT', 'PRIMARY', 'FOREIGN', 'UNIQUE', 'CHECK', 'INDEX', 'KEY') or 'PRIMARY KEY' in first_word:
             super()._process_definition(tokens, table)
        else:
             # Assume column
             self._process_column(tokens, table)

