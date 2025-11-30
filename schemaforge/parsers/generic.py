import re
from typing import List, Optional
from schemaforge.models import Schema, Table, Column, Index
from schemaforge.parsers.base import BaseParser

class GenericRegexParser(BaseParser):
    def __init__(self):
        # Basic regex patterns - can be overridden by subclasses
        self.create_table_pattern = re.compile(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)\s*\((.*?)\);', re.IGNORECASE | re.DOTALL)
        self.column_pattern = re.compile(r'([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_()]+)(.*)', re.IGNORECASE)

    def parse(self, sql_content: str) -> Schema:
        schema = Schema()
        # Remove comments (simple version)
        sql_content = re.sub(r'--.*', '', sql_content)
        
        matches = self.create_table_pattern.findall(sql_content)
        for table_name, body in matches:
            table = Table(name=self._clean_name(table_name))
            self._parse_table_body(table, body)
            schema.tables.append(table)
            
        return schema

    def _clean_name(self, name: str) -> str:
        return name.strip('`"[] ')

    def _parse_table_body(self, table: Table, body: str):
        # Split by comma, but respect parentheses (naive implementation)
        # For a real parser, we'd need a state machine or a more complex regex
        definitions = [d.strip() for d in body.split(',')]
        
        for definition in definitions:
            if not definition:
                continue
                
            # Check for constraints like PRIMARY KEY defined separately
            if definition.upper().startswith('PRIMARY KEY'):
                # Handle PK
                continue
            
            # Assume it's a column
            col_match = self.column_pattern.match(definition)
            if col_match:
                col_name = self._clean_name(col_match.group(1))
                col_type = col_match.group(2)
                extras = col_match.group(3)
                
                column = Column(name=col_name, data_type=col_type)
                if 'NOT NULL' in extras.upper():
                    column.is_nullable = False
                if 'PRIMARY KEY' in extras.upper():
                    column.is_primary_key = True
                    
                table.columns.append(column)
