from schemaforge.parsers.sqlglot_adapter import SqlglotParser
import re

class OracleParser(SqlglotParser):
    def __init__(self, strict=False):
        super().__init__(dialect='oracle', strict=strict)

    def parse(self, content: str):
        # Clean-the-Parse strategy for Oracle
        # sqlglot 28.6 fails on ORGANIZATION INDEX, STORAGE, PCTFREE, etc.
        
        # 1. Detect properties for each CREATE TABLE (global regex is risky but often works for DDL files)
        # We'll use a local parse loop instead
        return super().parse(content)

    def _preprocess(self, content: str) -> str:
        # Strip problematic Oracle-specific keywords that cause sqlglot to flip to Command
        orig_content = content
        
        # Capture and strip
        # Note: We don't store them here because preprocess doesn't know which table is which easily
        # But we'll rely on parse() catch them or re-regex them in _extract_create_table
        
        content = re.sub(r'\s+ORGANIZATION\s+INDEX', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\s+PCTFREE\s+\d+', '', content, flags=re.IGNORECASE)
        content = re.sub(r'\s+STORAGE\s*\(.*?\)', '', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'\s+TABLESPACE\s+[^\s;]+', '', content, flags=re.IGNORECASE)
        
        return super()._preprocess(content)

    def _extract_create_table(self, expression):
        table = super()._extract_create_table(expression)
        if table and hasattr(self, 'raw_content'):
             # Find the CREATE TABLE statement for THIS table in the raw content
             # This is a bit complex in a multi-table file, but we can search for 
             # CREATE TABLE table_name followed by its properties.
             clean_name = table.name.replace('"', '').replace('`', '').strip()
             pattern = rf'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:["`]?\w+["`]?\.)?["`]?{re.escape(clean_name)}["`]?.*?;'
             match = re.search(pattern, self.raw_content, re.IGNORECASE | re.DOTALL)
             if match:
                  stmt = match.group(0)
                  if re.search('ORGANIZATION INDEX', stmt, re.IGNORECASE):
                       table.storage_parameters['iot'] = True
                  if re.search('PCTFREE', stmt, re.IGNORECASE):
                       m = re.search(r'PCTFREE\s+(\d+)', stmt, re.IGNORECASE)
                       if m:
                            table.storage_parameters['pctfree'] = int(m.group(1))
                  if re.search('STORAGE', stmt, re.IGNORECASE):
                       m = re.search(r'STORAGE\s*(\(.*?\))', stmt, re.DOTALL)
                       if m:
                            table.storage_parameters['storage'] = m.group(1)
                  if re.search('TABLESPACE', stmt, re.IGNORECASE):
                       m = re.search(r'TABLESPACE\s+([^\s;)]+)', stmt, re.IGNORECASE)
                       if m:
                            table.tablespace = m.group(1).replace('"', '').strip().lower()
        return table

    def _clean_type(self, data_type: str) -> str:
        dt = data_type.upper()
        if 'VARCHAR2' in dt:
            return dt.replace('VARCHAR2', 'VARCHAR')
        if dt == 'NUMBER':
            return 'DECIMAL'
        return data_type



