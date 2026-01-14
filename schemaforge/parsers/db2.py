from schemaforge.parsers.sqlglot_adapter import SqlglotParser
from sqlglot import exp
from schemaforge.models import Table, Column, Schema, Index
import re

class DB2Parser(SqlglotParser):
    def __init__(self, strict=False):
        # sqlglot doesn't have a specific 'db2' dialect shorthand in 28.x Dialect.classes
        # We use generic parsing (None) as a base
        super().__init__(dialect=None, strict=strict)

    def _extract_create_table(self, expression):
        table = super()._extract_create_table(expression)
        if table:
            # self.raw_content is stored by parse()
            self._process_db2_properties(self.raw_content, table)
        return table

    def _post_process_table(self, table, expression):
        # Called for Command fallbacks
        self._process_db2_properties(self.raw_content, table)

    def _process_db2_properties(self, stmt_str, table):
        # Normalize whitespace for easier regex matching
        normalized_stmt = " ".join(stmt_str.split())
        
        # STOGROUP
        match_sto = re.search(r'USING\s+STOGROUP\s+([a-zA-Z0-9_"]+)', normalized_stmt, re.IGNORECASE)
        if match_sto:
            table.stogroup = match_sto.group(1).replace('"', '').lower()
            
        # PRIQTY
        match_pri = re.search(r'PRIQTY\s+(\d+)', normalized_stmt, re.IGNORECASE)
        if match_pri:
            table.priqty = int(match_pri.group(1))
            
        # SECQTY
        match_sec = re.search(r'SECQTY\s+(\d+)', normalized_stmt, re.IGNORECASE)
        if match_sec:
            table.secqty = int(match_sec.group(1))
            
        # AUDIT
        match_audit = re.search(r'AUDIT\s+(NONE|CHANGES|ALL)', normalized_stmt, re.IGNORECASE)
        if match_audit:
            table.audit = match_audit.group(1).lower()
            
        # CCSID
        match_ccsid = re.search(r'CCSID\s+(EBCDIC|ASCII|UNICODE)', normalized_stmt, re.IGNORECASE)
        if match_ccsid:
            table.ccsid = match_ccsid.group(1).lower()

        # IN clause (DB.TS)
        match_in = re.search(r'\sIN\s+(?:DATABASE\s+)?([a-zA-Z0-9_".]+)', normalized_stmt, re.IGNORECASE)
        if match_in:
            val = match_in.group(1).replace('"', '')
            if '.' in val:
                parts = val.split('.')
                table.database_name = parts[0].lower()
                table.tablespace = parts[1].lower()
            else:
                table.tablespace = val.lower()

    def _extract_create_index(self, expression, schema, is_unique=False):
        super()._extract_create_index(expression, schema, is_unique=is_unique)
        
        # Post-process for INCLUDE clause which sqlglot generic might miss
        raw_sql = expression.sql(comments=False)
        match = re.search(r'INCLUDE\s*\((.*?)\)', raw_sql, re.IGNORECASE)
        if match:
             include_cols = [c.strip().replace('"', '').replace('[', '').replace(']', '').lower() for c in match.group(1).split(',')]
             
             # The index was just added to some table. Let's find it.
             # We can get the table name from the expression.
             index_node = expression.this
             if isinstance(index_node, exp.Index):
                  table_node = index_node.args.get("table")
                  if table_node:
                       table_name = table_node.this.name if hasattr(table_node, 'this') else table_node.name
                       table = schema.get_table(table_name)
                       if table and table.indexes:
                            # Assuming it was the last index added
                            table.indexes[-1].include_columns = include_columns = include_cols
                            # Also store in properties for backward compatibility if needed by tests
                            table.indexes[-1].properties['include_columns'] = include_cols




