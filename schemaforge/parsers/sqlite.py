from schemaforge.parsers.sqlglot_adapter import SqlglotParser

class SQLiteParser(SqlglotParser):
    def __init__(self, strict=False):
        super().__init__(dialect='sqlite', strict=strict)

    def _process_property(self, prop, table):
        from sqlglot import exp
        # SQLite properties usually show up as generic properties or specific ones
        # sqlglot might parse WITHOUT ROWID as a property
        prop_sql = prop.sql().upper()
        if 'WITHOUT ROWID' in prop_sql:
             table.without_rowid = True
        if 'STRICT' in prop_sql:
             table.is_strict = True

    def _clean_type(self, data_type: str) -> str:
        dt = data_type.upper()
        if dt == 'INTEGER':
            return 'INT'
        if dt.startswith('TEXT'): 
            return dt.replace('TEXT', 'VARCHAR')
        return data_type
