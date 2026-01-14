from schemaforge.parsers.sqlglot_adapter import SqlglotParser

class MySQLParser(SqlglotParser):
    def __init__(self, strict=False):
        super().__init__(dialect='mysql', strict=strict)
    
    def _process_property(self, prop, table):
        from sqlglot import exp
        prop_type = type(prop)
        if prop_type is exp.EngineProperty:
             table.engine = prop.this.name if hasattr(prop.this, 'name') else str(prop.this)
        elif prop_type is exp.AutoIncrementProperty:
             try:
                 table.auto_increment = int(str(prop.this))
             except:
                 pass
        elif "ROW_FORMAT" in str(prop).upper():
             table.row_format = str(prop.this)
        elif isinstance(prop, exp.PartitionedByProperty):
             partition_sql = prop.sql(dialect='mysql')
             # Strip leading "PARTITION BY " if present to avoid duplication in generator
             if partition_sql.upper().startswith('PARTITION BY '):
                 partition_sql = partition_sql[len('PARTITION BY '):]
             table.partition_by = partition_sql
