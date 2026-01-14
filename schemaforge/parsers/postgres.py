from schemaforge.parsers.sqlglot_adapter import SqlglotParser

class PostgresParser(SqlglotParser):
    def __init__(self, strict=False):
        super().__init__(dialect='postgres', strict=strict)

    def _process_property(self, prop, table):
        from sqlglot import exp
        prop_type = type(prop).__name__
        
        if prop_type == 'UnloggedProperty':
             table.is_unlogged = True
        elif prop_type == 'PartitionedByProperty':
             # Extract partition expression
             # usually: PARTITION BY method(cols)
             # prop.this is typically the expression
             table.partition_by = prop.sql(dialect='postgres')
        elif prop_type == 'InheritsProperty':
             # Create string list of tables
             tables = [t.this.name for t in prop.expressions]
             table.inherits = ", ".join(tables)
        elif prop_type == 'TableSampleProperty': # example
             pass
