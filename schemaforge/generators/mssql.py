from schemaforge.generators.generic import GenericGenerator
from schemaforge.models import Table, Column, Index, ForeignKey

class MSSQLGenerator(GenericGenerator):
    def __init__(self):
        super().__init__()
        self.quote_char = '[]'

    def quote_ident(self, ident: str) -> str:
        if ident.startswith('[') and ident.endswith(']'):
            return ident
        
        # Handle schema.table
        if '.' in ident:
            parts = ident.split('.')
            return '.'.join([self.quote_ident(p) for p in parts])
            
        return f"[{ident}]"

    def create_table(self, table: Table) -> str:
        """Generates CREATE TABLE statement."""
        columns_sql = []
        for col in table.columns:
            col_def = f"{self.quote_ident(col.name)} {col.data_type}"
            
            if not col.is_nullable:
                col_def += " NOT NULL"
            else:
                col_def += " NULL"
                
            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            
            columns_sql.append(col_def)
            
        # Primary Key (Inline vs Constraint)
        # MS SQL prefers named constraints typically but inline is valid.
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if pk_cols:
             pk_sql = f"CONSTRAINT {self.quote_ident('PK_' + table.name)} PRIMARY KEY ({', '.join(map(self.quote_ident, pk_cols))})"
             columns_sql.append(pk_sql)

        return f"CREATE TABLE {self.quote_ident(table.name)} (\n    " + ",\n    ".join(columns_sql) + "\n);"

    def alter_column(self, table_name: str, column_name: str, new_type: str, new_nullability: bool = None) -> str:
        """Generates ALTER TABLE ... ALTER COLUMN."""
        # MSSQL: ALTER TABLE table ALTER COLUMN col type [NULL|NOT NULL]
        stmt = f"ALTER TABLE {self.quote_ident(table_name)} ALTER COLUMN {self.quote_ident(column_name)} {new_type}"
        if new_nullability is not None:
             stmt += " NOT NULL" if not new_nullability else " NULL"
        return stmt + ";"

    def add_column(self, table_name: str, column: Column) -> str:
        col_def = f"{self.quote_ident(column.name)} {column.data_type}"
        if not column.is_nullable:
             col_def += " NOT NULL"
        # Optional default handling...
        return f"ALTER TABLE {self.quote_ident(table_name)} ADD {col_def};"
        
    def drop_column(self, table_name: str, column_name: str) -> str:
        return f"ALTER TABLE {self.quote_ident(table_name)} DROP COLUMN {self.quote_ident(column_name)};"

    def rename_table(self, old_name: str, new_name: str) -> str:
        # T-SQL uses sp_rename
        return f"EXEC sp_rename '{old_name}', '{new_name}';"

    def _generate_create_index(self, index, table_name) -> str:
        """Override to support MSSQL CLUSTERED/NONCLUSTERED indexes."""
        stmt = "CREATE "
        if index.is_unique:
            stmt += "UNIQUE "
        if index.is_clustered:
            stmt += "CLUSTERED "
        elif hasattr(index, 'is_clustered'):
            # Explicitly NONCLUSTERED if is_clustered is False and we know it
            stmt += "NONCLUSTERED "
        stmt += f"INDEX {self.quote_ident(index.name)} ON {self.quote_ident(table_name)}"
        stmt += f"({', '.join([self.quote_ident(c) for c in index.columns])});"
        return stmt
