from typing import Any
from schemaforge.generators.base import BaseGenerator
from schemaforge.models import Table, Column

class GenericGenerator(BaseGenerator):
    def generate_migration(self, migration_plan: Any) -> str:
        statements = []
        
        # New Tables
        for table in migration_plan.new_tables:
            statements.append(self._generate_create_table(table))
            # Create indexes for new tables
            for index in table.indexes:
                statements.append(self._generate_create_index(index, table.name))
            
        # Dropped Tables
        for table in migration_plan.dropped_tables:
            statements.append(f"DROP TABLE {self.quote_ident(table.name)};")
            
        # Modified Tables
        for table_diff in migration_plan.modified_tables:
            statements.extend(self._generate_alter_table(table_diff))
            
        return "\n\n".join(statements)

    def _generate_create_table(self, table: Table) -> str:
        columns_def = []
        for col in table.columns:
            def_str = f"{self.quote_ident(col.name)} {col.data_type}"
            if not col.is_nullable:
                def_str += " NOT NULL"
            if col.is_primary_key:
                def_str += " PRIMARY KEY"
            columns_def.append(def_str)
            
        # Add Foreign Keys inline
        for fk in table.foreign_keys:
            cols = ", ".join([self.quote_ident(c) for c in fk.column_names])
            ref_cols_str = f"({', '.join([self.quote_ident(c) for c in fk.ref_column_names])})" if fk.ref_column_names else ""
            fk_def = f"CONSTRAINT {self.quote_ident(fk.name)} FOREIGN KEY ({cols}) REFERENCES {self.quote_ident(fk.ref_table)}{ref_cols_str}"
            columns_def.append(fk_def)
            
        return f"CREATE TABLE {self.quote_ident(table.name)} (\n    " + ",\n    ".join(columns_def) + "\n);"

    def _generate_create_index(self, index, table_name):
        unique_str = "UNIQUE " if index.is_unique else ""
        cols = ", ".join([self.quote_ident(c) for c in index.columns])
        return f"CREATE {unique_str}INDEX {self.quote_ident(index.name)} ON {self.quote_ident(table_name)}({cols});"

    def _generate_alter_table(self, diff: Any) -> list[str]:
        # diff is TableDiff
        statements = []
        
        # Track if we need to drop/recreate PK due to column modifications
        pk_cols = getattr(diff, 'pk_columns', [])  # Will be set by comparator if available
        pk_constraint_name = getattr(diff, 'pk_constraint_name', None)
        pk_dropped = False
        
        # Check if any modified column is part of PK
        modified_pk_cols = [new_col.name for old_col, new_col in diff.modified_columns 
                          if new_col.name.lower() in [c.lower() for c in pk_cols]]
        
        # If modifying PK columns, drop PK first
        if modified_pk_cols and pk_constraint_name:
            statements.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP CONSTRAINT {self.quote_ident(pk_constraint_name)};")
            pk_dropped = True
        
        # Add columns
        for col in diff.added_columns:
            statements.append(self.add_column(diff.table_name, col))
                
        # Drop columns
        for col in diff.dropped_columns:
            statements.append(self.drop_column(diff.table_name, col.name))
                
        # Modify columns
        for old_col, new_col in diff.modified_columns:
             statements.append(self.alter_column(diff.table_name, new_col.name, new_col.data_type, new_col.is_nullable))

        # Re-add PK if we dropped it
        if pk_dropped and pk_cols:
            pk_col_list = ', '.join([self.quote_ident(c) for c in pk_cols])
            statements.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD CONSTRAINT {self.quote_ident(pk_constraint_name)} PRIMARY KEY ({pk_col_list});")

        # Add Indexes
        for index in diff.added_indexes:
            statements.append(self._generate_create_index(index, diff.table_name))

        # Drop Indexes
        for index in diff.dropped_indexes:
            statements.append(self._drop_index_stmt(index, diff.table_name))

        # Add Foreign Keys
        for fk in diff.added_fks:
            statements.append(self._add_fk_stmt(diff.table_name, fk))

        # Drop Foreign Keys
        for fk in diff.dropped_fks:
            statements.append(self._drop_fk_stmt(diff.table_name, fk))
                     
        return statements

    # Default implementations
    def add_column(self, table_name: str, column: Column) -> str:
        def_str = f"{self.quote_ident(column.name)} {column.data_type}"
        if not column.is_nullable:
            def_str += " NOT NULL"
        return f"ALTER TABLE {self.quote_ident(table_name)} ADD COLUMN {def_str};"

    def drop_column(self, table_name: str, column_name: str) -> str:
        return f"ALTER TABLE {self.quote_ident(table_name)} DROP COLUMN {self.quote_ident(column_name)};"

    def alter_column(self, table_name: str, column_name: str, new_type: str, new_nullability: bool = None) -> str:
        return f"ALTER TABLE {self.quote_ident(table_name)} MODIFY COLUMN {self.quote_ident(column_name)} {new_type};"

    def _drop_index_stmt(self, index, table_name):
        return f"DROP INDEX {self.quote_ident(index.name)} ON {self.quote_ident(table_name)};"
    
    def _add_fk_stmt(self, table_name, fk):
        cols = ", ".join([self.quote_ident(c) for c in fk.column_names])
        ref_cols_str = f"({', '.join([self.quote_ident(c) for c in fk.ref_column_names])})" if fk.ref_column_names else ""
        return f"ALTER TABLE {self.quote_ident(table_name)} ADD CONSTRAINT {self.quote_ident(fk.name)} FOREIGN KEY ({cols}) REFERENCES {self.quote_ident(fk.ref_table)}{ref_cols_str};"

    def _drop_fk_stmt(self, table_name, fk):
        # Changed from DROP FOREIGN KEY to DROP CONSTRAINT (standard)
        return f"ALTER TABLE {self.quote_ident(table_name)} DROP CONSTRAINT {self.quote_ident(fk.name)};"

    def generate_rollback_migration(self, migration_plan: Any) -> str:
        """
        Generate a rollback migration that reverses the changes in the migration plan.
        
        The rollback undoes:
        - New tables -> DROP TABLE
        - Dropped tables -> CREATE TABLE
        - Added columns -> DROP COLUMN
        - Dropped columns -> ADD COLUMN
        - Added indexes -> DROP INDEX
        - Dropped indexes -> CREATE INDEX
        - Added foreign keys -> DROP FK
        - Dropped foreign keys -> ADD FK
        """
        statements = []
        
        # Inverse of new tables = DROP TABLE
        for table in migration_plan.new_tables:
            statements.append(f"DROP TABLE IF EXISTS {self.quote_ident(table.name)};")
            
        # Inverse of dropped tables = CREATE TABLE
        for table in migration_plan.dropped_tables:
            statements.append(self._generate_create_table(table))
            # Recreate indexes for restored tables
            for index in table.indexes:
                statements.append(self._generate_create_index(index, table.name))
            
        # Inverse of modified tables
        for diff in migration_plan.modified_tables:
            statements.extend(self._generate_rollback_alter_table(diff))
            
        return "\n\n".join(statements)
    
    def _generate_rollback_alter_table(self, diff: Any) -> list[str]:
        """Generate rollback statements for table modifications."""
        statements = []
        
        # Inverse of added columns = DROP COLUMN
        for col in diff.added_columns:
            statements.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP COLUMN {self.quote_ident(col.name)};")
        
        # Inverse of dropped columns = ADD COLUMN (restore original)
        for col in diff.dropped_columns:
            def_str = f"{self.quote_ident(col.name)} {col.data_type}"
            if not col.is_nullable:
                def_str += " NOT NULL"
            if col.default_value:
                def_str += f" DEFAULT {col.default_value}"
            statements.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD COLUMN {def_str};")
        
        # Inverse of modified columns = revert to old definition
        for old_col, new_col in diff.modified_columns:
            statements.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} MODIFY COLUMN {self.quote_ident(old_col.name)} {old_col.data_type};")
        
        # Inverse of added indexes = DROP INDEX
        for index in diff.added_indexes:
            statements.append(f"DROP INDEX {self.quote_ident(index.name)} ON {self.quote_ident(diff.table_name)};")
        
        # Inverse of dropped indexes = CREATE INDEX
        for index in diff.dropped_indexes:
            statements.append(self._generate_create_index(index, diff.table_name))
        
        # Inverse of added foreign keys = DROP FK
        for fk in diff.added_fks:
            statements.append(self._drop_fk_stmt(diff.table_name, fk))
        
        # Inverse of dropped foreign keys = ADD FK
        for fk in diff.dropped_fks:
            statements.append(self._add_fk_stmt(diff.table_name, fk))
        
        return statements
