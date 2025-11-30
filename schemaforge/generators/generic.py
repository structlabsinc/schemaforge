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
            statements.append(f"DROP TABLE {table.name};")
            
        # Modified Tables
        for table_diff in migration_plan.modified_tables:
            statements.extend(self._generate_alter_table(table_diff))
            
        return "\n\n".join(statements)

    def _generate_create_table(self, table: Table) -> str:
        columns_def = []
        for col in table.columns:
            def_str = f"{col.name} {col.data_type}"
            if not col.is_nullable:
                def_str += " NOT NULL"
            if col.is_primary_key:
                def_str += " PRIMARY KEY"
            columns_def.append(def_str)
            
        # Add Foreign Keys inline
        for fk in table.foreign_keys:
            cols = ", ".join(fk.column_names)
            ref_cols = ", ".join(fk.ref_column_names)
            fk_def = f"CONSTRAINT {fk.name} FOREIGN KEY ({cols}) REFERENCES {fk.ref_table}({ref_cols})"
            columns_def.append(fk_def)
            
        return f"CREATE TABLE {table.name} (\n    " + ",\n    ".join(columns_def) + "\n);"

    def _generate_create_index(self, index, table_name):
        unique_str = "UNIQUE " if index.is_unique else ""
        cols = ", ".join(index.columns)
        return f"CREATE {unique_str}INDEX {index.name} ON {table_name}({cols});"

    def _generate_alter_table(self, diff: Any) -> list[str]:
        # diff is TableDiff
        statements = []
        
        # Add columns
        for col in diff.added_columns:
            def_str = f"{col.name} {col.data_type}"
            if not col.is_nullable:
                def_str += " NOT NULL"
            statements.append(f"ALTER TABLE {diff.table_name} ADD COLUMN {def_str};")
                
        # Drop columns
        for col in diff.dropped_columns:
            statements.append(f"ALTER TABLE {diff.table_name} DROP COLUMN {col.name};")
                
        # Modify columns
        for old_col, new_col in diff.modified_columns:
             statements.append(f"ALTER TABLE {diff.table_name} MODIFY COLUMN {new_col.name} {new_col.data_type};")

        # Add Indexes
        for index in diff.added_indexes:
            statements.append(self._generate_create_index(index, diff.table_name))

        # Drop Indexes
        for index in diff.dropped_indexes:
            statements.append(f"DROP INDEX {index.name} ON {diff.table_name};")

        # Add Foreign Keys
        for fk in diff.added_fks:
            cols = ", ".join(fk.column_names)
            ref_cols = ", ".join(fk.ref_column_names)
            statements.append(f"ALTER TABLE {diff.table_name} ADD CONSTRAINT {fk.name} FOREIGN KEY ({cols}) REFERENCES {fk.ref_table}({ref_cols});")

        # Drop Foreign Keys
        for fk in diff.dropped_fks:
            statements.append(f"ALTER TABLE {diff.table_name} DROP FOREIGN KEY {fk.name};")
                     
        return statements
