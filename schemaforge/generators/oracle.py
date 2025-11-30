from schemaforge.generators.generic import GenericGenerator

from schemaforge.generators.generic import GenericGenerator

class OracleGenerator(GenericGenerator):
    def generate_migration(self, plan):
        sql = []
        
        # Custom Objects
        for obj in plan.dropped_custom_objects:
            sql.append(f"DROP {obj.obj_type} {obj.name};")
            
        for obj in plan.new_custom_objects:
            if 'raw_sql' in obj.properties:
                stmt = obj.properties['raw_sql']
                # Oracle PL/SQL often needs a slash at the end
                if obj.obj_type in ('FUNCTION', 'PROCEDURE', 'PACKAGE', 'TRIGGER'):
                    if not stmt.strip().endswith('/'):
                        stmt += "\n/"
                elif not stmt.strip().endswith(';'):
                    stmt += ";"
                sql.append(stmt)
            else:
                sql.append(f"-- Create {obj.obj_type} {obj.name}")

        for old, new in plan.modified_custom_objects:
             sql.append(f"DROP {old.obj_type} {old.name};")
             if 'raw_sql' in new.properties:
                stmt = new.properties['raw_sql']
                if new.obj_type in ('FUNCTION', 'PROCEDURE', 'PACKAGE', 'TRIGGER'):
                    if not stmt.strip().endswith('/'):
                        stmt += "\n/"
                elif not stmt.strip().endswith(';'):
                    stmt += ";"
                sql.append(stmt)

        # Tables
        for table in plan.new_tables:
            sql.append(self.create_table_sql(table))
            
        for table in plan.dropped_tables:
            sql.append(f"DROP TABLE {table.name};")
            
        for diff in plan.modified_tables:
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {diff.table_name} ADD {self._col_def(col)};")
                
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {diff.table_name} DROP COLUMN {col.name};")
                
            for old_col, new_col in diff.modified_columns:
                # Oracle Modify: MODIFY (col type ...)
                if old_col.data_type != new_col.data_type or old_col.is_nullable != new_col.is_nullable:
                    sql.append(f"ALTER TABLE {diff.table_name} MODIFY {self._col_def(new_col)};")

        return "\n".join(sql)

    def create_table_sql(self, table):
        stmt = f"CREATE TABLE {table.name} (\n"
        cols = [f"  {self._col_def(c)}" for c in table.columns]
        
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if pk_cols:
            cols.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")
            
        stmt += ",\n".join(cols)
        stmt += "\n)"
        
        if table.tablespace:
            stmt += f" TABLESPACE {table.tablespace}"
            
        if table.partition_by:
            stmt += f" PARTITION BY {table.partition_by}"
            
        stmt += ";"
        return stmt

    def _col_def(self, col):
        base = f"{col.name} {col.data_type}"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        if not col.is_nullable:
            base += " NOT NULL"
        return base
