from schemaforge.generators.generic import GenericGenerator

class SnowflakeGenerator(GenericGenerator):
    def generate_migration(self, plan):
        sql = []
        
        # 1. Custom Objects
        
        # Dropped Custom Objects
        for obj in plan.dropped_custom_objects:
            sql.append(f"DROP {obj.obj_type} {obj.name};")
            
        # New Custom Objects
        for obj in plan.new_custom_objects:
            # For now, we rely on the raw_sql stored in properties
            if 'raw_sql' in obj.properties:
                raw = obj.properties['raw_sql'].strip()
                if raw.endswith(';'):
                    sql.append(raw)
                else:
                    sql.append(raw + ";")
            else:
                sql.append(f"-- TODO: Generate SQL for {obj.obj_type} {obj.name}")
                
        # Modified Custom Objects
        for old, new in plan.modified_custom_objects:
             # Simplest strategy: Drop and Recreate
             sql.append(f"DROP {old.obj_type} {old.name};")
             if 'raw_sql' in new.properties:
                raw = new.properties['raw_sql'].strip()
                if raw.endswith(';'):
                    sql.append(raw)
                else:
                    sql.append(raw + ";")

        # 2. Tables
        
        # New Tables
        for table in plan.new_tables:
            sql.append(self.create_table_sql(table))
            
        # Dropped Tables
        for table in plan.dropped_tables:
            sql.append(f"DROP TABLE {table.name};")
            
        # Modified Tables
        for diff in plan.modified_tables:
            # Add Columns
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {diff.table_name} ADD COLUMN {self._col_def(col)};")
                
            # Drop Columns
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {diff.table_name} DROP COLUMN {col.name};")
                
            # Modify Columns
            for old, new in diff.modified_columns:
                sql.append(f"ALTER TABLE {diff.table_name} MODIFY COLUMN {self._col_def(new)};")
                
            # Snowflake Specific Alterations
            # We need to check if table properties changed. 
            # The generic Diff object doesn't track property changes yet.
            # This is a limitation. For "God Mode", we might just recreate the table if properties change?
            # Or we can add logic here if we had access to the full table objects.
            pass

        return "\n".join(sql)

    def create_table_sql(self, table):
        stmt = "CREATE "
        if table.is_transient:
            stmt += "TRANSIENT "
        stmt += f"TABLE {table.name} (\n"
        
        cols = [f"  {self._col_def(c)}" for c in table.columns]
        
        # Primary Keys
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if pk_cols:
            cols.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")
            
        stmt += ",\n".join(cols)
        stmt += "\n)"
        
        # Properties
        if table.cluster_by:
            stmt += f"\nCLUSTER BY ({', '.join(table.cluster_by)})"
            
        if table.retention_days is not None:
            stmt += f"\nDATA_RETENTION_TIME_IN_DAYS = {table.retention_days}"
            
        if table.comment:
            stmt += f"\nCOMMENT = '{table.comment}'"
            
        stmt += ";"
        return stmt

    def _col_def(self, col):
        base = f"{col.name} {col.data_type}"
        if not col.is_nullable:
            base += " NOT NULL"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        return base
