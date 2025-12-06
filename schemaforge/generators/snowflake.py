from schemaforge.generators.generic import GenericGenerator

class SnowflakeGenerator(GenericGenerator):
    def quote_ident(self, ident: str) -> str:
        return f'"{ident}"'

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
            sql.append(f"DROP TABLE {self.quote_ident(table.name)};")
            
        # Modified Tables
        for diff in plan.modified_tables:
            # Add Columns
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD COLUMN {self._col_def(col)};")
                
            # Drop Columns
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP COLUMN {self.quote_ident(col.name)};")
                
            # Modify Columns
            for old, new in diff.modified_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} MODIFY COLUMN {self._col_def(new)};")
                
            # Snowflake Specific Alterations
            # We need to check if table properties changed. 
            # The generic Diff object doesn't track property changes yet.
            # This is a limitation. For "God Mode", we might just recreate the table if properties change?
            # Or we can add logic here if we had access to the full table objects.
            
            # Handle Primary Key Changes
            # We need to check if the set of PK columns changed
            # Since we don't have easy access to old/new table objects here (only diff), 
            # we rely on modified_columns or property_changes if we had them.
            # But wait, diff.property_changes IS available!
            
            pk_changed = False
            for prop in diff.property_changes:
                if "Primary Key Name" in prop:
                    pk_changed = True
            
            # Also check if any column's PK status changed
            for old, new in diff.modified_columns:
                if old.is_primary_key != new.is_primary_key:
                    pk_changed = True
            
            if pk_changed:
                # Drop existing PK (if any) - we assume there was one if we are changing it, or we just try to drop
                # In Snowflake, "DROP PRIMARY KEY" works even if we are adding one? No.
                # If we are adding a PK where none existed, DROP might fail.
                # But if we are modifying, we likely need to drop old one.
                # Safe approach: Try to drop if we suspect there was one, then add new one.
                # For now, we will just generate the ADD/DROP based on current state.
                
                if pk_changed:
                    sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP PRIMARY KEY;")
                    
                    if diff.new_table_obj:
                        pk_cols = [c.name for c in diff.new_table_obj.columns if c.is_primary_key]
                        if pk_cols:
                            pk_def = f"PRIMARY KEY ({', '.join([self.quote_ident(c) for c in pk_cols])})"
                            if diff.new_table_obj.primary_key_name:
                                pk_def = f"CONSTRAINT {self.quote_ident(diff.new_table_obj.primary_key_name)} {pk_def}"
                            sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD {pk_def};")

        return "\n".join(sql)

    def create_table_sql(self, table):
        stmt = "CREATE "
        if table.is_transient:
            stmt += "TRANSIENT "
        stmt += f"TABLE {self.quote_ident(table.name)} (\n"
        
        cols = [f"  {self._col_def(c)}" for c in table.columns]
        
        # Primary Keys
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if pk_cols:
            pk_def = f"PRIMARY KEY ({', '.join([self.quote_ident(c) for c in pk_cols])})"
            if table.primary_key_name:
                pk_def = f"CONSTRAINT {self.quote_ident(table.primary_key_name)} {pk_def}"
            cols.append(f"  {pk_def}")
            
        stmt += ",\n".join(cols)
        stmt += "\n)"
        
        # Properties
        if table.cluster_by:
            stmt += f"\nCLUSTER BY ({', '.join([self.quote_ident(c) for c in table.cluster_by])})"
            
        if table.retention_days is not None:
            stmt += f"\nDATA_RETENTION_TIME_IN_DAYS = {table.retention_days}"
            
        if table.comment:
            stmt += f"\nCOMMENT = '{table.comment}'"
            
        stmt += ";"
        return stmt

    def _col_def(self, col):
        base = f"{self.quote_ident(col.name)} {col.data_type}"
        if not col.is_nullable:
            base += " NOT NULL"
        if col.is_identity:
            start = col.identity_start if col.identity_start is not None else 1
            step = col.identity_step if col.identity_step is not None else 1
            base += f" IDENTITY({start}, {step})"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        return base
