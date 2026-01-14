from schemaforge.generators.generic import GenericGenerator

class PostgresGenerator(GenericGenerator):
    def quote_ident(self, ident: str) -> str:
        return f'"{ident}"'

    def generate_migration(self, plan):
        sql = []
        
        # Custom Objects
        for obj in plan.dropped_custom_objects:
            sql.append(f"DROP {obj.obj_type} {obj.name};")
            
        for obj in plan.new_custom_objects:
            if 'raw_sql' in obj.properties:
                sql.append(obj.properties['raw_sql'] + ";")
            else:
                sql.append(f"-- Create {obj.obj_type} {obj.name}")

        for old, new in plan.modified_custom_objects:
             sql.append(f"DROP {old.obj_type} {old.name};")
             if 'raw_sql' in new.properties:
                sql.append(new.properties['raw_sql'] + ";")

        # Tables
        for table in plan.new_tables:
            sql.append(self.create_table_sql(table))
            # Indexes for new tables
            for idx in table.indexes:
                sql.append(self.create_index_sql(idx, table.name))
            
        for table in plan.dropped_tables:
            sql.append(f"DROP TABLE {self.quote_ident(table.name)};")
            
        for diff in plan.modified_tables:
            # Postgres ALTER TABLE
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD COLUMN {self._col_def(col)};")
                
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP COLUMN {self.quote_ident(col.name)};")
                
            for old_col, new_col in diff.modified_columns:
                # Type change
                if old_col.data_type != new_col.data_type:
                    # Safety: Add USING clause for type conversion
                    # For simple cases like TEXT -> INT, this prevents "operator does not exist" errors
                    # and allows explicit casting.
                    sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ALTER COLUMN {self.quote_ident(new_col.name)} TYPE {new_col.data_type} USING {self.quote_ident(new_col.name)}::{new_col.data_type};")
                # Nullability
                if old_col.is_nullable != new_col.is_nullable:
                    action = "DROP NOT NULL" if new_col.is_nullable else "SET NOT NULL"
                    sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ALTER COLUMN {self.quote_ident(new_col.name)} {action};")
                # Default
                if old_col.default_value != new_col.default_value:
                    if new_col.default_value:
                        sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ALTER COLUMN {self.quote_ident(new_col.name)} SET DEFAULT {new_col.default_value};")
                    else:
                        sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ALTER COLUMN {self.quote_ident(new_col.name)} DROP DEFAULT;")

            # Indexes
            for idx in diff.added_indexes:
                sql.append(self.create_index_sql(idx, diff.table_name))
                
            for idx in diff.dropped_indexes:
                sql.append(f"DROP INDEX {idx.name};")

            # Foreign Keys
            for fk in diff.dropped_fks:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP CONSTRAINT {self.quote_ident(fk.name)};")
                
            for fk in diff.added_fks:
                cols = ", ".join([self.quote_ident(c) for c in fk.column_names])
                ref_cols_str = f"({', '.join([self.quote_ident(c) for c in fk.ref_column_names])})" if fk.ref_column_names else ""
                fk_sql = f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD CONSTRAINT {self.quote_ident(fk.name)} FOREIGN KEY ({cols}) REFERENCES {self.quote_ident(fk.ref_table)}{ref_cols_str}"
                if fk.on_delete:
                    fk_sql += f" ON DELETE {fk.on_delete}"
                if fk.on_update:
                    fk_sql += f" ON UPDATE {fk.on_update}"
                fk_sql += ";"
                sql.append(fk_sql)
            
            # Modified FKs (drop and recreate)
            for old_fk, new_fk in diff.modified_fks:
                # Drop old FK
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP CONSTRAINT {self.quote_ident(old_fk.name)};")
                # Create new FK
                cols = ", ".join([self.quote_ident(c) for c in new_fk.column_names])
                ref_cols_str = f"({', '.join([self.quote_ident(c) for c in new_fk.ref_column_names])})" if new_fk.ref_column_names else ""
                fk_sql = f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD CONSTRAINT {self.quote_ident(new_fk.name)} FOREIGN KEY ({cols}) REFERENCES {self.quote_ident(new_fk.ref_table)}{ref_cols_str}"
                if new_fk.on_delete:
                    fk_sql += f" ON DELETE {new_fk.on_delete}"
                if new_fk.on_update:
                    fk_sql += f" ON UPDATE {new_fk.on_update}"
                fk_sql += ";"
                sql.append(fk_sql)

        return "\n".join(sql)

    def create_table_sql(self, table):
        stmt = "CREATE "
        if table.is_unlogged:
            stmt += "UNLOGGED "
        stmt += f"TABLE {self.quote_ident(table.name)} (\n"
        
        cols = [f"  {self._col_def(c)}" for c in table.columns]
        
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if pk_cols:
            cols.append(f"  PRIMARY KEY ({', '.join([self.quote_ident(c) for c in pk_cols])})")
            
        # Add Foreign Keys inline (BUG-001 fix)
        for fk in table.foreign_keys:
            cols_str = ", ".join([self.quote_ident(c) for c in fk.column_names])
            ref_cols_str = f"({', '.join([self.quote_ident(c) for c in fk.ref_column_names])})" if fk.ref_column_names else ""
            fk_def = f"  CONSTRAINT {self.quote_ident(fk.name)} FOREIGN KEY ({cols_str}) REFERENCES {self.quote_ident(fk.ref_table)}{ref_cols_str}"
            cols.append(fk_def)
            
        stmt += ",\n".join(cols)
        stmt += "\n)"
        
        if table.partition_by:
            stmt += f" PARTITION BY {table.partition_by}"
            
        stmt += ";"
        return stmt

    def create_index_sql(self, index, table_name):
        # Override to support USING method
        stmt = "CREATE "
        if index.is_unique:
            stmt += "UNIQUE "
        stmt += f"INDEX {self.quote_ident(index.name)} ON {self.quote_ident(table_name)} "
        if index.method and index.method != 'btree':
            stmt += f"USING {index.method} "
        stmt += f"({', '.join(index.columns)});"
        return stmt

    def _col_def(self, col):
        base = f"{self.quote_ident(col.name)} {col.data_type}"
        if not col.is_nullable:
            base += " NOT NULL"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        return base
