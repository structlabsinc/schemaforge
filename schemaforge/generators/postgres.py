from schemaforge.generators.generic import GenericGenerator

class PostgresGenerator(GenericGenerator):
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
            sql.append(f"DROP TABLE {table.name};")
            
        for diff in plan.modified_tables:
            # Postgres ALTER TABLE
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {diff.table_name} ADD COLUMN {self._col_def(col)};")
                
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {diff.table_name} DROP COLUMN {col.name};")
                
            for old_col, new_col in diff.modified_columns:
                # Type change
                if old_col.data_type != new_col.data_type:
                    sql.append(f"ALTER TABLE {diff.table_name} ALTER COLUMN {new_col.name} TYPE {new_col.data_type};")
                # Nullability
                if old_col.is_nullable != new_col.is_nullable:
                    action = "DROP NOT NULL" if new_col.is_nullable else "SET NOT NULL"
                    sql.append(f"ALTER TABLE {diff.table_name} ALTER COLUMN {new_col.name} {action};")
                # Default
                if old_col.default_value != new_col.default_value:
                    if new_col.default_value:
                        sql.append(f"ALTER TABLE {diff.table_name} ALTER COLUMN {new_col.name} SET DEFAULT {new_col.default_value};")
                    else:
                        sql.append(f"ALTER TABLE {diff.table_name} ALTER COLUMN {new_col.name} DROP DEFAULT;")

            # Indexes
            for idx in diff.added_indexes:
                sql.append(self.create_index_sql(idx, diff.table_name))
                
            for idx in diff.dropped_indexes:
                sql.append(f"DROP INDEX {idx.name};")

        return "\n".join(sql)

    def create_table_sql(self, table):
        stmt = "CREATE "
        if table.is_unlogged:
            stmt += "UNLOGGED "
        stmt += f"TABLE {table.name} (\n"
        
        cols = [f"  {self._col_def(c)}" for c in table.columns]
        
        pk_cols = [c.name for c in table.columns if c.is_primary_key]
        if pk_cols:
            cols.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")
            
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
        stmt += f"INDEX {index.name} ON {table_name} "
        if index.method and index.method != 'btree':
            stmt += f"USING {index.method} "
        stmt += f"({', '.join(index.columns)});"
        return stmt

    def _col_def(self, col):
        base = f"{col.name} {col.data_type}"
        if not col.is_nullable:
            base += " NOT NULL"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        return base
