from schemaforge.generators.generic import GenericGenerator

from schemaforge.generators.generic import GenericGenerator

class MySQLGenerator(GenericGenerator):
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
            # MySQL ALTER TABLE
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {diff.table_name} ADD COLUMN {self._col_def(col)};")
                
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {diff.table_name} DROP COLUMN {col.name};")
                
            for old_col, new_col in diff.modified_columns:
                # Type change or Nullability
                # MySQL MODIFY COLUMN covers both
                sql.append(f"ALTER TABLE {diff.table_name} MODIFY COLUMN {self._col_def(new_col)};")

            # Indexes
            for idx in diff.added_indexes:
                sql.append(self.create_index_sql(idx, diff.table_name))
                
            for idx in diff.dropped_indexes:
                sql.append(f"DROP INDEX {idx.name} ON {diff.table_name};")

        return "\n".join(sql)

    def create_table_sql(self, table):
        stmt = f"CREATE TABLE {table.name} (\n"
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
        stmt = "CREATE "
        if index.is_unique:
            stmt += "UNIQUE "
        elif index.method == 'fulltext':
            stmt += "FULLTEXT "
            
        stmt += f"INDEX {index.name} ON {table_name} ({', '.join(index.columns)});"
        return stmt

    def _col_def(self, col):
        base = f"{col.name} {col.data_type}"
        if not col.is_nullable:
            base += " NOT NULL"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        return base
