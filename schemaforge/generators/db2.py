from schemaforge.generators.generic import GenericGenerator


class DB2Generator(GenericGenerator):
    def quote_ident(self, ident: str) -> str:
        return f'"{ident}"'

    def generate_migration(self, plan):
        sql = []
        
        # Custom Objects (Aliases)
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
            
        for table in plan.dropped_tables:
            sql.append(f"DROP TABLE {self.quote_ident(table.name)};")
            
        for diff in plan.modified_tables:
            # DB2 ALTER TABLE is standard-ish
            # But we need to handle IDENTITY if added/removed (complex)
            for col in diff.added_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ADD COLUMN {self._col_def(col)};")
                
            for col in diff.dropped_columns:
                sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} DROP COLUMN {self.quote_ident(col.name)};")
                
            for old_col, new_col in diff.modified_columns:
                # DB2 Modify: ALTER COLUMN ... SET DATA TYPE ...
                if old_col.data_type != new_col.data_type:
                    sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ALTER COLUMN {self.quote_ident(new_col.name)} SET DATA TYPE {new_col.data_type};")
                # Nullability
                if old_col.is_nullable != new_col.is_nullable:
                    action = "DROP NOT NULL" if new_col.is_nullable else "SET NOT NULL"
                    sql.append(f"ALTER TABLE {self.quote_ident(diff.table_name)} ALTER COLUMN {self.quote_ident(new_col.name)} {action};")

        return "\n".join(sql)

    def create_table_sql(self, table):
        kind = "TABLE"
        if table.table_type == "AUX TABLE":
            kind = "AUX TABLE"
            
        stmt = f"CREATE {kind} {self.quote_ident(table.name)}"
        
        # AUX TABLES don't have standard column defs
        if table.table_type == "AUX TABLE":
            # Add STORES clause
             if 'aux_stores_table' in table.tags and 'aux_stores_col' in table.tags:
                 stmt += f"\n STORES {self.quote_ident(table.tags['aux_stores_table'])} COLUMN {self.quote_ident(table.tags['aux_stores_col'])}"
        else:
            stmt += " (\n"
            lines = [f"  {self._col_def(c)}" for c in table.columns]
            
            # PK
            pk_cols = [c.name for c in table.columns if c.is_primary_key]
            if pk_cols:
                pk_sql = f"PRIMARY KEY ({', '.join([self.quote_ident(c) for c in pk_cols])})"
                if table.primary_key_name:
                    pk_sql = f"CONSTRAINT {self.quote_ident(table.primary_key_name)} {pk_sql}"
                lines.append(f"  {pk_sql}")
                
            # PERIOD
            if table.period_for:
                lines.append(f"  PERIOD FOR {table.period_for}")
                
            # Foreign Keys
            for fk in table.foreign_keys:
                fk_sql = f"FOREIGN KEY ({', '.join([self.quote_ident(c) for c in fk.column_names])}) REFERENCES {self.quote_ident(fk.ref_table)} ({', '.join([self.quote_ident(c) for c in fk.ref_column_names])})"
                if fk.on_delete:
                    fk_sql += f" ON DELETE {fk.on_delete}"
                if fk.on_update:
                    fk_sql += f" ON UPDATE {fk.on_update}"
                
                if fk.name:
                    fk_sql = f"CONSTRAINT {self.quote_ident(fk.name)} {fk_sql}"
                lines.append(f"  {fk_sql}")
                
            # Check Constraints
            for ck in table.check_constraints:
                ck_sql = f"CHECK ({ck.expression})"
                if ck.name:
                    ck_sql = f"CONSTRAINT {self.quote_ident(ck.name)} {ck_sql}"
                # Add ENFORCED/NOT ENFORCED logic if model supports it?
                # Model doesn't explicitly have it, assuming expression handles it or default.
                lines.append(f"  {ck_sql}")
                
            stmt += ",\n".join(lines)
            stmt += "\n)"
        
        if table.tablespace:
            if table.database_name:
                stmt += f" IN DATABASE {self.quote_ident(table.database_name)}.{self.quote_ident(table.tablespace)}"
            else:
                stmt += f" IN {self.quote_ident(table.tablespace)}"
            
        if table.stogroup:
            stmt += f" USING STOGROUP {self.quote_ident(table.stogroup)}"
        
        if table.priqty:
            stmt += f" PRIQTY {table.priqty}"
            
        if table.secqty:
            stmt += f" SECQTY {table.secqty}"
            
        if table.audit:
            stmt += f" AUDIT {table.audit}"
            
        if table.ccsid:
            stmt += f" CCSID {table.ccsid}"
            
        if table.partition_by:
            stmt += f" PARTITION BY {table.partition_by}"
            
        stmt += ";"
        return stmt

    def _col_def(self, col):
        base = f"{self.quote_ident(col.name)} {col.data_type}"
        if not col.is_nullable:
            base += " NOT NULL"
        if col.default_value:
            base += f" DEFAULT {col.default_value}"
        if col.is_identity:
            base += " GENERATED ALWAYS AS IDENTITY"
        return base

    def create_index_sql(self, index, table_name) -> str:
        unique_str = "UNIQUE " if index.is_unique else ""
        cols = ", ".join([self.quote_ident(c) for c in index.columns])
        stmt = f"CREATE {unique_str}INDEX {self.quote_ident(index.name)} ON {self.quote_ident(table_name)} ({cols})"
        
        # DB2 Extras
        if 'include_columns' in index.properties and index.properties['include_columns']:
            inc_cols = ", ".join([self.quote_ident(c) for c in index.properties['include_columns']])
            stmt += f"\n INCLUDE ({inc_cols})"
            
        if index.properties.get('cluster'):
            stmt += "\n CLUSTER"
            
        if index.properties.get('partitioned'):
            stmt += "\n PARTITIONED"
            
        stmt += ";"
        return stmt
