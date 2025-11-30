from dataclasses import dataclass, field
from typing import List, Optional
from schemaforge.models import Schema, Table, Column, Index, ForeignKey, CustomObject

@dataclass
class TableDiff:
    table_name: str
    added_columns: List[Column] = field(default_factory=list)
    dropped_columns: List[Column] = field(default_factory=list)
    modified_columns: List[tuple[Column, Column]] = field(default_factory=list) # (old, new)
    added_indexes: List[Index] = field(default_factory=list)
    dropped_indexes: List[Index] = field(default_factory=list)
    added_fks: List[ForeignKey] = field(default_factory=list)
    dropped_fks: List[ForeignKey] = field(default_factory=list)
    
    def to_dict(self):
        return {
            "table_name": self.table_name,
            "added_columns": [c.to_dict() for c in self.added_columns],
            "dropped_columns": [c.to_dict() for c in self.dropped_columns],
            "modified_columns": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_columns
            ],
            "added_indexes": [i.to_dict() for i in self.added_indexes],
            "dropped_indexes": [i.to_dict() for i in self.dropped_indexes],
            "added_fks": [fk.to_dict() for fk in self.added_fks],
            "dropped_fks": [fk.to_dict() for fk in self.dropped_fks]
        }

@dataclass
class MigrationPlan:
    new_tables: List[Table] = field(default_factory=list)
    dropped_tables: List[Table] = field(default_factory=list)
    modified_tables: List[TableDiff] = field(default_factory=list) 
    
    # Custom Objects
    new_custom_objects: List[CustomObject] = field(default_factory=list)
    dropped_custom_objects: List[CustomObject] = field(default_factory=list)
    modified_custom_objects: List[tuple[CustomObject, CustomObject]] = field(default_factory=list)
    
    def to_dict(self):
        return {
            "new_tables": [t.to_dict() for t in self.new_tables],
            "dropped_tables": [t.to_dict() for t in self.dropped_tables],
            "modified_tables": [t.to_dict() for t in self.modified_tables],
            "new_custom_objects": [o.to_dict() for o in self.new_custom_objects],
            "dropped_custom_objects": [o.to_dict() for o in self.dropped_custom_objects],
            "modified_custom_objects": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_custom_objects
            ]
        } 

class Comparator:
    def compare(self, old_schema: Schema, new_schema: Schema) -> MigrationPlan:
        plan = MigrationPlan()
        
        old_tables = {t.name: t for t in old_schema.tables}
        new_tables = {t.name: t for t in new_schema.tables}
        
        # Detect new tables
        for name, table in new_tables.items():
            if name not in old_tables:
                plan.new_tables.append(table)
                
        # Detect dropped tables
        for name, table in old_tables.items():
            if name not in new_tables:
                plan.dropped_tables.append(table)
                
        # Detect modifications
        for name, new_table in new_tables.items():
            if name in old_tables:
                old_table = old_tables[name]
                diff = self._compare_tables(old_table, new_table)
                if diff:
                    plan.modified_tables.append(diff)
                    
        # Custom Objects Comparison
        old_objs = {(o.obj_type, o.name): o for o in old_schema.custom_objects}
        new_objs = {(o.obj_type, o.name): o for o in new_schema.custom_objects}
        
        for key, obj in new_objs.items():
            if key not in old_objs:
                plan.new_custom_objects.append(obj)
            else:
                # Check for modification (simple property check)
                old_obj = old_objs[key]
                if old_obj.properties != obj.properties:
                    plan.modified_custom_objects.append((old_obj, obj))
                    
        for key, obj in old_objs.items():
            if key not in new_objs:
                plan.dropped_custom_objects.append(obj)
                    
        return plan

    def _compare_tables(self, old_table: Table, new_table: Table) -> Optional[TableDiff]:
        diff = TableDiff(table_name=new_table.name)
        has_changes = False
        
        old_cols = {c.name: c for c in old_table.columns}
        new_cols = {c.name: c for c in new_table.columns}
        
        # Added columns
        for name, col in new_cols.items():
            if name not in old_cols:
                diff.added_columns.append(col)
                has_changes = True
                
        # Dropped columns
        for name, col in old_cols.items():
            if name not in new_cols:
                diff.dropped_columns.append(col)
                has_changes = True
                
        # Modified columns
        for name, new_col in new_cols.items():
            if name in old_cols:
                old_col = old_cols[name]
                if self._is_column_modified(old_col, new_col):
                    diff.modified_columns.append((old_col, new_col))
                    has_changes = True
                    
        # Indexes
        old_indexes = {i.name: i for i in old_table.indexes}
        new_indexes = {i.name: i for i in new_table.indexes}
        
        for name, idx in new_indexes.items():
            if name not in old_indexes:
                diff.added_indexes.append(idx)
                has_changes = True
        
        for name, idx in old_indexes.items():
            if name not in new_indexes:
                diff.dropped_indexes.append(idx)
                has_changes = True
                
        # Foreign Keys
        old_fks = {fk.name: fk for fk in old_table.foreign_keys}
        new_fks = {fk.name: fk for fk in new_table.foreign_keys}
        
        for name, fk in new_fks.items():
            if name not in old_fks:
                diff.added_fks.append(fk)
                has_changes = True
                
        for name, fk in old_fks.items():
            if name not in new_fks:
                diff.dropped_fks.append(fk)
                has_changes = True
                    
        return diff if has_changes else None

    def _is_column_modified(self, old_col: Column, new_col: Column) -> bool:
        return (old_col.data_type != new_col.data_type or 
                old_col.is_nullable != new_col.is_nullable or
                old_col.default_value != new_col.default_value or
                old_col.is_primary_key != new_col.is_primary_key)
