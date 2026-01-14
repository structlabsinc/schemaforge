from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict, Any
from schemaforge.models import Schema, Table, Column, Index, ForeignKey, CheckConstraint, ExclusionConstraint, CustomObject

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
    modified_fks: List[tuple[ForeignKey, ForeignKey]] = field(default_factory=list)  # (old, new)
    added_checks: List[CheckConstraint] = field(default_factory=list)
    dropped_checks: List[CheckConstraint] = field(default_factory=list)
    property_changes: List[str] = field(default_factory=list)
    added_exclusion_constraints: List[ExclusionConstraint] = field(default_factory=list)
    dropped_exclusion_constraints: List[ExclusionConstraint] = field(default_factory=list)
    modified_exclusion_constraints: List[tuple[ExclusionConstraint, ExclusionConstraint]] = field(default_factory=list)
    new_table_obj: Optional[Any] = None # Holds the full new Table object for context
    
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
            "dropped_fks": [fk.to_dict() for fk in self.dropped_fks],
            "modified_fks": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_fks
            ],
            "added_checks": [c.to_dict() for c in self.added_checks],
            "dropped_checks": [c.to_dict() for c in self.dropped_checks],
            "added_exclusion_constraints": [c.to_dict() for c in self.added_exclusion_constraints],
            "dropped_exclusion_constraints": [c.to_dict() for c in self.dropped_exclusion_constraints],
            "modified_exclusion_constraints": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_exclusion_constraints
            ],
            "property_changes": self.property_changes
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
    new_domains: List[CustomObject] = field(default_factory=list)
    dropped_domains: List[CustomObject] = field(default_factory=list)
    modified_domains: List[tuple[CustomObject, CustomObject]] = field(default_factory=list)
    new_types: List[CustomObject] = field(default_factory=list)
    dropped_types: List[CustomObject] = field(default_factory=list)
    modified_types: List[tuple[CustomObject, CustomObject]] = field(default_factory=list)
    new_policies: List[CustomObject] = field(default_factory=list)
    dropped_policies: List[CustomObject] = field(default_factory=list)
    modified_policies: List[tuple[CustomObject, CustomObject]] = field(default_factory=list)
    
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
            ],
            "new_policies": [o.to_dict() for o in self.new_policies],
            "dropped_policies": [o.to_dict() for o in self.dropped_policies],
            "modified_policies": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_policies
            ],
            "new_domains": [o.to_dict() for o in self.new_domains],
            "dropped_domains": [o.to_dict() for o in self.dropped_domains],
            "modified_domains": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_domains
            ],
            "new_types": [o.to_dict() for o in self.new_types],
            "dropped_types": [o.to_dict() for o in self.dropped_types],
            "modified_types": [
                {"old": old.to_dict(), "new": new.to_dict()} 
                for old, new in self.modified_types
            ]
        } 

class Comparator:
    def _compare_lists(self, old_list, new_list, plan_new, plan_dropped):
        old_objs = {o.name: o for o in old_list}
        new_objs = {o.name: o for o in new_list}
        
        for name, obj in new_objs.items():
            if name not in old_objs:
                plan_new.append(obj)
                
        for name, obj in old_objs.items():
            if name not in new_objs:
                plan_dropped.append(obj)

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

        # Helper for other custom objects
        def compare_collection(old_list, new_list, added_list, dropped_list, modified_list):
            old_dict = {o.name: o for o in old_list}
            new_dict = {o.name: o for o in new_list}
            
            for name, obj in new_dict.items():
                if name not in old_dict:
                    added_list.append(obj)
                else:
                    old_obj = old_dict[name]
                    if old_obj.properties != obj.properties:
                        modified_list.append((old_obj, obj))
                        
            for name, obj in old_dict.items():
                if name not in new_dict:
                    dropped_list.append(obj)

        compare_collection(old_schema.domains, new_schema.domains, plan.new_domains, plan.dropped_domains, plan.modified_domains)
        compare_collection(old_schema.types, new_schema.types, plan.new_types, plan.dropped_types, plan.modified_types)
        compare_collection(old_schema.policies, new_schema.policies, plan.new_policies, plan.dropped_policies, plan.modified_policies)

        return plan

    def _compare_tables(self, old_table: Table, new_table: Table) -> Optional[TableDiff]:
        diff = TableDiff(table_name=new_table.name, new_table_obj=new_table)
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
            else:
                # Check for modifications (e.g. comment)
                new_idx = new_indexes[name]
                if idx.comment != new_idx.comment:
                     # We don't have a specific list for modified indexes in TableDiff yet,
                     # or we can treat it as a property change?
                     # Existing code doesn't have modified_indexes list in TableDiff.
                     # Let's add it or use property_changes?
                     # Usually indexes are dropped/recreated if definition changes.
                     # But for comments, we can just report it.
                     # Let's add specific field to TableDiff if possible, or just append to property_changes
                     # But main.py needs to handle it.
                     # Let's check TableDiff definition first.
                     pass
                     
                # Actually, let's look at how we handle this.
                # If we want to support "Comment on Index", we should probably track it.
                # Let's see if we can just treat it as a property change for the TABLE?
                # "Index idx comment changed"
                if idx.comment != new_idx.comment:
                    diff.property_changes.append(f"Index {name} Comment: {idx.comment} -> {new_idx.comment}")
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
            else:
                new_fk = new_fks[name]
                # Check for modification
                # We need to compare properties: ref_table, ref_cols, on_delete, on_update
                is_modified = False
                if fk.ref_table != new_fk.ref_table: is_modified = True
                if fk.column_names != new_fk.column_names: is_modified = True
                if fk.ref_column_names != new_fk.ref_column_names: is_modified = True
                if fk.on_delete != new_fk.on_delete: is_modified = True
                if fk.on_update != new_fk.on_update: is_modified = True
                
                if is_modified:
                    # Track as modified FK - will be dropped and recreated
                    diff.modified_fks.append((fk, new_fk))
                    has_changes = True
        
        # Check Constraints
        old_checks = {c.name: c for c in old_table.check_constraints}
        new_checks = {c.name: c for c in new_table.check_constraints}
        
        for name, check in new_checks.items():
            if name not in old_checks:
                diff.added_checks.append(check)
                has_changes = True
        
        for name, check in old_checks.items():
            if name not in new_checks:
                diff.dropped_checks.append(check)
                has_changes = True

        # Exclusion Constraints
        old_excl = {c.name: c for c in old_table.exclusion_constraints}
        new_excl = {c.name: c for c in new_table.exclusion_constraints}
        
        for name, excl in new_excl.items():
            if name not in old_excl:
                diff.added_exclusion_constraints.append(excl)
                has_changes = True
                
        for name, excl in old_excl.items():
            if name not in new_excl:
                diff.dropped_exclusion_constraints.append(excl)
                has_changes = True
            else:
                new_c = new_excl[name]
                if excl.method != new_c.method or excl.elements != new_c.elements or excl.comment != new_c.comment:
                    diff.modified_exclusion_constraints.append((excl, new_c))
                    has_changes = True
        
        # Table Properties
        if old_table.cluster_by != new_table.cluster_by:
            diff.property_changes.append(f"Cluster Key: {old_table.cluster_by} -> {new_table.cluster_by}")
            has_changes = True
            
        if old_table.retention_days != new_table.retention_days:
            diff.property_changes.append(f"Retention: {old_table.retention_days} -> {new_table.retention_days}")
            has_changes = True
            
        if old_table.comment != new_table.comment:
            diff.property_changes.append(f"Comment: {old_table.comment} -> {new_table.comment}")
            has_changes = True
            
        if old_table.is_transient != new_table.is_transient:
            diff.property_changes.append(f"Transient: {old_table.is_transient} -> {new_table.is_transient}")
            has_changes = True
            
        if old_table.primary_key_name != new_table.primary_key_name:
            diff.property_changes.append(f"Primary Key Name: {old_table.primary_key_name} -> {new_table.primary_key_name}")
            has_changes = True

        # MySQL Specifics
        if old_table.engine != new_table.engine:
            diff.property_changes.append(f"Engine: {old_table.engine} -> {new_table.engine}")
            has_changes = True

        if old_table.row_format != new_table.row_format:
            diff.property_changes.append(f"Row Format: {old_table.row_format} -> {new_table.row_format}")
            has_changes = True

        # SQLite Specifics
        if old_table.is_strict != new_table.is_strict:
            diff.property_changes.append(f"Strict: {old_table.is_strict} -> {new_table.is_strict}")
            has_changes = True

        if old_table.without_rowid != new_table.without_rowid:
            diff.property_changes.append(f"Without RowID: {old_table.without_rowid} -> {new_table.without_rowid}")
            has_changes = True
            
        # Policies
        old_policies = set(old_table.policies)
        new_policies = set(new_table.policies)
        if old_policies != new_policies:
            added = new_policies - old_policies
            removed = old_policies - new_policies
            for p in added:
                diff.property_changes.append(f"Policy: {p}")
            for p in removed:
                # Format: "MASKING POLICY name ON col" -> "Unset Policy: name ON col"
                # Or just "Unset Policy" if the test expects that
                # Test expects "Unset Policy" or "Drop Policy"
                if "ROW ACCESS POLICY" in p:
                    diff.property_changes.append(f"Drop Policy: {p}")
                else:
                    diff.property_changes.append(f"Unset Policy: {p}")
            has_changes = True
            
        # Tags
        if old_table.tags != new_table.tags:
            # Check for unset tags
            old_tags = set(old_table.tags.keys())
            new_tags = set(new_table.tags.keys())
            
            added_tags = new_tags - old_tags
            removed_tags = old_tags - new_tags
            modified_tags = {k for k in old_tags & new_tags if old_table.tags[k] != new_table.tags[k]}
            
            for t in added_tags:
                diff.property_changes.append(f"Tag: {t}={new_table.tags[t]}")
            for t in removed_tags:
                diff.property_changes.append(f"Unset Tag: {t}")
            for t in modified_tags:
                diff.property_changes.append(f"Tag: {t} {old_table.tags[t]} -> {new_table.tags[t]}")
                
            has_changes = True
            
        # Other Dialect Properties
        if old_table.partition_by != new_table.partition_by:
            diff.property_changes.append(f"Partition: {old_table.partition_by} -> {new_table.partition_by}")
            has_changes = True
            
        if old_table.tablespace != new_table.tablespace:
            diff.property_changes.append(f"Tablespace: {old_table.tablespace} -> {new_table.tablespace}")
            has_changes = True

        if old_table.database_name != new_table.database_name:
            diff.property_changes.append(f"Database: {old_table.database_name} -> {new_table.database_name}")
            has_changes = True
            
        if old_table.stogroup != new_table.stogroup:
            diff.property_changes.append(f"Stogroup: {old_table.stogroup} -> {new_table.stogroup}")
            has_changes = True
            
        if old_table.priqty != new_table.priqty:
            diff.property_changes.append(f"Priqty: {old_table.priqty} -> {new_table.priqty}")
            has_changes = True
            
        if old_table.secqty != new_table.secqty:
            diff.property_changes.append(f"Secqty: {old_table.secqty} -> {new_table.secqty}")
            has_changes = True
            
        if old_table.audit != new_table.audit:
            diff.property_changes.append(f"Audit: {old_table.audit} -> {new_table.audit}")
            has_changes = True
            
        if old_table.ccsid != new_table.ccsid:
            diff.property_changes.append(f"CCSID: {old_table.ccsid} -> {new_table.ccsid}")
            has_changes = True
            
        if old_table.without_rowid != new_table.without_rowid:
            diff.property_changes.append(f"Without RowID: {old_table.without_rowid} -> {new_table.without_rowid}")
            has_changes = True
            
        # Postgres Properties
        if old_table.inherits != new_table.inherits:
            diff.property_changes.append(f"Inherits: {old_table.inherits} -> {new_table.inherits}")
            has_changes = True
            
        if old_table.row_security != new_table.row_security:
            diff.property_changes.append(f"Row Security: {old_table.row_security} -> {new_table.row_security}")
            has_changes = True
            
        if old_table.partition_of != new_table.partition_of:
            diff.property_changes.append(f"Partition Of: {old_table.partition_of} -> {new_table.partition_of}")
            has_changes = True

        if old_table.partition_bound != new_table.partition_bound:
            diff.property_changes.append(f"Partition Bound: {old_table.partition_bound} -> {new_table.partition_bound}")
            has_changes = True

        return diff if has_changes else None

    def _is_column_modified(self, old_col: Column, new_col: Column) -> Optional[List[str]]:
        changes = []
        if old_col.data_type != new_col.data_type:
            changes.append(f"Data Type: {old_col.data_type} -> {new_col.data_type}")
        if old_col.is_nullable != new_col.is_nullable:
            changes.append(f"Nullable: {old_col.is_nullable} -> {new_col.is_nullable}")
        if old_col.default_value != new_col.default_value:
            changes.append(f"Default: {old_col.default_value} -> {new_col.default_value}")
        if old_col.is_primary_key != new_col.is_primary_key:
            changes.append(f"Primary Key: {old_col.is_primary_key} -> {new_col.is_primary_key}")
        if old_col.comment != new_col.comment:
            changes.append(f"Comment: {old_col.comment} -> {new_col.comment}")
        if old_col.collation != new_col.collation:
            changes.append(f"Collation: {old_col.collation} -> {new_col.collation}")
        if old_col.masking_policy != new_col.masking_policy:
            changes.append(f"Masking Policy: {old_col.masking_policy} -> {new_col.masking_policy}")
        if old_col.is_identity != new_col.is_identity:
            changes.append(f"Identity: {old_col.is_identity} -> {new_col.is_identity}")
        if old_col.identity_start != new_col.identity_start:
            changes.append(f"Identity Start: {old_col.identity_start} -> {new_col.identity_start}")
        if old_col.identity_step != new_col.identity_step:
            changes.append(f"Identity Step: {old_col.identity_step} -> {new_col.identity_step}")
        if old_col.is_generated != new_col.is_generated:
            changes.append(f"Generated: {old_col.is_generated} -> {new_col.is_generated}")
        if old_col.generation_expression != new_col.generation_expression:
            changes.append(f"Generation Expr: {old_col.generation_expression} -> {new_col.generation_expression}")
        if old_col.identity_cycle != new_col.identity_cycle:
            changes.append(f"Identity Cycle: {old_col.identity_cycle} -> {new_col.identity_cycle}")
            
        return changes if changes else None
