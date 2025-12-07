from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict

@dataclass
class Column:
    name: str
    data_type: str
    is_nullable: bool = True
    default_value: Optional[Any] = None
    is_primary_key: bool = False
    comment: Optional[str] = None # Added comment field
    collation: Optional[str] = None
    masking_policy: Optional[str] = None # Added for Snowflake Masking Policy
    # DB2/Oracle/Snowflake Specifics
    is_identity: bool = False
    identity_start: Optional[int] = None
    identity_step: Optional[int] = None
    
    # Postgres Specifics
    is_generated: bool = False
    generation_expression: Optional[str] = None
    identity_cycle: bool = False
    
    def __repr__(self):
        return f"Column(name='{self.name}', type='{self.data_type}')"
        
    def to_dict(self):
        return {
            "name": self.name,
            "data_type": self.data_type,
            "is_nullable": self.is_nullable,
            "default_value": str(self.default_value) if self.default_value else None,
            "is_primary_key": self.is_primary_key,
            "comment": self.comment,
            "collation": self.collation,
            "masking_policy": self.masking_policy,
            "is_identity": self.is_identity,
            "identity_start": self.identity_start,
            "identity_step": self.identity_step,
            "is_generated": self.is_generated,
            "generation_expression": self.generation_expression,
            "identity_cycle": self.identity_cycle
        }

@dataclass
class CheckConstraint:
    name: str
    expression: str
    comment: Optional[str] = None
    
    def to_dict(self):
        return {
            "name": self.name,
            "expression": self.expression,
            "comment": self.comment
        }

@dataclass
class ExclusionConstraint:
    name: str
    elements: List[str] # e.g. ["account_id WITH =", "valid_range WITH &&"]
    method: str = "gist"
    comment: Optional[str] = None
    
    def to_dict(self):
        return {
            "name": self.name,
            "elements": self.elements,
            "method": self.method,
            "comment": self.comment
        }

@dataclass
class ForeignKey:
    name: str
    column_names: List[str]
    ref_table: str
    ref_column_names: List[str]
    on_delete: Optional[str] = None
    on_update: Optional[str] = None
    comment: Optional[str] = None
    is_deferrable: bool = False
    
    def to_dict(self):
        return {
            "name": self.name,
            "column_names": self.column_names,
            "ref_table": self.ref_table,
            "ref_column_names": self.ref_column_names,
            "on_delete": self.on_delete,
            "on_update": self.on_update,
            "comment": self.comment,
            "is_deferrable": self.is_deferrable
        }

@dataclass
class Index:
    name: str
    columns: List[str]
    is_unique: bool = False
    is_primary: bool = False # Implicitly created by PK?
    method: Optional[str] = None # btree, gin, gist, etc.
    where_clause: Optional[str] = None # Partial index
    include_columns: List[str] = field(default_factory=list) # INCLUDE clause
    comment: Optional[str] = None
    properties: Dict = field(default_factory=dict) # For DB2 INCLUDE, CLUSTER, etc.
    
    def to_dict(self):
        return {
            "name": self.name,
            "columns": self.columns,
            "is_unique": self.is_unique,
            "method": self.method,
            "where_clause": self.where_clause,
            "include_columns": self.include_columns,
            "comment": self.comment
        }

@dataclass
class CustomObject:
    """
    Represents non-standard SQL objects (Stages, Pipes, Tasks, etc.)
    that are dialect-specific.
    """
    obj_type: str
    name: str
    properties: dict = field(default_factory=dict)
    
    def to_dict(self):
        return {
            "type": self.obj_type,
            "name": self.name,
            "properties": self.properties
        }

@dataclass
class Table:
    name: str
    columns: List[Column] = field(default_factory=list)
    indexes: List[Index] = field(default_factory=list)
    foreign_keys: List[ForeignKey] = field(default_factory=list)
    check_constraints: List[CheckConstraint] = field(default_factory=list)
    exclusion_constraints: List[ExclusionConstraint] = field(default_factory=list)
    
    # Snowflake Specifics
    table_type: str = "Table" # e.g. "Table", "Dynamic Table", "Iceberg Table", "View", "Materialized View"
    is_transient: bool = False
    cluster_by: List[str] = field(default_factory=list)
    retention_days: Optional[int] = None
    comment: Optional[str] = None
    policies: List[str] = field(default_factory=list)
    tags: dict = field(default_factory=dict)
    primary_key_name: Optional[str] = None # Added for Named Constraints
    period_for: Optional[str] = None # DB2 Temporal: "SYSTEM_TIME (start, end)"
    
    # DB2/Oracle Specifics
    tablespace: Optional[str] = None # For z/OS: DATABASE.TABLESPACE
    database_name: Optional[str] = None # Specific for logical grouping if parsed separately
    partition_by: Optional[str] = None
    
    # DB2 z/OS Specifics
    stogroup: Optional[str] = None
    priqty: Optional[int] = None
    secqty: Optional[int] = None
    audit: Optional[str] = None # NONE, CHANGES, ALL
    ccsid: Optional[str] = None # EBCDIC, ASCII, UNICODE
    
    # Postgres Specifics
    is_unlogged: bool = False
    inherits: Optional[str] = None
    row_security: bool = False
    partition_of: Optional[str] = None # If it's a partition
    partition_bound: Optional[str] = None
    
    # SQLite Specifics
    is_strict: bool = False
    without_rowid: bool = False
    
    def get_column(self, name: str) -> Optional[Column]:
        for col in self.columns:
            if col.name == name:
                return col
        return None
        
    def to_dict(self):
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "indexes": [i.to_dict() for i in self.indexes],
            "foreign_keys": [fk.to_dict() for fk in self.foreign_keys],
            "check_constraints": [c.to_dict() for c in self.check_constraints],
            "exclusion_constraints": [c.to_dict() for c in self.exclusion_constraints],
            "table_type": self.table_type,
            "is_transient": self.is_transient,
            "cluster_by": self.cluster_by,
            "retention_days": self.retention_days,
            "comment": self.comment,
            "policies": self.policies,
            "tags": self.tags,
            "tablespace": self.tablespace,
            "database_name": self.database_name,
            "partition_by": self.partition_by,
            "stogroup": self.stogroup,
            "priqty": self.priqty,
            "secqty": self.secqty,
            "audit": self.audit,
            "ccsid": self.ccsid,
            "is_unlogged": self.is_unlogged,
            "inherits": self.inherits,
            "row_security": self.row_security,
            "partition_of": self.partition_of,
            "partition_bound": self.partition_bound,
            "is_strict": self.is_strict,
            "without_rowid": self.without_rowid
        }

@dataclass
class Schema:
    tables: List[Table] = field(default_factory=list)
    custom_objects: List[CustomObject] = field(default_factory=list)
    policies: List[CustomObject] = field(default_factory=list) # Using CustomObject for now for simplicity or could make specialized
    domains: List[CustomObject] = field(default_factory=list)
    types: List[CustomObject] = field(default_factory=list)
    
    def get_table(self, name: str) -> Optional[Table]:
        for table in self.tables:
            if table.name == name:
                return table
        return None
        
    def to_dict(self):
        return {
            "tables": [t.to_dict() for t in self.tables],
            "custom_objects": [o.to_dict() for o in self.custom_objects],
            "policies": [o.to_dict() for o in self.policies],
            "domains": [o.to_dict() for o in self.domains],
            "types": [o.to_dict() for o in self.types]
        }
