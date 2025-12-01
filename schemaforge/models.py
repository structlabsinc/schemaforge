from dataclasses import dataclass, field
from typing import List, Optional, Any

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
            "identity_step": self.identity_step
        }

@dataclass
class CheckConstraint:
    name: str
    expression: str
    
    def to_dict(self):
        return {
            "name": self.name,
            "expression": self.expression
        }

@dataclass
class ForeignKey:
    name: str
    column_names: List[str]
    ref_table: str
    ref_column_names: List[str]
    
    def to_dict(self):
        return {
            "name": self.name,
            "column_names": self.column_names,
            "ref_table": self.ref_table,
            "ref_column_names": self.ref_column_names
        }

@dataclass
class Index:
    name: str
    columns: List[str]
    is_unique: bool = False
    method: Optional[str] = None # btree, gin, gist, etc.
    
    def to_dict(self):
        return {
            "name": self.name,
            "columns": self.columns,
            "is_unique": self.is_unique,
            "method": self.method
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
    
    # Snowflake Specifics
    table_type: str = "Table" # e.g. "Table", "Dynamic Table", "Iceberg Table", "View", "Materialized View"
    is_transient: bool = False
    cluster_by: List[str] = field(default_factory=list)
    retention_days: Optional[int] = None
    comment: Optional[str] = None
    policies: List[str] = field(default_factory=list)
    tags: dict = field(default_factory=dict)
    primary_key_name: Optional[str] = None # Added for Named Constraints
    
    # DB2/Oracle Specifics
    tablespace: Optional[str] = None
    partition_by: Optional[str] = None
    
    # Postgres Specifics
    is_unlogged: bool = False
    
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
            "table_type": self.table_type,
            "is_transient": self.is_transient,
            "cluster_by": self.cluster_by,
            "retention_days": self.retention_days,
            "comment": self.comment,
            "policies": self.policies,
            "tags": self.tags,
            "tablespace": self.tablespace,
            "partition_by": self.partition_by,
            "is_unlogged": self.is_unlogged,
            "is_strict": self.is_strict,
            "without_rowid": self.without_rowid
        }

@dataclass
class Schema:
    tables: List[Table] = field(default_factory=list)
    custom_objects: List[CustomObject] = field(default_factory=list)
    
    def get_table(self, name: str) -> Optional[Table]:
        for table in self.tables:
            if table.name == name:
                return table
        return None
        
    def to_dict(self):
        return {
            "tables": [t.to_dict() for t in self.tables],
            "custom_objects": [o.to_dict() for o in self.custom_objects]
        }
