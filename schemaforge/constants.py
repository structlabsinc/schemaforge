"""
SchemaForge SQL Constants

Centralized definitions for SQL keywords and dialect-specific constants
to reduce magic strings throughout the codebase.
"""

from enum import Enum, auto
from typing import Set


class SQLKeyword(str, Enum):
    """Standard SQL keywords."""
    
    # DDL Keywords
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    TABLE = "TABLE"
    INDEX = "INDEX"
    VIEW = "VIEW"
    SCHEMA = "SCHEMA"
    DATABASE = "DATABASE"
    
    # Constraint Keywords
    PRIMARY = "PRIMARY"
    FOREIGN = "FOREIGN"
    KEY = "KEY"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"
    CONSTRAINT = "CONSTRAINT"
    REFERENCES = "REFERENCES"
    
    # Column Modifiers
    NOT = "NOT"
    NULL = "NULL"
    DEFAULT = "DEFAULT"
    IDENTITY = "IDENTITY"
    GENERATED = "GENERATED"
    AUTO_INCREMENT = "AUTO_INCREMENT"
    
    # Data Definition
    COLUMN = "COLUMN"
    ADD = "ADD"
    MODIFY = "MODIFY"
    
    # Actions
    ON = "ON"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    CASCADE = "CASCADE"
    SET = "SET"
    NO = "NO"
    ACTION = "ACTION"
    
    # Other
    AS = "AS"
    OR = "OR"
    REPLACE = "REPLACE"
    IF = "IF"
    EXISTS = "EXISTS"
    COMMENT = "COMMENT"
    COLLATE = "COLLATE"
    WITH = "WITH"


class TableType(str, Enum):
    """Table types across dialects."""
    
    STANDARD = "TABLE"
    TEMPORARY = "TEMPORARY"
    
    # Snowflake specific
    DYNAMIC = "DYNAMIC"
    ICEBERG = "ICEBERG"
    HYBRID = "HYBRID"
    TRANSIENT = "TRANSIENT"
    EVENT = "EVENT"
    EXTERNAL = "EXTERNAL"
    
    # PostgreSQL specific
    UNLOGGED = "UNLOGGED"
    PARTITIONED = "PARTITIONED"
    FOREIGN = "FOREIGN"
    
    # DB2 specific
    AUX_TABLE = "AUX TABLE"
    HISTORY_TABLE = "HISTORY TABLE"
    
    # SQLite specific
    STRICT = "STRICT"
    VIRTUAL = "VIRTUAL"


class Dialect(str, Enum):
    """Supported SQL dialects."""
    
    MYSQL = "mysql"
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    ORACLE = "oracle"
    DB2 = "db2"
    SNOWFLAKE = "snowflake"


# Keywords that should NOT be used as unquoted identifiers
RESERVED_KEYWORDS: Set[str] = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
    "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "INDEX",
    "VIEW", "DATABASE", "SCHEMA", "PRIMARY", "FOREIGN", "KEY", "UNIQUE",
    "CHECK", "CONSTRAINT", "REFERENCES", "DEFAULT", "ON", "CASCADE", "SET",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL",
    "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION", "INTERSECT",
    "EXCEPT", "ALL", "DISTINCT", "AS", "IN", "EXISTS", "BETWEEN", "LIKE",
    "IS", "CASE", "WHEN", "THEN", "ELSE", "END", "IF", "BEGIN", "COMMIT",
    "ROLLBACK", "TRANSACTION", "USER", "ROLE", "GRANT", "REVOKE",
}

# Dialect-specific reserved keywords extensions
DB2_RESERVED: Set[str] = RESERVED_KEYWORDS | {
    "STOGROUP", "PRIQTY", "SECQTY", "CCSID", "PERIOD", "VERSIONING",
    "PARTITION", "CLUSTER", "AUXILIARY", "AUDIT",
}

SNOWFLAKE_RESERVED: Set[str] = RESERVED_KEYWORDS | {
    "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY", "STREAM",
    "TASK", "PIPE", "STAGE", "FILE", "FORMAT", "WAREHOUSE", "RESOURCE",
    "MONITOR", "SHARE", "POLICY", "TAG", "MASKING", "CLUSTER",
}

POSTGRES_RESERVED: Set[str] = RESERVED_KEYWORDS | {
    "SERIAL", "BIGSERIAL", "SMALLSERIAL", "RETURNING", "WINDOW",
    "PARTITION", "TABLESPACE", "EXTENSION", "DOMAIN", "TYPE",
    "TRIGGER", "FUNCTION", "PROCEDURE", "OPERATOR", "AGGREGATE",
}
