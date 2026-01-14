"""
SchemaForge Custom Exceptions

This module defines custom exception classes used throughout SchemaForge.
"""


class SchemaForgeError(Exception):
    """Base exception for all SchemaForge errors."""
    pass


class StrictModeError(SchemaForgeError):
    """
    Raised when strict mode encounters an unparseable or invalid SQL statement.
    
    Attributes:
        statement: The SQL statement that failed to parse
        reason: Description of why parsing failed
    """
    def __init__(self, statement: str, reason: str = "Failed to parse statement"):
        self.statement = statement
        self.reason = reason
        # Truncate long statements for readability
        display_stmt = statement[:100] + "..." if len(statement) > 100 else statement
        super().__init__(f"{reason}: {display_stmt}")


class ValidationError(SchemaForgeError):
    """Raised when schema validation fails."""
    pass


class DialectError(SchemaForgeError):
    """Raised when an unsupported or invalid dialect is specified."""
    pass
