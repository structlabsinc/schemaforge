from abc import ABC, abstractmethod
from schemaforge.models import Schema

class BaseParser(ABC):
    def __init__(self, strict: bool = False):
        """
        Initialize parser.
        
        Args:
            strict: If True, raise StrictModeError on unparseable statements.
                   If False, log warnings and continue (default behavior).
        """
        self.strict = strict
    
    @abstractmethod
    def parse(self, sql_content: str) -> Schema:
        """Parses SQL content and returns a Schema object."""
        pass

