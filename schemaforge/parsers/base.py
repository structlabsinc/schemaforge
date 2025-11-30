from abc import ABC, abstractmethod
from schemaforge.models import Schema

class BaseParser(ABC):
    @abstractmethod
    def parse(self, sql_content: str) -> Schema:
        """Parses SQL content and returns a Schema object."""
        pass
