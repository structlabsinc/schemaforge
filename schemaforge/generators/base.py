from abc import ABC, abstractmethod
from typing import List, Any

class BaseGenerator(ABC):
    @abstractmethod
    def generate_migration(self, migration_plan: Any) -> str:
        """Generates SQL migration script from a migration plan."""
        pass
        
    def quote_ident(self, ident: str) -> str:
        """Quotes an identifier if needed. Default implementation returns as is."""
        return ident
