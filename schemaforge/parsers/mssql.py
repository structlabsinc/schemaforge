from schemaforge.parsers.sqlglot_adapter import SqlglotParser

class MSSQLParser(SqlglotParser):
    def __init__(self, strict=False):
        super().__init__(dialect='tsql', strict=strict)

    def _preprocess(self, content: str) -> str:
        import re
        # Replace standalone GO statements with semicolon
        return re.sub(r'^\s*GO\s*$', ';', content, flags=re.MULTILINE | re.IGNORECASE)

    def _clean_type(self, data_type: str) -> str:
        dt = data_type.upper()
        if dt == 'INTEGER':
            return 'INT'
        if 'NVARCHAR(MAX)' in dt:
            return 'TEXT'
        if 'VARCHAR(MAX)' in dt:
            return 'TEXT'
        if dt == 'DATETIME2':
            return 'DATETIME'
        return data_type


