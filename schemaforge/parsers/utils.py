import sqlparse
import re

def normalize_sql(sql: str) -> str:
    """
    Normalizes SQL string to ensure consistent comparison regardless of 
    cosmetic differences like whitespace, case, or comments.
    
    Args:
        sql (str): The raw SQL string.
        
    Returns:
        str: The normalized SQL string.
    """
    if not sql:
        return ""
        
    normalized = sqlparse.format(
        sql,
        keyword_case='upper',
        identifier_case='upper',
        strip_comments=True,
        reindent=False # Disable reindent to avoid flaky whitespace handling
    ).strip()
    
    # Collapse all whitespace to single spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Post-process to remove spaces around punctuation
    # Remove space before , ; )
    normalized = re.sub(r'\s+([,;)])', r'\1', normalized)
    # Remove space after (
    normalized = re.sub(r'\(\s+', '(', normalized)
    
    return normalized.strip()
