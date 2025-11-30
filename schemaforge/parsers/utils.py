import sqlparse

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
        
    return sqlparse.format(
        sql,
        keyword_case='upper',
        identifier_case='upper',
        strip_comments=True,
        reindent=True
    ).strip()
