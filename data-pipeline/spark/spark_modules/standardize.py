from pyspark.sql import Column
from pyspark.sql import functions as F

def remove_specialchar(col, pattern: str = r'[^a-zA-Z0-9\s]'):
    """
    Remove special characters from the specified column using regex.
    If the content looks like an email, '@' and '.' are preserved.
    
    Args:
        col (str or Column): The input column name or Column object.
        pattern (str): Regex pattern for characters to remove. Default keeps alphanumeric and spaces.
        
    Returns:
        Column: A Column expression with special characters removed.
    """
    c = F.col(col) if isinstance(col, str) else col
    
    # Check if the string has an email-like shape (contains @ and a dot later)
    email_pattern = r'.*@.*\..*'
    
    # Pattern that keeps @ and . in addition to alphanumeric/space
    email_safe_pattern = r'[^a-zA-Z0-9\s.@]'
    
    return F.when(c.rlike(email_pattern), F.regexp_replace(c, email_safe_pattern, '')) \
            .otherwise(F.regexp_replace(c, pattern, ''))

def standardize_nulls(col, null_values: list = None):
    """
    Replace various string representations of null (e.g., "", "NULL", "nan") with actual SQL NULL.
    
    Args:
        col (str or Column): The input column name or Column object.
        null_values (list, optional): List of string values to replace with NULL. 
                                      Default: ["", "NULL", "null", "nan", "NA"]
        
    Returns:
        Column: A Column expression with standardized nulls.
    """
    if null_values is None:
        null_values = ["", "NULL", "null", "nan", "NA"]
        
    c = F.col(col) if isinstance(col, str) else col
    return F.when(c.isin(null_values), None).otherwise(c)

def emoji_remove(col):
    """
    Remove emojis from the specified column using regex.
    
    Args:
        col (str or Column): The input column name or Column object.
        
    Returns:
        Column: A Column expression with emojis removed.
    """
    # Regex for surrogate pairs (covers most emojis)
    pattern = r'[\uD800-\uDBFF][\uDC00-\uDFFF]'
    c = F.col(col) if isinstance(col, str) else col
    return F.regexp_replace(c, pattern, '')
