from pyspark.sql import DataFrame

def duplicate_check(df: DataFrame, subset=None):
    """
    Check if the input DataFrame has duplicates.
    
    Args:
        df (DataFrame): The input Spark DataFrame.
        subset (list, optional): List of columns to check for duplicates. None checks all columns.
        
    Returns:
        tuple: (bool, int) - (has_duplicates, duplicate_count)
    """
    total_count = df.count()
    distinct_count = df.dropDuplicates(subset).count()
    duplicate_count = total_count - distinct_count
    has_duplicates = duplicate_count > 0
    
    print(f">>> [Cleansing: Check] Total records: {total_count}")
    print(f">>> [Cleansing: Check] Duplicate records found: {duplicate_count}")
    
    return has_duplicates, duplicate_count

def duplicate_remove(df: DataFrame, subset=None):
    """
    Remove duplicates from the input DataFrame.
    
    Args:
        df (DataFrame): The input Spark DataFrame.
        subset (list, optional): List of columns to use for deduplication. None uses all columns.
        
    Returns:
        DataFrame: Deduplicated Spark DataFrame.
    """
    initial_count = df.count()
    deduped_df = df.dropDuplicates(subset)
    final_count = deduped_df.count()
    removed_count = initial_count - final_count
    
    print(f">>> [Cleansing: Remove] Records before: {initial_count}")
    print(f">>> [Cleansing: Remove] Records after: {final_count}")
    print(f">>> [Cleansing: Remove] Records removed: {removed_count}")
    
    return deduped_df
