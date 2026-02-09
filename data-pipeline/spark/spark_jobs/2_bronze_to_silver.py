#init
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_modules.minio import MinioStorage
from spark_modules.cleansing import duplicate_check, duplicate_remove
from spark_modules.standardize import remove_specialchar, standardize_nulls, emoji_remove
from pyspark.sql.types import StringType

#---------Maincode

def main():
    print("--- BRONZE TO SILVER PROCESSING STARTED ---")
    
    # 1. Initialize Bronze Connection (and Spark Session)
    bronze = MinioStorage()
    try:
        print(">>> [Init] Connecting to Bronze Layer...")
        bronze.load_config("minio_bronze")
        bronze.init_client("Bronze to Silver Processing")

        # 2. Initialize Silver Connection
        print(">>> [Init] Connecting to Silver Layer...")
        silver = MinioStorage()
        silver.load_config("minio_silver")
        silver.spark = bronze.spark
        
        FOLDER_NAME = "sample_data"
        
        # 3. List files in Bronze folder
        print(f">>> [Process] Scanning folder: {FOLDER_NAME}")
        # list_files returns "objects" which are files in a bucket context.
        files = bronze.list_files(FOLDER_NAME)
        
        if not files:
            print("!!! No files found in source folder.")
            return

        print(f">>> Found {len(files)} files to process.")

        # 4. Process each file
        for file_info in files:
            file_name = file_info['name']
            
            # Skip directory markers if any (though list_minio_files filters some)
            if file_name.endswith('/'):
                continue

            print(f"\n--------------------------------------------------")
            print(f">>> Processing file: {file_name}")
            
            # Construct paths
            # Source: sample_data/filename.csv
            source_path = f"{FOLDER_NAME}/{file_name}"
            
            # Target: sample_data/filename (as parquet folder)
            # We strip extension from filename for the target folder name
            target_name = os.path.splitext(file_name)[0]
            target_path = f"{FOLDER_NAME}/{target_name}"
            
            # A. Read from Bronze
            # We assume it's CSV or similar if not specified, read_file infers schema
            df = bronze.read_file(source_path)
            
            if df:
                # B. Cleansing (Duplicate Check & Remove)
                has_dups, count = duplicate_check(df)
                
                if has_dups:
                    print(f"    -> Duplicates detected! Cleaning data...")
                    df_clean = duplicate_remove(df)
                else:
                    print(f"    -> No duplicates found.")
                    df_clean = df
                
                # C. Standardization (Emoji, Special Char, Nulls)
                print(f"    -> Standardizing string columns...")
                string_cols = [f.name for f in df_clean.schema.fields if isinstance(f.dataType, StringType)]
                
                for col_name in string_cols:
                    df_clean = df_clean.withColumn(
                        col_name,
                        standardize_nulls(
                            remove_specialchar(
                                emoji_remove(col_name)
                            )
                        )
                    )
                
                # D. Write to Silver (Parquet)
                # The requirement says "switch to parquet format and save to bucket silver, folder 'sample_data'"
                print(f"    -> Writing to {target_path} (parquet)")
                silver.write_file(df_clean, target_path, format="parquet")
                
    except Exception as e:
        print(f"!!! Error in execution: {e}")
    finally:
        bronze.stop_spark()
        print("--- PROGRAM FINISHED ---")

if __name__ == "__main__":
    main()
