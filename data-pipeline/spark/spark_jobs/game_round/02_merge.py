from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import traceback

def main():
    spark = get_spark_session("03_Merge_GameRound_CSV_with_Existing")

    # Paths
    csv_source_path = "s3a://raw/csv_hilo_game_rounds"
    db_source_path = "s3a://raw/db_hilo_game_rounds"
    merged_output_path = "s3a://raw/merged_hilo_game_rounds"

    print("="*60)
    print("MERGING CSV DATA WITH EXISTING DATABASE DATA")
    print("="*60)

    # Read CSV source
    print(f"\n1. Reading CSV source from {csv_source_path}")
    try:
        df_csv = MinIOConnector.read_parquet(spark, csv_source_path)
        csv_count = df_csv.count()
        print(f"   - CSV records: {csv_count:,}")
    except Exception as e:
        print(f"   - WARNING: CSV source not found: {e}")
        df_csv = None

    # Read database source
    print(f"\n2. Reading database source from {db_source_path}")
    try:
        df_db = MinIOConnector.read_parquet(spark, db_source_path)
        db_count = df_db.count()
        print(f"   - Database records: {db_count:,}")
    except Exception as e:
        print(f"   - WARNING: Database source not found: {e}")
        df_db = None

    # Check if we have data to merge
    if df_csv is None and df_db is None:
        print("\nERROR: No data sources available!")
        spark.stop()
        sys.exit(1)

    # Merge logic
    print("\n3. Merging data...")

    if df_csv is None:
        print("   - Only database source available, using as-is")
        df_merged = df_db
    elif df_db is None:
        print("   - Only CSV source available, using as-is")
        df_merged = df_csv
    else:
        # Both sources available - perform union and deduplication
        print("   - Both sources available, combining...")

        # Add source tag
        df_csv_tagged = df_csv.withColumn("data_source", F.lit("CSV"))
        df_db_tagged = df_db.withColumn("data_source", F.lit("Database"))

        # Union both datasets
        df_union = df_csv_tagged.union(df_db_tagged)
        union_count = df_union.count()
        print(f"   - Combined records (before dedup): {union_count:,}")

        # Deduplication strategy: Keep the latest record per round_id based on timestamp
        # If timestamps are equal, prefer database source
        window_spec = Window.partitionBy("round_id").orderBy(
            F.desc("timestamp"),
            F.when(F.col("data_source") == "Database", 1).otherwise(2)
        )

        df_merged = (df_union
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        merged_count = df_merged.count()
        duplicates_removed = union_count - merged_count

        print(f"   - Deduplicated records: {merged_count:,}")
        print(f"   - Duplicates removed: {duplicates_removed:,}")

        # Show source distribution
        print("\n   - Source distribution in merged data:")
        df_merged.groupBy("data_source").count().show()

    # Data quality validation before writing
    print("\n4. Validating merged data...")

    # Check for critical nulls
    null_check = df_merged.filter(
        F.col("round_id").isNull() |
        F.col("user_id").isNull()
    ).count()

    if null_check > 0:
        print(f"   - WARNING: Found {null_check} records with null critical fields!")

    # Check data ranges
    invalid_dice = df_merged.filter(
        (F.col("die1") < 1) | (F.col("die1") > 6) |
        (F.col("die2") < 1) | (F.col("die2") > 6) |
        (F.col("die3") < 1) | (F.col("die3") > 6)
    ).count()

    if invalid_dice > 0:
        print(f"   - WARNING: Found {invalid_dice} records with invalid dice values!")

    print("   - Validation complete")

    # Show sample of merged data
    print("\n5. Merged data sample:")
    df_merged.select(
        "round_id", "user_id", "timestamp", "bet_type",
        "bet_amount", "result", "data_source"
    ).show(10, truncate=False)

    # Write merged data
    print(f"\n6. Writing merged data to {merged_output_path}")
    try:
        MinIOConnector.write_parquet(df_merged, merged_output_path, mode="overwrite")
        final_count = df_merged.count()
        print(f"   - Successfully wrote {final_count:,} records")
    except Exception as e:
        print(f"ERROR: Failed to write merged data: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

    # Summary statistics
    print("\n" + "="*60)
    print("MERGE SUMMARY")
    print("="*60)
    if df_csv and df_db:
        print(f"CSV records:      {csv_count:,}")
        print(f"Database records: {db_count:,}")
        print(f"Merged records:   {final_count:,}")
        print(f"Duplicates:       {union_count - final_count:,}")
    print(f"\nOutput location:  {merged_output_path}")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    main()