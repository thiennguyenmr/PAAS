from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
import sys

def check_data_quality(df, dataset_name):
    """
    Perform comprehensive data quality checks on game round data.
    Returns a report dictionary with quality metrics.
    """
    print(f"\n{'='*60}")
    print(f"DATA QUALITY REPORT: {dataset_name}")
    print(f"{'='*60}\n")

    # 1. Row Count
    total_rows = df.count()
    print(f"1. TOTAL RECORDS: {total_rows:,}")

    # 2. Null/Missing Value Analysis
    print("\n2. NULL VALUE ANALYSIS:")
    for col_name in df.columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        if null_count > 0:
            print(f"   - {col_name}: {null_count:,} nulls ({null_pct:.2f}%)")

    # 3. Duplicate Analysis
    print("\n3. DUPLICATE ANALYSIS:")
    duplicate_count = df.count() - df.dropDuplicates(["round_id"]).count()
    print(f"   - Duplicate round_ids: {duplicate_count:,}")

    # 4. Data Range Validation
    print("\n4. DATA RANGE VALIDATION:")

    # Dice values (should be 1-6)
    invalid_dice = df.filter(
        (F.col("die1") < 1) | (F.col("die1") > 6) |
        (F.col("die2") < 1) | (F.col("die2") > 6) |
        (F.col("die3") < 1) | (F.col("die3") > 6)
    ).count()
    print(f"   - Invalid dice values (not 1-6): {invalid_dice:,}")

    # Total points (should be 3-18)
    invalid_total = df.filter(
        (F.col("total_points") < 3) | (F.col("total_points") > 18)
    ).count()
    print(f"   - Invalid total_points (not 3-18): {invalid_total:,}")

    # Negative bet amounts
    negative_bets = df.filter(F.col("bet_amount") <= 0).count()
    print(f"   - Negative/zero bet amounts: {negative_bets:,}")

    # 5. Value Distribution
    print("\n5. VALUE DISTRIBUTION:")

    # Result distribution
    print("   - Result distribution:")
    df.groupBy("result").count().orderBy(F.desc("count")).show(truncate=False)

    # Bet type distribution
    print("   - Top 10 bet types:")
    df.groupBy("bet_type").count().orderBy(F.desc("count")).limit(10).show(truncate=False)

    # 6. Statistical Summary
    print("\n6. STATISTICAL SUMMARY (Numeric Columns):")
    df.select("bet_amount", "payout_amount", "net_profit", "total_points").summary().show()

    # 7. Date Range
    print("\n7. DATE RANGE:")
    date_stats = df.select(
        F.min("timestamp").alias("min_date"),
        F.max("timestamp").alias("max_date")
    ).collect()[0]
    print(f"   - Earliest: {date_stats['min_date']}")
    print(f"   - Latest: {date_stats['max_date']}")

    # 8. Data Integrity Checks
    print("\n8. DATA INTEGRITY CHECKS:")

    # Check if die sum equals total_points
    incorrect_sums = df.filter(
        F.col("total_points") != (F.col("die1") + F.col("die2") + F.col("die3"))
    ).count()
    print(f"   - Incorrect dice sums: {incorrect_sums:,}")

    # Check if triple flag is correct
    incorrect_triples = df.filter(
        ((F.col("die1") == F.col("die2")) & (F.col("die2") == F.col("die3")) & (F.col("is_triple") == False)) |
        ((F.col("die1") != F.col("die2")) | (F.col("die2") != F.col("die3"))) & (F.col("is_triple") == True)
    ).count()
    print(f"   - Incorrect triple flags: {incorrect_triples:,}")

    # 9. Quality Score
    print("\n9. OVERALL QUALITY SCORE:")
    issues = duplicate_count + invalid_dice + invalid_total + negative_bets + incorrect_sums + incorrect_triples
    quality_score = max(0, 100 - (issues / total_rows * 100)) if total_rows > 0 else 0
    print(f"   - Quality Score: {quality_score:.2f}%")

    if quality_score >= 95:
        print("   - Status: ✅ EXCELLENT")
    elif quality_score >= 85:
        print("   - Status: ✓ GOOD")
    elif quality_score >= 70:
        print("   - Status: ⚠ FAIR - Review recommended")
    else:
        print("   - Status: ❌ POOR - Action required")

    print(f"\n{'='*60}\n")

    return {
        "total_rows": total_rows,
        "duplicates": duplicate_count,
        "invalid_dice": invalid_dice,
        "invalid_total": invalid_total,
        "negative_bets": negative_bets,
        "incorrect_sums": incorrect_sums,
        "incorrect_triples": incorrect_triples,
        "quality_score": quality_score
    }

def main():
    spark = get_spark_session("02_Check_GameRound_Data_Quality")

    # Check multiple data sources
    sources = [
        ("s3a://raw/merged_hilo_game_rounds", "merged_hilo_game_rounds - Raw")
    ]

    all_reports = {}

    for path, name in sources:
        try:
            print(f"\nChecking: {path}")
            df = MinIOConnector.read_parquet(spark, path)
            report = check_data_quality(df, name)
            all_reports[name] = report
        except Exception as e:
            print(f"WARNING: Could not read {path}: {e}")
            continue

    # Comparison report
    if len(all_reports) > 1:
        print("\n" + "="*60)
        print("COMPARISON REPORT")
        print("="*60)
        for name, report in all_reports.items():
            print(f"\n{name}:")
            print(f"  - Total Rows: {report['total_rows']:,}")
            print(f"  - Quality Score: {report['quality_score']:.2f}%")

    spark.stop()

if __name__ == "__main__":
    main()