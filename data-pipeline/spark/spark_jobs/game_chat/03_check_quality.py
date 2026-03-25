from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
import sys

def check_data_quality(df, dataset_name):
    """
    Perform comprehensive data quality checks on chat message data.
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
    duplicate_count = df.count() - df.dropDuplicates(["message_id"]).count()
    print(f"   - Duplicate message_ids: {duplicate_count:,}")

    # 4. Data Validation
    print("\n4. DATA VALIDATION:")

    # Valid message types
    valid_msg_types = ['public', 'private', 'system', 'game_announcement', 'support']
    invalid_types = df.filter(~F.col("message_type").isin(valid_msg_types)).count()
    print(f"   - Invalid message types: {invalid_types:,}")

    # Valid statuses
    valid_statuses = ['sent', 'delivered', 'read']
    invalid_status = df.filter(~F.col("status").isin(valid_statuses)).count()
    print(f"   - Invalid status values: {invalid_status:,}")

    # Valid languages
    valid_languages = ['en', 'vi', 'th']
    invalid_lang = df.filter(~F.col("language").isin(valid_languages)).count()
    print(f"   - Invalid language values: {invalid_lang:,}")

    # Empty messages
    empty_messages = df.filter(
        F.col("message_content").isNull() |
        (F.trim(F.col("message_content")) == "")
    ).count()
    print(f"   - Empty message content: {empty_messages:,}")

    # 5. Value Distribution
    print("\n5. VALUE DISTRIBUTION:")

    # Message type distribution
    print("   - Message type distribution:")
    df.groupBy("message_type").count().orderBy(F.desc("count")).show(truncate=False)

    # Channel distribution
    print("   - Channel distribution:")
    df.groupBy("channel").count().orderBy(F.desc("count")).show(truncate=False)

    # Language distribution
    print("   - Language distribution:")
    df.groupBy("language").count().orderBy(F.desc("count")).show(truncate=False)

    # 6. Message Statistics
    print("\n6. MESSAGE STATISTICS:")
    msg_stats = df.select(
        F.avg(F.length(F.col("message_content"))).alias("avg_length"),
        F.min(F.length(F.col("message_content"))).alias("min_length"),
        F.max(F.length(F.col("message_content"))).alias("max_length")
    ).collect()[0]

    print(f"   - Average message length: {msg_stats['avg_length']:.2f} chars")
    print(f"   - Min message length: {msg_stats['min_length']} chars")
    print(f"   - Max message length: {msg_stats['max_length']} chars")

    # 7. Date Range
    print("\n7. DATE RANGE:")
    date_stats = df.select(
        F.min("timestamp").alias("min_date"),
        F.max("timestamp").alias("max_date")
    ).collect()[0]
    print(f"   - Earliest: {date_stats['min_date']}")
    print(f"   - Latest: {date_stats['max_date']}")

    # 8. User Activity
    print("\n8. USER ACTIVITY:")
    total_users = df.select("user_id").distinct().count()
    print(f"   - Total unique users: {total_users:,}")

    # Top 10 most active users
    print("\n   - Top 10 most active users:")
    df.groupBy("user_id", "username") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(10) \
        .show(truncate=False)

    # 9. Private Message Validation
    print("\n9. PRIVATE MESSAGE VALIDATION:")
    private_msgs = df.filter(F.col("message_type") == "private")
    private_count = private_msgs.count()
    private_missing_recipient = private_msgs.filter(F.col("recipient_id").isNull()).count()

    print(f"   - Total private messages: {private_count:,}")
    print(f"   - Private messages missing recipient: {private_missing_recipient:,}")

    # 10. Quality Score
    print("\n10. OVERALL QUALITY SCORE:")
    issues = (duplicate_count + invalid_types + invalid_status +
              invalid_lang + empty_messages + private_missing_recipient)
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
        "invalid_types": invalid_types,
        "invalid_status": invalid_status,
        "invalid_lang": invalid_lang,
        "empty_messages": empty_messages,
        "quality_score": quality_score
    }

def main():
    spark = get_spark_session("02_Check_GameChat_Data_Quality")

    # Check multiple data sources
    sources = [
        ("s3a://raw/merged_chat_messages", " merged chat messages - Raw")
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