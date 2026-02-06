# hello_world.py
from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

try: 

    # Tạo DataFrame đơn giản
    df = spark.createDataFrame([("Hello", "World")], ["col1", "col2"])

    # Show dữ liệu
    df.show()

    print("===SPARK_JOB_SUCCESS===")

except Exception as e:
    print("===SPARK_JOB_FAILED===")
    raise

finally:
    spark.stop()

