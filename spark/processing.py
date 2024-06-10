import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import IntegerType, TimestampType
from config import *


def process_result(result_df, name):
    # Encode name
    if name != "":
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
        result_df = result_df.filter(col("student_name") == name)
        file_name = name.replace(" ", "_")
    else:
        file_name = "Full"

    # Save output
    result_df.repartition(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{SPARK_HOME}/result/{file_name}.csv")


if __name__ == '__main__':
    # Init Spark session
    spark = SparkSession.builder \
        .appName("Assigment") \
        .master(MASTER_URL) \
        .getOrCreate()

    # Load the parquet data from HDFS
    parquet_file_path = f"{HDFS_PATH}/raw_zone/fact/activity/*"
    action_df = spark.read.parquet(parquet_file_path)
    action_df = action_df.withColumn("date", col("timestamp").cast(TimestampType()))

    # Load the student data
    student_df = spark.read.csv(f"{HDFS_PATH}/data/danh_sach_sv_de.csv")

    student_df = student_df.withColumnRenamed("_c0", "student_code") \
        .withColumnRenamed("_c1", "student_name")

    student_df = student_df.withColumn("student_code", col("student_code").cast(IntegerType()))

    # Process data
    join_df = action_df.join(student_df, action_df["student_code"] == student_df["student_code"]).drop(
        action_df["student_code"])

    result_df = join_df.groupBy("date",
                                "student_code",
                                "student_name",
                                "activity") \
        .agg(sum("numberOfFile").alias("totalFile")) \
        .orderBy(col("date"), col("student_code"))

    name = "Nguyễn Mai Anh"

    process_result(result_df, name)

    spark.stop()
