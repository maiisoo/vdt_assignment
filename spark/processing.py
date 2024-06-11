import unidecode
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date
from pyspark.sql.types import IntegerType, TimestampType
from config import *


def transform_name(name):
    name = unidecode.unidecode(name)
    name = name.replace(" ", "_")
    return name


def process_result(join_df, name):
    result_df = join_df.filter(col("student_name") == name)

    result_df = result_df.groupBy("date",
                                  "student_code",
                                  "student_name",
                                  "activity") \
        .agg(sum("numberOfFile").alias("totalFile")) \
        .orderBy(col("date"))

    # Save output
    file_name = transform_name(name)
        # Save to HDFS
    save_path = f"{HDFS_PATH}/result/{file_name}"
    result_df.repartition(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(save_path)
        # Save to local
    result_df.repartition(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"result/{file_name}.csv")

if __name__ == '__main__':
    # Init Spark session
    spark = SparkSession.builder \
        .appName("Assigment") \
        .master(MASTER_URL) \
        .getOrCreate()

    # Load the parquet data from HDFS
    parquet_file_path = f"{HDFS_PATH}/raw_zone/fact/activity/*"
    action_df = spark.read.parquet(parquet_file_path)
    # cast datetype
    action_df = action_df.withColumn("date", to_date(col("timestamp"), "M/d/yyyy"))

    # Load the student data
    student_df = spark.read.csv(f"{HDFS_PATH}/data/danh_sach_sv_de.csv")
    student_df = student_df.withColumnRenamed("_c0", "student_code") \
        .withColumnRenamed("_c1", "student_name")
    # cast int
    student_df = student_df.withColumn("student_code", col("student_code").cast(IntegerType()))

    # Process data
    join_df = action_df.join(student_df, action_df["student_code"] == student_df["student_code"]).drop(
        action_df["student_code"])

    name = "Nguyễn Mai Anh"

    process_result(join_df, name)

    spark.stop()
