from pyspark.sql import SparkSession
from pysparky.spark_configs import AwsS3TablesSparkConfig

conf = AwsS3TablesSparkConfig(
    catalog_name="s3tablescatalog/tablebucket",
    table_bucket_arn="arn:aws:s3tables:us-east-1:886416940696:bucket/tablebucket",
    jars_packages=(
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        "software.amazon.awssdk:s3tables:2.29.26",
        "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.5",
    ),
).get_spark_conf()
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.read.table(f"mynamespace.daily_sales").show()
