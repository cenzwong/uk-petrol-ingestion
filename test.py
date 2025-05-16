from pyspark.sql import SparkSession

# I could use the root key to  access it, but don't know why the normal user key cannot
catalog_name = "s3tablescatalog/tablebucket"
table_bucket_arn = "arn:aws:s3tables:us-east-1:886416940696:bucket/tablebucket"

spark_config = {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:s3tables:2.29.26,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.5",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": catalog_name,
    f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{catalog_name}.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    f"spark.sql.catalog.{catalog_name}.warehouse": table_bucket_arn,
}

spark = (
    SparkSession.builder.config(map=spark_config)
    # .config(
    #     "spark.jars.packages",
    #     "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
    #     ",software.amazon.awssdk:s3tables:2.29.26"
    #     ",software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.5"
    # )
    # .config(
    #     "spark.sql.extensions",
    #     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # )
    # .config("spark.sql.defaultCatalog", catalog_name)
    # .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    # .config(f"spark.sql.catalog.{catalog_name}.catalog-impl","software.amazon.s3tables.iceberg.S3TablesCatalog")
    # .config(f"spark.sql.catalog.{catalog_name}.warehouse", table_bucket_arn)
    .getOrCreate()
)

spark.read.table(f"mynamespace.daily_sales").show()
