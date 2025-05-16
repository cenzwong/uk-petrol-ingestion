from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession



def landing_to_raw_transformation(df: DataFrame) -> DataFrame:
    df = df.select("last_updated", F.explode("stations").alias("station")).select(
        F.col("last_updated"),
        F.col("station.site_id").alias("site_id"),
        F.col("station.brand").alias("brand"),
        F.col("station.address").alias("address"),
        F.col("station.postcode").alias("postcode"),
        F.col("station.location.latitude").alias("lat"),
        F.col("station.location.longitude").alias("lon"),
        F.col("station.prices.*") # add prices dynamicly
    )
    return df

def execute_landing_to_raw(spark: SparkSession, source_path: str, target_path: str) -> None:
    print(f"{source_path=}, {target_path=}")

    df = spark.read.json(
        source_path
    )
    df.select("last_updated", F.explode("stations").alias("station")).printSchema()
    raw_df = df.transform(landing_to_raw_transformation)
    raw_df.write.parquet(target_path, mode="overwrite")