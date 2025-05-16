from pyspark import SparkConf
from pyspark.sql import SparkSession
from pysparky.spark_configs import AwsS3SparkConfig

from pipeline.raw.base import execute_landing_to_raw
import boto3

s3_client = boto3.client('s3', region_name='us-east-1')

response = s3_client.list_objects_v2(Bucket="uk-fuel-price-data", Prefix="raw")
raw_file_keys = {"/".join(item['Key'].split("/")[1:-2]) for item in response.get('Contents', []) if not item['Key'].endswith('/')}
raw_file_keys

response = s3_client.list_objects_v2(Bucket="uk-fuel-price-data", Prefix="landing")
landing_file_keys = {"/".join(item['Key'].split("/")[1:-1]) for item in response.get('Contents', []) if not item['Key'].endswith('/')}
landing_file_keys


if __name__ == "__main__":
    conf: SparkConf = AwsS3SparkConfig(jars_packages=("org.apache.hadoop:hadoop-aws:3.3.4",)).get_spark_conf()

    spark = (
        SparkSession.builder
        .config(conf=conf)
        .appName("read-s3-with-spark")
        .getOrCreate()
    )

    for key in landing_file_keys - raw_file_keys:
        print(key)

        source_path = f"s3a://uk-fuel-price-data/landing/{key}/fuel_prices_data.json"
        target_path = source_path.replace("landing", "raw").replace("json", "parquet")
        execute_landing_to_raw(spark, source_path, target_path)