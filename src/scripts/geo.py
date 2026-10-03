from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def get_geo(spark: SparkSession, geo_path: str) -> DataFrame:
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("city", StringType()),
        StructField("geo_lat", StringType()),
        StructField("geo_lon", StringType()),
        StructField("timezone", StringType())
    ])

    geo = spark.read.csv(header=True, path=geo_path, sep=';',schema=schema)

    geo = (
        geo
        .withColumn(
            "geo_lat",
            F.regexp_replace("geo_lat", ",", ".").cast(DecimalType(7, 4))
        )
        .withColumn(
            "geo_lon",
            F.regexp_replace("geo_lon", ",", ".").cast(DecimalType(7, 4))
        )
    )
    return geo