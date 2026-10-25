from pyspark import pipelines as dp
from pyspark.sql.functions import col,current_timestamp

# Please edit the sample below


@dp.materialized_view(
    name="transportation.silver.city",
    comment="Cleaned and Transformed City Data",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():
    return spark.read.table("transportation.bronze.city").select(col("city_id"),col("city_name"),col("ingestion_datetime").alias("bronze_ingest_datetime")).withColumn("silver_ingest_datetime", current_timestamp())