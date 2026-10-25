from pyspark import pipelines as dp
from pyspark.sql.functions import col,current_timestamp

SOURCE_PATH = "s3://ashishsahus3bucket/transportation/trips/"

#Notes: rescue mode will not fail the process if new column is encounter in new files instead it will create rescue column
# format cloudFiles is AutoLoaded feature provided by databricks

@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw orders data from AutoLoaded",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def trip_bronze():
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .option("cloudFiles.maxFilesPerTrigger", 100) \
        .load(SOURCE_PATH)

    df = df.withColumnRenamed("distance_travelled(km)","distance_travelled_km")

    df = df.withColumn("file_name", col("_metadata.file_path")) \
        .withColumn("ingestion_datetime", current_timestamp())

    return df