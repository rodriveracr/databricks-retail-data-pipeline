from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
@dp.table(
name="bronze_retail_transactions",
comment="Raw retail data ingested with Auto Loader"
)
def bronze_retail_transactions():
    source_path = "/Volumes/workspace/default/retail_source/"
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .load(source_path)
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
    )