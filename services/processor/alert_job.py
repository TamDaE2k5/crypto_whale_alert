import os
import sys
from pyspark.sql.functions import from_json, col, window, sum as _sum, struct, to_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from utils.config import (
    KAFKA_BROKER_INTERNAL,
    TOPIC_TRADES,
    TOPIC_ALERTS,
    DEFAULT_THRESHOLDS
)

schema = StructType([
    StructField('s', StringType()),
    StructField('p', StringType()),
    StructField('q', StringType()),
    StructField('T', LongType())
])

def main():
    # create session
    spark = SparkSession.builder.appName("Processing Realtime") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # error->log

    # read
    raw_df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER_INTERNAL) \
    .option('subscribe', TOPIC_TRADES) \
    .option("startingOffsets", "earliest").load() # subscribe -> consumer

    # data type
    trade_df = raw_df.selectExpr("CAST (value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select(
            col('data.s').alias('Symbol'),
            col('data.p').cast(DoubleType()).alias('Price'),
            col('data.q').cast(DoubleType()).alias('Quantity'),
            (col('data.T')/1000).cast(TimestampType()).alias('Timestamp') # ms ->s
        )    
#         "T": 1779091675833,   Event time
#         "s": "BTCUSDT",       Symbol
#         "p": "77155.70000000",Price
#         "q": "0.00074000",    Quantity

    trade_usd = trade_df.withColumn('usd_value',col('Price')*col('Quantity'))
#         "T": 1779091675833,   
#         "s": "BTCUSDT",       
#         "p": "77155.70000000",
#         "q": "0.00074000",    
#         'usd_value': '9182' 

    threshold = list(DEFAULT_THRESHOLDS.items())
    # {'BTC':500000} -> [(BTC,5000), (key,val)]
    threshold_df = spark.createDataFrame(threshold, ['symbol_cfg', 'threshold'])

    # aggreate 10s
    # Watermark(window (0-10s), +10s = 20s)
    aggregated_df = trade_usd.withWatermark('Timestamp', '10 second') \
                    .groupBy(col('Symbol'), window(col("Timestamp"), "10 seconds")) \
                    .agg(_sum("usd_value").alias("total_usd_value"))
    
    whale_df = aggregated_df.join(threshold_df, aggregated_df.Symbol == threshold_df.symbol_cfg, 'left') \
            .filter(col("total_usd_value") >= col("threshold"))               

    #log
    trade_usd.writeStream.format("console").option("truncate", "false").start() #-> log aggegate 10s
    whale_df.writeStream.format("console").outputMode("update").start() #-> log alert

    # (key,val) = (BTCUSD, {'window_start':'time', ... 'alert_nessage':'Spark Streaming find out Whale'})
    alert_kafka = whale_df.select(
        col("Symbol").alias("key"),
        to_json(struct(
            col("window.start").cast(StringType()).alias("window_start"),
            col("window.end").cast(StringType()).alias("window_end"),
            col("Symbol"),
            col("total_usd_value"),
            lit("Spark Streaming find out WHALE").alias("alert_message")
        )).alias("value")
    )

    checkpoint_path = "/tmp/spark_checkpoints"
    query = alert_kafka.writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_INTERNAL) \
        .option("topic", TOPIC_ALERTS) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update").start() # topic -> producer

    print(">>> Spark Whale Detector ready to start!")
    query.awaitTermination() # run anyway

if __name__ == '__main__':
   main() 