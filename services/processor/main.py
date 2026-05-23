import os
import sys
from pyspark.sql.functions import (from_json, col, window, sum as _sum, struct, to_json, lit,
                                   first, max, min, last, when, count)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, BooleanType
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from utils.config import KAFKA_BROKER_INTERNAL, TOPIC_TRADES, TOPIC_ALERTS, TOPIC_CANDLES_1M, TOPIC_CANDLES_5M, DEFAULT_THRESHOLDS

schema = StructType([
    StructField('s', StringType()),
    StructField('p', StringType()),
    StructField('q', StringType()),
    StructField('T', LongType()),
    StructField('m', StringType())
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

#--------------------------------- ALERT JOB ---------------------------------------------------
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
        .outputMode("append").start() # topic -> producer, dung append de cho 20s, upodate la bi 1s nem output roi


    # -------------------------------------- Candles JOB --------------------------------------
    trade_df_2 = raw_df.selectExpr("CAST (value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select(
            col('data.s').alias('symbol'),
            col('data.p').cast(DoubleType()).alias('price'),
            col('data.q').cast(DoubleType()).alias('quantity'),
            (col('data.T')/1000).cast(TimestampType()).alias('timestamp'), # ms ->s
            col('data.m').cast(BooleanType()).alias('m')
        )  
    
    candle_1m = trade_df_2.withWatermark('timestamp', '30 second') \
                .groupBy(col('symbol'), window(col("timestamp"), "1 minute")) \
                .agg(first('price').alias('open'), max('price').alias('high'), min('price').alias('low'),
                     last('price').alias('close'), _sum('quantity').alias('volume'), 
                _sum(when(col('m')==False, col('quantity')).otherwise(0)).alias('isBuyer'), 
                _sum(when(col('m')==True, col('quantity')).otherwise(0)).alias('isSeller'), 
                count('*').alias('cnt')
            )
    
    candle_5m = trade_df_2.withWatermark('timestamp', '1 minute') \
                .groupBy(col('symbol'), window(col("timestamp"), "5 minutes")) \
                .agg(first('price').alias('open'), max('price').alias('high'), min('price').alias('low'),
                     last('price').alias('close'), _sum('quantity').alias('volume'), 
                _sum(when(col('m')==False, col('quantity')).otherwise(0)).alias('isBuyer'), 
                _sum(when(col('m')==True, col('quantity')).otherwise(0)).alias('isSeller'), 
                count('*').alias('cnt')
            )
    
    candle_1m.writeStream.format("console").outputMode("update").start()
    candle_5m.writeStream.format("console").outputMode("update").start()

    candles_1m_kafka = candle_1m.select(
        col("symbol").alias("key"),
        to_json(struct(
            col('symbol'),
            col('window.start').cast(StringType()).alias('timestamp'),
            lit('1 minute').alias('timeframe'),
            col('open'),
            col('high'),
            col('low'),
            col('close'),
            col('volume'),
            (col('isBuyer')-col('isSeller')).alias('buyer'),
            col('cnt')
        )).alias("value")
    )

    candles_5m_kafka = candle_5m.select(
        col("symbol").alias("key"),
        to_json(struct(
            col('symbol'),
            col('window.start').cast(StringType()).alias('timestamp'),
            lit('5 minutes').alias('timeframe'),
            col('open'),
            col('high'),
            col('low'),
            col('close'),
            col('volume'),
            (col('isBuyer')-col('isSeller')).alias('buyer'),
            col('cnt')
        )).alias("value")
    )

    query_1m = candles_1m_kafka.writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_INTERNAL) \
        .option("topic", TOPIC_CANDLES_1M) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/candles_1m") \
        .outputMode("append").start()

    query_5m = candles_5m_kafka.writeStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_INTERNAL) \
        .option("topic", TOPIC_CANDLES_5M) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/candles_5m") \
        .outputMode("append").start()

    print(">>> Spark Whale Detector and Candles ready to start!")
    spark.streams.awaitAnyTermination()
    
if __name__ == '__main__':
   main() 