import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
azure_key = open('azure_key.txt').read()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DoubleType

spark = SparkSession.builder.master('local').appName('app').config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstorage.blob.core.windows.net",azure_key)

# Define Schemas
trade_schema = StructType([
    StructField('trade_dt', DateType(), True),
    StructField('file_tm', TimestampType(), True),
    StructField('event_type', StringType(), True),
    StructField('symbol', StringType(), True),
    StructField('event_tm', TimestampType(), True),
    StructField('event_seq_nb', IntegerType(), True),
    StructField('exchange', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('size', IntegerType(), True),
    ])

quote_schema = StructType([
    StructField('trade_dt', DateType(), True),
    StructField('file_tm', TimestampType(), True),
    StructField('event_type', StringType(), True),
    StructField('symbol', StringType(), True),
    StructField('event_tm', TimestampType(), True),
    StructField('event_seq_nb', IntegerType(), True),
    StructField('exchange', StringType(), True),
    StructField('bid_pr', DoubleType(), True),
    StructField('bid_size', IntegerType(), True),
    StructField('ask_pr', DoubleType(), True),
    StructField('ask_size', IntegerType(), True),
    ])

class TradesAndQuotes:

    def load_text():
        # Look in data/csv directory and it's nested folders
        df = spark.read.option('recursiveFileLookup','true').schema(quote_schema).csv("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/csv")
        # Filter for Trades
        df_trade = df.filter(df[2] == 'T')
        # Remove unused columns
        null_cols = ('ask_pr','ask_size')
        df_trade = df_trade.drop(*null_cols)
        # Create dataframe with trade schema
        df_trade_csv = spark.createDataFrame(df_trade.rdd,schema=trade_schema)
        # Filter for Quotes
        df_quote_csv = df.filter(df[2] == 'Q')
        return df_trade_csv,df_quote_csv


    def load_json():
        # Look in data/json directory and it's nested folders
        df = spark.read.option('recursiveFileLookup','true').json("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/json")
        # Filter for trades
        df_trade = df.filter(df.event_type == 'T')
        # Drop columns with no data, or no matches on other dataframe
        drop_cols = ('ask_pr','ask_size','bid_pr','bid_size','execution_id')
        df_trade = df_trade.drop(*drop_cols)
        # Create temp view to do SQL on
        df_trade = df_trade.createOrReplaceTempView("Cast")
        # Cast for datatypes
        df_trade_json = spark.sql("SELECT DATE(trade_dt),TIMESTAMP(file_tm),STRING(event_type),STRING(symbol),TIMESTAMP(event_tm),INT(event_seq_nb),STRING(exchange),DOUBLE(price),INT(size) from Cast")
        # Filter for quotes
        df_quote = df.filter(df.event_type == 'Q')
        # Drop unneeded columns
        drop_cols = ('price','size','execution_id')
        df_quote = df_quote.drop(*drop_cols)
        # Create temp view to do SQL on 
        df_quote = df_quote.createOrReplaceTempView("Cast")
        # Cast datatypes with SQL
        df_quote_json = spark.sql("SELECT DATE(trade_dt),TIMESTAMP(file_tm),STRING(event_type),STRING(symbol),TIMESTAMP(event_tm),INT(event_seq_nb),STRING(exchange),DOUBLE(bid_pr),INT(bid_size),DOUBLE(ask_pr),INT(ask_size) from Cast")

        return df_trade_json,df_quote_json 


    def combine_dfs():
        # Take info from text and json, combine together
        dfs_text = TradesAndQuotes.load_text()
        dfs_json = TradesAndQuotes.load_json()
        # Trades
        df_trade_csv = dfs_text[0]
        df_trade_json = dfs_json[0]
        df_trades = df_trade_csv.union(df_trade_json)
        # Quotes
        df_quote_csv = dfs_text[1]
        df_quote_json = dfs_json[1]
        df_quotes = df_quote_csv.union(df_quote_json)

        return df_trades,df_quotes


    def write_dfs():

        # Write DFs to Blob
        trades_and_quotes = TradesAndQuotes.combine_dfs()
        trades = trades_and_quotes[0]
        # trades = TradesAndQuotes.combine_dfs()[0]
        trades.coalesce(1).write.parquet("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/combined_trade_and_quote/event_type=T")
        quotes = trades_and_quotes[1]
        # quotes = TradesAndQuotes.combine_dfs()[1]
        quotes.coalesce(1).write.parquet("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/combined_trade_and_quote/event_type=Q")


TradesAndQuotes.write_dfs()

