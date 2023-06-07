# Robert Jones
# 2.3.23
# Guided Capstone for SpringBoard
# ...take temp data from spark_parse_from_blob.py and
# ...transform
# ...load back into blob

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
azure_key = open('azure_key.txt').read()

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

spark = SparkSession.builder.master('local').appName('app').config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1').getOrCreate()
spark.conf.set("fs.azure.account.key.springboardstorage.blob.core.windows.net",azure_key)

class DataLoad:

    def read_temp():
        """Read output (trades and quotes DFs) from spark_parse_blob.py into trade and quote DFs """
        trade_common = spark.read.option('inferSchema','true').parquet('wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/combined_trade_and_quote/event_type=T')
        trade = trade_common.select('trade_dt','symbol','exchange','event_tm','event_seq_nb','file_tm','price')

        quote_common = spark.read.option('inferSchema','true').parquet('wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/combined_trade_and_quote/event_type=Q')
        quote = quote_common.select('trade_dt','symbol','exchange','event_tm','event_seq_nb','file_tm')

        return trade,quote

    def drop_dups_keep_latest():

        """Drop duplicates based on trade_dt','symbol','exchange','event_seq_nb and while keeping most recent arrival_tm."""

        # Read DataFrames (DataLoad.read_temp())
        quotes_and_trades = DataLoad.read_temp()
        quote = quotes_and_trades[1]
        quote.printSchema() # Show Schema
        trade = quotes_and_trades[0]
        trade.printSchema() # Show Schema

        # Create UniqueID (rank) on columns ('trade_dt','symbol','exchange','event_seq_nb')
        # ...ordering by DESC event_tm
        # ...drop duplicates (any rank over 1 is a duplicate)
        # ...print bad dates and drop rank column

        trade_window = Window.partitionBy('trade_dt','symbol','exchange','event_seq_nb').orderBy(F.desc('event_tm'))
        trade_corrections = trade.withColumn('rank',F.row_number().over(trade_window)).filter(col('rank') > 1)
        bad_trade_dates = trade_corrections.rdd.map(lambda x:x.trade_dt).collect()
        print(f'Trade dates with corrections {bad_trade_dates}')
        trade = trade.withColumn('rank',F.row_number().over(trade_window)).filter(col('rank') == 1).drop('rank')
        trade.show()


        quote_window = Window.partitionBy('trade_dt','symbol','exchange','event_seq_nb').orderBy(F.desc('event_tm'))
        quote_corrections = quote.withColumn('rank',F.row_number().over(quote_window)).filter(col('rank') > 1)
        bad_quote_dates = quote_corrections.rdd.map(lambda x:x.trade_dt).collect()
        print(f'Quote dates with corrections {bad_quote_dates}')
        quote = quote.withColumn('rank',F.row_number().over(quote_window)).filter(col('rank') == 1).drop('rank')
        quote.show()

        return trade,quote
    
    def write_to_blob():

        """Write de-duplicated data to Azure Blob"""

        trades_and_quotes = DataLoad.drop_dups_keep_latest()
        trade = trades_and_quotes[0]
        quote = trades_and_quotes[1]

        trade.coalesce(1).write.parquet("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/transformed/trades/")
        quote.coalesce(1).write.parquet("wasbs://springboardcontainer@springboardstorage.blob.core.windows.net/data/transformed/quotes/")


instance = DataLoad
instance.drop_dups_keep_latest()
# instance.write_to_blob()