from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from typing import List


def get_df_timestamps(raw_df: DataFrame, symbol: str) -> (DataFrame, List):
    # From a raw_df with may symbols, get a DataFrame for a specific symbol and a list of all the timestamps in ascending order 
    raw_df = raw_df.filter(raw_df["symbol"] == symbol)
    raw_df = raw_df.orderBy('time')
    timestamps = raw_df.select(raw_df['time']).collect()

    return raw_df, timestamps


def gen_empty_df_row(schema: StructType, spark: SparkSession) -> (DataFrame, Row):
    # Create an empty DataFrame with the designated schema and a Row class with the same schema
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)
    CryptoRow = Row(*df.columns)
    return df, CryptoRow


def transform_as_df(schema: StructType, symbol: str, spark: SparkSession, raw_df: DataFrame, sacrified_rows: int, past_delta_match: dict) -> List[Row]:
    symbol_df, timestamps = get_df_timestamps(raw_df, symbol)
    train_df, CryptoRow = gen_empty_df_row(schema=schema, spark=spark)
    list_of_rows = []
    price_df = symbol_df.select(['time', 'open', 'close', 'high', 'low'])
    price_df.cache()
    volume_df = symbol_df.select(['time', 'volumefrom', 'volumeto'])
    volume_df.cache()

    
    for i in range(sacrified_rows, len(timestamps)):       
        self_now = timestamps[i][0]
        # build each CryptoRow
        crypto_row = [symbol, self_now]
        for header, hours_diff in past_delta_match['price'].items():
            seek_time = self_now + timedelta(hours=hours_diff)
            price = price_df.filter(price_df['time'] == seek_time).select('open').collect()[0][0]
            crypto_row.append(price)  
        for header, hours_dif in past_delta_match['volume'].items():
            seek_time = self_now + timedelta(hours=hours_dif)
            volume = volume_df.filter(volume_df['time'] == seek_time).select('volumeto').collect()[0][0]
            crypto_row.append(volume)
        list_of_rows.append(CryptoRow(*crypto_row))
        print(CryptoRow(*crypto_row))
    
    return list_of_rows