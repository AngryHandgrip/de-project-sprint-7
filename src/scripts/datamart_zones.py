from pyspark.sql import SparkSession, Window, DataFrame
import pyspark.sql.functions as F
import numpy as np
import sys
from typing import List
from geo import get_geo


def get_agg_data(event: DataFrame, geo: DataFrame, agg_name: str, event_ts:str, event_keys:List[str]) -> DataFrame:
    R = 6371
    event_keys = list(map(F.col, event_keys))
    agg_event = (
        event
        .withColumn('month', F.date_format(F.col(event_ts), 'MMMM'))
        .withColumn(
            'week',
            F.ceil((F.dayofmonth(event_ts) + (F.dayofweek(F.trunc(event_ts, "MM")) + 5) % 7) / 7).cast("int")
        )
        .crossJoin(geo)
        .withColumn(
            'distance',
            2 * R * F.asin(F.sqrt(F.pow(F.sin((F.col('geo_lat') - F.col('lat')) * np.pi/360), 2) + F.cos(F.col('geo_lat') * np.pi/180) * F.cos(F.col('lat') * np.pi/180) * F.pow(F.sin((F.col('geo_lon') - F.col('lon')) * np.pi/360), 2)))
        )
        .withColumn(
            'enum_distances',
            F.row_number().over(Window.partitionBy(event_keys).orderBy('distance'))
        )
        .filter(F.col('enum_distances') == 1)
        .select('month', 'week', F.col('id').alias('zone_id'))
        .withColumn(
            f'week_{agg_name}',
            F.count('*').over(Window.partitionBy('month', 'week', 'zone_id'))
        )
        .withColumn(
            f'month_{agg_name}',
            F.count('*').over(Window.partitionBy('month', 'zone_id'))
        )
        .distinct()
    )
    return agg_event

def main() -> None:
    base_input_path = sys.argv[1]
    geo_path = sys.argv[2]
    base_output_path = sys.argv[3]

    spark = (
        SparkSession
        .builder
        .master('yarn')
        .appName('datamart_zones')
        .getOrCreate()
    )

    events = spark.read.parquet(base_input_path)
    messages = (
        events
        .select(F.col('event.message_from').alias('message_from'), F.col('event.message_ts').alias('message_ts'), F.col('event.message_id').alias('message_id'), 'lat', 'lon')
        .filter(F.col('event_type') == 'message')
        .dropna()
    )
    reactions = (
        events
        .select(F.col('event.reaction_from').alias('reaction_from'), F.col('event.message_id').alias('reaction_message_id'), F.col('event.datetime').alias('reaction_ts'))
        .filter(F.col('event_type') == 'reaction')
        .dropna()    
    )
    subscriptions = (
        events
        .select(F.col('event.user').alias('subscription_from'), F.col('event.subscription_channel').alias('subscription_channel'), F.col('event.datetime').alias('subscription_ts'))
        .filter(F.col('event_type') == 'subscription')
        .dropna()
    )

    geo = get_geo(spark, geo_path)

    min_date, max_date = events.select(
        F.min("date"), 
        F.max("date")
    ).first()

    all_agg_keys = (
        spark.range(1)
        .select(
            F.explode(F.sequence(
                F.to_date(F.lit(min_date)), 
                F.to_date(F.lit(max_date))
            )).alias("date")
        )
        .withColumn('month', F.date_format(F.col('date'), 'MMMM'))
        .withColumn('week', F.ceil((F.dayofmonth("date") + (F.dayofweek(F.trunc("date", "MM")) + 5) % 7) / 7).cast("int"))
        .drop('date')
        .distinct()
        .crossJoin(geo)
        .select('month', 'week', F.col('id').alias('zone_id'))
    )
    
    last_messages = (
        messages
        .withColumn(
            'enum_messages',
            F.row_number().over(Window.partitionBy('message_from').orderBy(F.desc('message_ts')))
        )
        .filter(F.col('enum_messages') == 1)
        .select('message_from', F.col('lat').alias('last_lat'), F.col('lon').alias('last_lon'))
    )
    
    first_messages = (
        messages
        .withColumn(
            'enum_messages',
            F.row_number().over(Window.partitionBy('message_from').orderBy('message_ts'))
        )
        .filter(F.col('enum_messages') == 1)
        .drop('enum_messages')
    )
    
    first_messages = (
        first_messages
        .join(last_messages, 'message_from')
        .drop('lat', 'lon')
        .withColumnRenamed('last_lat', 'lat')
        .withColumnRenamed('last_lon', 'lon')
    )

    agg_first_messages = get_agg_data(first_messages, geo, 'user', 'message_ts', ['message_id'])

    agg_messages = get_agg_data(messages, geo, 'message', 'message_ts', ['message_id'])

    agg_reactions = (
        messages
        .withColumn(
            'enum_messages',
            F.row_number().over(Window.partitionBy('message_from').orderBy(F.desc(F.col('message_ts'))))
        )
        .filter(F.col('enum_messages') == 1)
        .join(reactions, messages.message_from == reactions.reaction_from, 'inner')
    )
    agg_reactions = get_agg_data(agg_reactions, geo, 'reaction', 'reaction_ts', ['reaction_message_id', 'reaction_from'])

    agg_subscriptions = (
        messages
        .withColumn(
            'enum_messages',
            F.row_number().over(Window.partitionBy('message_from').orderBy(F.desc(F.col('message_ts'))))
        )
        .filter(F.col('enum_messages') == 1)
        .join(subscriptions, messages.message_from == subscriptions.subscription_from, 'inner')
    )
    agg_subscriptions = get_agg_data(agg_subscriptions, geo, 'subscription', 'subscription_ts', ['subscription_channel', 'subscription_from'])

    result = (
        all_agg_keys
        .join(agg_first_messages, ['month', 'week', 'zone_id'], 'left')
        .join(agg_messages, ['month', 'week', 'zone_id'], 'left')
        .join(agg_reactions, ['month', 'week', 'zone_id'], 'left')
        .join(agg_subscriptions, ['month', 'week', 'zone_id'], 'left')
        .fillna(0)
        .select('month', 'week','zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user', 'month_message', 'month_reaction', 'month_subscription', 'month_user')
    )
    result.write.mode('overwrite').parquet(base_output_path)
    
    
if __name__ == '__main__':
    main()