from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import numpy as np
import sys
from geo import get_geo


def main():
    base_input_path = sys.argv[1]
    geo_path = sys.argv[2]
    base_output_path = sys.argv[3]
    
    spark = (
        SparkSession
        .builder
        .master('yarn')
        .appName('datamart_users')
        .getOrCreate()
    )

    R = 6371

    events = (
        spark.read.parquet(base_input_path)
        .filter(F.col('event_type') == 'message')
        .select(F.col('event.message_from').alias('message_from'), F.col('event.message_id').alias('message_id'), F.coalesce('event.datetime', 'event.message_ts').alias('message_ts'), F.col('lat'), F.col('lon'))
    )

    geo = get_geo(spark, geo_path)
    
    events_with_geo = events.crossJoin(geo)

    min_distance_to_event = (
        events_with_geo
        .withColumn(
            'distance',
            2 * R * F.asin(F.sqrt(F.pow(F.sin((F.col('geo_lat') - F.col('lat')) * np.pi/360), 2) + F.cos(F.col('geo_lat') * np.pi/180) * F.cos(F.col('lat') * np.pi/180) * F.pow(F.sin((F.col('geo_lon') - F.col('lon')) * np.pi/360), 2)))
        )
        .withColumn(
            'enum_distances',
            F.row_number().over(Window.partitionBy(F.col('message_id')).orderBy('distance'))
        )
        .filter(F.col('enum_distances') == 1)
        .drop('lat','lon','id','geo_lat', 'geo_lon', 'enum_distances')
    )

    home_cities = (
        min_distance_to_event
        .withColumn(
            'last_city',
            F.lag('city').over(Window.partitionBy('message_from').orderBy('message_ts'))
        )
        .withColumn(
            'is_city_changed', 
            F.when(F.col('city') != F.coalesce(F.col('last_city'), F.col('city')), 1)
            .otherwise(0)
       )
        .withColumn(
            'city_ts_group',
            F.sum(F.col('is_city_changed')).over(Window.partitionBy(F.col('message_from')).orderBy(F.col('message_ts')).rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )
        .groupBy('message_from', 'city', 'city_ts_group', 'timezone')
        .agg(
            F.max(F.col('message_ts')).alias('max_group_ts'),
            F.min(F.col('message_ts')).alias('min_group_ts')
        )
        .withColumn(
            'days_spent_in_city',
            F.datediff(F.col('max_group_ts'), F.col('min_group_ts'))
        )
        .filter(F.col('days_spent_in_city') >= 27)
        .withColumn(
            'enum_home_cities',
            F.row_number().over(Window.partitionBy('message_from').orderBy(F.desc('min_group_ts')))
        )
        .filter(F.col('enum_home_cities') == 1)
        .select('message_from', F.col('city').alias('home_city'))
    )
    
    home_and_act_cities = (
        min_distance_to_event
        .withColumn(
            'enum_messages',
            F.row_number().over(Window.partitionBy('message_from').orderBy(F.desc(F.col('message_ts'))))
        )
        .filter(F.col('enum_messages') == 1)
        .join(home_cities, 'message_from', 'left')
        .withColumn(
            'local_time',
            F.from_utc_timestamp(F.col('message_ts'), F.col('timezone'))
        )
        .select(F.col('message_from').alias('user_id'), F.col('city').alias('act_city'), 'home_city', 'local_time')
    )
    
    grouped_message_locations = (
        min_distance_to_event
        .withColumn(
            'last_city',
            F.lag('city').over(Window.partitionBy('message_from').orderBy('message_ts'))
        )
        .withColumn('is_city_changed', 
            F.when(F.col('city') != F.coalesce(F.col('last_city'), F.col('city')), 1)
            .otherwise(0)
       )
        .withColumn('city_ts_group', F.sum(F.col('is_city_changed')).over(Window.partitionBy(F.col('message_from')).orderBy(F.col('message_ts')).rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .groupBy(F.col('message_from').alias('user_id'), 'city', 'city_ts_group')
        .agg(F.max(F.col('message_ts')).alias('last_group_ts'))
        .groupBy('user_id')
        .agg(
            F.count('*').alias('travel_count'),
            F.sort_array(F.collect_list(F.struct('city_ts_group', 'city'))
            ).alias('travel_array_struct')
        )
        .withColumn(
            'travel_array',
            F.expr('transform(travel_array_struct, x -> x.city)')
        )
        .drop('travel_array_struct')
    )
    result = (
        home_and_act_cities
        .select('user_id', 'act_city','home_city', 'local_time')
        .join(grouped_message_locations, 'user_id', 'left')
        .select('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
    )
    result.write.mode('overwrite').parquet(base_output_path)
    
    
if __name__ == '__main__':
    main()