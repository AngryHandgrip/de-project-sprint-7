from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import numpy as np
import sys
from geo import get_geo


def main() -> None:
    base_input_path = sys.argv[1]
    geo_path = sys.argv[2]
    base_output_path = sys.argv[3]
    
    spark = (
        SparkSession
        .builder
        .master('yarn')
        .appName('datamart_friends_recommendations')
        .getOrCreate()
    )

    R = 6371

    events = spark.read.parquet(base_input_path)

    messages = (
        events
        .filter(F.col('event_type') == 'message')
        .select(
            F.col('event.message_from').alias('message_from'),
            F.col('event.message_to').alias('message_to'),
            'lat',
            'lon'
        )
    )

    subscriptions = (
        events
        .filter(F.col('event_type') == 'subscription')
        .select(
            F.col('event.subscription_channel').alias('subscription_channel'),
            F.col('event.user').alias('user')
        )
        .distinct()
    )
    
    geo = get_geo(spark, geo_path)

    s1 = subscriptions.alias('s1')
    s2 = subscriptions.alias('s2')

    candidate_pairs = (
        s1
        .join(
            s2,
            (F.col('s1.subscription_channel') == F.col('s2.subscription_channel')) &
            (F.col('s1.user') < F.col('s2.user')),
            'inner'
        )
        .select(
            F.col('s1.user').alias('user_left'),
            F.col('s2.user').alias('user_right')
        )
        .distinct()
    )

    messages = (
        events
        .filter(F.col('event_type') == 'message')
        .select(
            F.col('event.message_from').alias('user1'),
            F.col('event.message_to').alias('user2')
        )
        .filter(
            F.col('user1').isNotNull() &
            F.col('user2').isNotNull() &
            (F.col('user1') != F.col('user2'))
        )
        .select(
            F.least('user1', 'user2').alias('user_left'),
            F.greatest('user1', 'user2').alias('user_right')
        )
        .distinct()
    )

    candidate_pairs = (
        candidate_pairs
        .join(
            messages,
            ['user_left', 'user_right'],
            'leftanti'
        )
    )

    user_last_geo = (
        events
        .filter(F.col('event_type') == 'message')
        .select(
            F.col('event.message_from').alias('user'),
            F.col('event.message_ts').alias('message_ts'),
            'lat',
            'lon'
        )
        .withColumn(
            'enum_message_ts',
            F.row_number().over(
                Window
                .partitionBy('user')
                .orderBy(F.desc('message_ts'))
            )
        )
        .filter(F.col('enum_message_ts') == 1)
        .select('user', 'lat', 'lon')
    )

    left_geo = user_last_geo.alias('left_geo')
    right_geo = user_last_geo.alias('right_geo')

    result = (
        candidate_pairs
        .join(
            left_geo,
            F.col('user_left') == F.col('left_geo.user'),
            'inner'
        )
        .join(
            right_geo,
            F.col('user_right') == F.col('right_geo.user'),
            'inner'
        )
        .withColumn(
            'distance',
            2 * R * F.asin(F.sqrt(F.pow(F.sin((F.col('left_geo.lat') - F.col('right_geo.lat')) * np.pi/360), 2) + F.cos(F.col('left_geo.lat') * np.pi/180) * F.cos(F.col('right_geo.lat') * np.pi/180) * F.pow(F.sin((F.col('left_geo.lon') - F.col('right_geo.lon')) * np.pi/360), 2)))
        )
        .filter(F.col('distance') <= 1)
        .withColumn('avg_lat', (F.col('left_geo.lat') + F.col('right_geo.lat')) / 2)
        .withColumn('avg_lon', (F.col('left_geo.lon') + F.col('right_geo.lon')) / 2)
        .select('user_left', 'user_right', 'avg_lat', 'avg_lon')
        .crossJoin(geo)
        .withColumn('distance', 2 * R * F.asin(F.sqrt(F.pow(F.sin((F.col('geo_lat') - F.col('avg_lat')) * np.pi/360), 2) + F.cos(F.col('geo_lat') * np.pi/180) * F.cos(F.col('avg_lat') * np.pi/180) * F.pow(F.sin((F.col('geo_lon') - F.col('avg_lon')) * np.pi/360), 2))))
        .withColumn('enum_distances', F.row_number().over(Window.partitionBy(F.col('user_left'), F.col('user_right')).orderBy('distance')))
        .filter(F.col('enum_distances') == 1)
        .withColumn('processed_dttm', F.current_timestamp())
        .withColumn("local_time", F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone")))
        .select('user_left', 'user_right', 'processed_dttm', F.col('id').alias('zone_id'), 'local_time')
    )
    
    result.write.mode('overwrite').parquet(base_output_path)
    
    
if __name__ == '__main__':
    main()