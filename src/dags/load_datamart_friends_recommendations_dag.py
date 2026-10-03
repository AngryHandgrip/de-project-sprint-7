import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 's19419897',
                                'start_date':datetime(2026, 1, 1),
                                }

dag = DAG(
            dag_id = "load_datamart_friends_recommendations_dag",
            default_args=default_args,
            schedule_interval='0 1 * * *',
            start_date=datetime(2026, 1, 1),
            catchup=False,
            )

load_friends_recommendations_datamart = SparkSubmitOperator(
                        task_id='load_friends_recommendations_datamart',
                        dag=dag,
                        application ='/lessons/dags/scripts/datamart_friends_recommendations.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [
                            '/user/master/data/geo/events/',
                            '/user/s19419897/data/geo_2.csv',
                            '/user/s19419897/analytics/datamart_friends_recommendations/'
                        ],
                        conf={
                            "spark.driver.maxResultSize": "20g",
                            'spark.executor.memory': '4g',
                            'spark.executor.cores': '2',
                            'spark.driver.memory': '2g',
                        },
                        verbose=True
                    )

load_friends_recommendations_datamart