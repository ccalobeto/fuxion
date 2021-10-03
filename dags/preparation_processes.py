from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime

# STEPS For Production Environment:
# 1. Move the slq files to SQL_PROD_PATH
# 2. Change the DB_Target in sqls
# 3. Uncomment SQL_PROD_PATH and comment SQL_DEV_PATH

SQL_PROD_PATH = '/Users/carlosalbertoleonliza/projects/fuxion/airflow-developer/include/'
#SQL_PROD_PATH = '/usr/local/airflow/include/'

dag4 = DAG(
    dag_id="week_aggregations",
    start_date=datetime(2021,9,20,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

wait_for_dag1 = ExternalTaskSensor(
    task_id='wait_for_dag1',
    external_dag_id='orders_etl',
    external_task_id='load_semana_primera_compra',
    dag=dag4
)

wait_for_dag2 = ExternalTaskSensor(
    task_id='wait_for_dag2',
    external_dag_id='week_aggregation_money_currency',
    external_task_id='week_aggregation_tipo_cambio',
    dag=dag4
)

wait_for_dag3 = ExternalTaskSensor(
    task_id='wait_for_dag3',
    external_dag_id='customers_etl',
    external_task_id='load_consumidores',
    dag=dag4
)

load_week_aggregations_actividad_ventas = MsSqlOperator(
    task_id='load_week_aggregations_actividad_ventas',
    sql='aggw_query_actividad_ventas.sql',
    mssql_conn_id='mssql_analytics',
    dag=dag4
)


[wait_for_dag1, wait_for_dag2, wait_for_dag3] >> load_week_aggregations_actividad_ventas