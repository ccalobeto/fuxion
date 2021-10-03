from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from operators.fuxion_mssql_operator import MsSqlTransferOperator

from datetime import datetime

#SQL_PROD_PATH = '/Users/carlosalbertoleonliza/airflow/include'
SQL_PROD_PATH = '/usr/local/airflow/include/'

dag1 = DAG(
    dag_id="orders_etl",
    start_date=datetime(2021,9,15,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

dag2 = DAG(
    dag_id="week_aggregation_money_currency_v2",
    start_date=datetime(2021,9,6,0,0,0),
    #end_date=datetime(2021,9,20,0,0,15),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='10 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

dag3 = DAG(
    dag_id="customers_etl",
    start_date=datetime(2021,9,15,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

dag4 = DAG(
    dag_id="Fetches_sales_customers_activities",
    start_date=datetime(2021, 9, 1, 0, 0, 0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='10 14 * * *'
)


load_ordenes = MsSqlTransferOperator(
    task_id='load_ordenes',
    sql='etl_query_ordenes.sql',
    mssql_conn_source='mssql_fuxionreporting',
    mssql_conn_target='mssql_analytics',
    table_target='etl_ordenes',
    force_integer_columns = ['ReturnOrderID'],
    dag=dag1
)

load_semana_primera_compra = MsSqlOperator(
    task_id='load_semana_primera_compra',
    sql='sop_query_semana_primera_compra.sql',
    mssql_conn_id='mssql_analytics',
    dag=dag1
)

load_consumidores = MsSqlTransferOperator(
    task_id='load_consumidores',
    sql='etl_query_consumidores.sql',
    mssql_conn_source='mssql_fuxionreporting',
    mssql_conn_target='mssql_analytics',
    table_target='etl_consumidores',
    dag=dag3
)


week_aggregation_tipo_cambio = MsSqlOperator(
    task_id='week_aggregation_tipo_cambio',
    sql='aggw_query_tipo_cambio.sql',
    mssql_conn_id='mssql_analytics',
    dag=dag2
)

week_aggregation_actividad_ventas = MsSqlOperator(
    task_id='week_aggregation_actividad_ventas',
    sql='aggw_query_tipo_cambio.sql',
    mssql_conn_id='mssql_analytics',
    dag=dag4

)

week_aggregation_tipo_cambio
load_consumidores
load_ordenes >> load_semana_primera_compra

#wait_for_sales_process >> week_aggregation_actividad_ventas
week_aggregation_actividad_ventas

