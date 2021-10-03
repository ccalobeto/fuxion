from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from operators.fuxion_mssql_operator import MsSqlTransferOperator
from operators.fuxion_currencylayer_operator import FuxionApiToMsSqlOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime

SQL_PROD_PATH = '/Users/carlosalbertoleonliza/airflow/include'
#SQL_PROD_PATH = '/usr/local/airflow/include/'

dag0 = DAG(
    dag_id="currency_etl",
    start_date=datetime(2021,9,20,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

dag1 = DAG(
    dag_id="orders_etl",
    start_date=datetime(2021,9,20,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

dag2 = DAG(
    dag_id="week_aggregation_money_currency",
    start_date=datetime(2021,9,20,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:10 every day (UTC time), Runs at 9:00 AM in Lima
)

dag3 = DAG(
    dag_id="customers_etl",
    start_date=datetime(2021,9,20,0,0,0),
    template_searchpath=SQL_PROD_PATH,
    schedule_interval='0 14 * * *' # Executes at 14:00 every day (UTC time), Runs at 9:00 AM in Lima
)

load_tipo_cambio = FuxionApiToMsSqlOperator(
    task_id='load_tipo_cambio',
    currencylayer_conn_id='currencylayer',
    base_uri="{schema}://{host}/{endpoint}?access_key={your_api_access_key}&currencies={currency_list}&date={execution_date}",
    mssql_conn_id='mssql_analytics',
    table_target='etl_tipo_cambio',
    execution_date='{{ ds }}',
    dag=dag0
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

wait_for_currencies = ExternalTaskSensor(
    task_id='wait_for_currencies',
    external_dag_id='currency_etl',
    external_task_id='load_tipo_cambio',
    dag=dag2
)

week_aggregation_tipo_cambio = MsSqlOperator(
    task_id='week_aggregation_tipo_cambio',
    sql='aggw_query_tipo_cambio.sql',
    mssql_conn_id='mssql_analytics',
    dag=dag2
)

load_consumidores = MsSqlTransferOperator(
    task_id='load_consumidores',
    sql='etl_query_consumidores.sql',
    mssql_conn_source='mssql_fuxionreporting',
    mssql_conn_target='mssql_analytics',
    table_target='etl_consumidores',
    dag=dag3
)

load_tipo_cambio
load_ordenes >> load_semana_primera_compra
wait_for_currencies>>week_aggregation_tipo_cambio
load_consumidores



