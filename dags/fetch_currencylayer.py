from datetime import datetime

from airflow import DAG
from operators.fuxion_currencylayer_operator import FuxionApiToMsSqlOperator

SQL_PROD_PATH = '/Users/carlosalbertoleonliza/airflow/include'
#SQL_PROD_PATH = '/usr/local/airflow/include/'

dag0 = DAG(
    dag_id="currency_etl",
    start_date=datetime(2021,9,27,0,0,0),
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
