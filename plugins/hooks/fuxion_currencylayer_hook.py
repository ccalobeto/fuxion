from airflow.hooks.base_hook import BaseHook
import requests


class FuxionCurrencyLayerHook(BaseHook):

    def __init__(self, currencylayer_conn_id, base_uri, execution_date):
        """
        get the connection to the api and extract the parameters setted in Airflow UI
        """
        super().__init__()
        self.conn_id = currencylayer_conn_id
        self.connection = self.get_connection(self.conn_id)
        self.host = self.connection.host
        self.schema = self.connection.schema
        self.endpoint = self.connection.extra_dejson['endpoint']
        self.your_api_access_key = self.connection.extra_dejson['your_api_access_key']
        self.currency_list = self.connection.extra_dejson['currency_list']
        self.execution_date = execution_date

        self.base_uri = base_uri


    def get_response(self):
        """
        Get the api response.
        """
        url = self.base_uri.format(
            schema=self.schema,
            host=self.host,
            endpoint=self.endpoint,
            your_api_access_key=self.your_api_access_key,
            currency_list=self.currency_list,
            execution_date=self.execution_date)
        self.log.info('Url:%s',url)
        response = requests.get(url)
        return response.text