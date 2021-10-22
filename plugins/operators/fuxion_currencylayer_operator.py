#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pandas as pd
from datetime import datetime


from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.fuxion_currencylayer_hook import FuxionCurrencyLayerHook
from hooks.astro_mssql_hook import AstroMsSqlHook


class FuxionApiToMsSqlOperator(BaseOperator):
    """
    load data from api to MSSql server
    :param currencylayer_conn_id: type str. api connection in Airflow UI
    :param base_uri: type str. Reference to the source connection in UI airflow
    :param mssql_conn_id: type str. Reference to Mssql connection id define in Airflow UI
    :param target_table: type str. Reference to a specific table in mssql database
    :param parameter: type Optional. Reference to parameter the query
    :param autocommit: type bool. Reference to TRUE
    """
    template_fields = ('execution_date',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        currencylayer_conn_id: str,
        base_uri: str,
        mssql_conn_id: str,
        table_target : str,
        execution_date : str,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        autocommit: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.currencylayer_conn_id = currencylayer_conn_id
        self.base_uri = base_uri
        self.mssql_conn_id = mssql_conn_id
        self.table_target = table_target
        self.execution_date = execution_date
        self.parameters = parameters
        self.autocommit = autocommit

    def execute(self, context):
        hook_api = FuxionCurrencyLayerHook(self.currencylayer_conn_id, self.base_uri, self.execution_date)
        self.log.info('Fetch the records from CurrencyLayer')
        response = hook_api.get_response()
        self.log.info('Api Response: %s', response)

        hook_target = AstroMsSqlHook(self.mssql_conn_id)
        self.log.info('Construct the tuples from api')
        tuples = self.get_records_all(response)
        columns = self.get_column_names(hook_target, self.table_target)
        self.log.info('Inserting tuples into target table')
        hook_target.insert_rows(self.table_target, tuples, columns)

    def get_column_names(self, hook, table):
        """ return the columns names from schema"""
        results = list(hook.get_schema(table))
        columns = []
        for i in results:
            columns.append(i['COLUMN_NAME'])
        return columns

    def get_records_all(self, response):
        # Convert json response into dataframe for manipulation. Custom manipulation of some currencies, return tuplas

        # read into a dataframe
        df_ = pd.read_json(response)
        df = df_.reset_index()[['date', 'index', 'quotes']]

        # clean data
        df.rename(columns={'date': 'FechaCreacion', 'index': 'CodigoMoneda', 'quotes': 'TipoDeCambio'}, inplace=True)
        df['CodigoMoneda'] = df['CodigoMoneda'].str[-3:]
        df['CodigoMoneda'] = df['CodigoMoneda'].str.lower()
        df['FechaProcesoETL'] = datetime.utcnow()
        # currency from El Salvador is USD: change the value and the currency name collected from api
        index = df.index
        df.loc[index[df['CodigoMoneda'] == 'svc'][0], 'TipoDeCambio'] = df.loc[
            index[df['CodigoMoneda'] == 'usd'][0], 'TipoDeCambio']
        df.replace({'svc': 'svd'}, inplace=True)

        # insert two more currencies: Ecuador and Franco Aleman
        df_new = df[df['CodigoMoneda'].isin(['usd', 'eur'])]
        df_new.replace({'usd': 'ecd', 'eur': 'eug'}, inplace=True)
        df = df.append(df_new).reset_index(drop=True)

        # Format the python datetime dtype to MSSQL datetime type
        df['FechaCreacion'] = df['FechaCreacion'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['FechaProcesoETL'] = df['FechaProcesoETL'].dt.strftime('%Y-%m-%d %H:%M:%S')

        tuplas = [tuple(x) for x in df.values.tolist()]
        return tuplas