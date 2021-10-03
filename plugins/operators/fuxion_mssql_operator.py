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

import json
import numpy as np
import pandas as pd
import datetime
import math

from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks.astro_mssql_hook import AstroMsSqlHook


class MsSqlTransferOperator(BaseOperator):
    """
    Transfer data between different MSSql servers
    :param sql: type str. Reference to the sql query
    :param mssql_conn_source: type str. Reference to the source connection in UI airflow
    :param mssql_conn_target: type str. Reference to the target connection in UI airflow
    :param table_target: type str. Reference the target table
    :param package_schema: type bool. Reference to TRUE to query the columns of the target table
    :param force_integer_columns: type list. name the column to be force to integer cause null values
    :param parameter: type Optional. Reference to parameter the query
    :param autocommit: type bool. Reference to TRUE
    """
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        sql: str,
        mssql_conn_source: str,
        mssql_conn_target: str,
        table_target: str,
        package_schema: bool = True,
        force_integer_columns : list = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        autocommit: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.mssql_conn_source = mssql_conn_source
        self.mssql_conn_target = mssql_conn_target
        self.table_target = table_target
        self.force_integer_columns = force_integer_columns
        self.package_schema = package_schema
        self.parameters = parameters
        self.autocommit = autocommit

    def execute(self, context):
        hook_source = AstroMsSqlHook(self.mssql_conn_source)
        hook_target = AstroMsSqlHook(self.mssql_conn_target)
        self.log.info('Fetch the records from source table')
        tuples = self.get_records_all(hook_source, self.sql)
        columns = None
        if self.package_schema:
            columns = self.get_column_names(hook_target, self.table_target)
        self.log.info('Inserting rows into target table')
        #self.log.info('tuplas: %s', tuples)
        hook_target.insert_rows(self.table_target, tuples, columns)

    def get_column_names(self, hook, table):
        """ return the columns names from schema"""
        results = list(hook.get_schema(table))
        columns = []
        for i in results:
            columns.append(i['COLUMN_NAME'])
        return columns

    def get_records_all(self, hook, query):
        # Perform query and returned a list of tuples.
        # Some columns with null values are forced to be integer. NA and nan values in tuples are converted to None to not
        # have problems in the target table
        df = hook.get_pandas_df(query)
        if self.force_integer_columns:
            for column in self.force_integer_columns:
                df[column] = df[column].astype('Int64')
        datetime_columns = df.select_dtypes(include=[np.datetime64]).columns.to_list()
        #self.log.info('las columnas datetime son: %s', datetime_columns)
        for column in datetime_columns:
            df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
        #self.log.info(df.dtypes)
        #self.log.info(df.isnull().sum())
        tuplas = [tuple(x) for x in df.values.tolist()]
        tuplas = [tuple(None if ((isinstance(i, float) and math.isnan(i)) or i is pd.NA) else i for i in t) for t in tuplas]
        self.log.info('Successfully performed query.')
        return tuplas


