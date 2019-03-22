# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ctds

import logging
from airflow.hooks.dbapi_hook import DbApiHook

class CtdsHook(DbApiHook):
    '''
    Based off of Zillow's ctds library. Allows for bulk
    load operations against Microsoft databases.
    '''

    conn_name_attr = 'ctds_conn_id'
    default_conn_name = 'ctds_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a ctds connection object
        """
        conn = self.get_connection(self.ctds_conn_id)
        conn = ctds.connect( server=conn.host, user=conn.login, password=conn.password, database=conn.schema, port=conn.port)
        return conn

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = True

    def bulk_load(self, table, rows):
        conn = self.get_connection(self.ctds_conn_id)
        with ctds.connect( server=conn.host, user=conn.login, password=conn.password, database=conn.schema, port=conn.port) as c:
            if len(rows) > 0:
                for i in range( len( rows ) ):
                    for j in range( len( rows[i] ) ):
                        if type(rows[i][j]) == 'str':
                            rows[i][j] = ctds.SqlVarChar(rows[i][j].encode('latin-1'))
                c.bulk_insert(table, iter(rows))

