# -*- coding: utf-8 -*-
# Copyright 2020 Aneior Studio, SL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from airflow import settings
from airflow.models import Connection

from tests import TestBase


class OperatorTestBaseLocal(TestBase):
    """Operator Test Base"""

    conn_id = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        conn = Connection(conn_id='pdi_default')

        extra = """
        {
            "rep": "Default",
            "pentaho_home": "/opt/pentaho",
        }
        """

        session = settings.Session()

        try:
            if not conn.login:
                conn = Connection(
                    conn_type='pentaho',
                    conn_id='pdi_default',
                    host='localhost',
                    port=8880,
                    login='airflow',
                    password='airflow',
                    extra=extra
                )
                session.add(conn)
                session.commit()
        except Exception as ex:  # pylint: disable=broad-except
            print(ex)
            session.rollback()
