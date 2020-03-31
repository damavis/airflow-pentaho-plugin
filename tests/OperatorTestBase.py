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

from tests.TestBase import TestBase


class OperatorTestBase(TestBase):

    conn_id = None

    def setUp(self) -> None:
        super().setUp()
        conn = Connection(conn_id="pdi_default")

        extra = """
        {
            "rep": "Default",
            "pentaho_home": "/opt/pentaho",
            "carte_username": "cluster",
            "carte_password": "cluster"
        }
        """

        if not conn:
            conn = Connection(
                conn_id="pdi_default",
                host="localhost",
                port="8880",
                login="admin",
                password="password",
                extra=extra
            )
            session = settings.Session()
            session.add(conn)
            session.commit()
