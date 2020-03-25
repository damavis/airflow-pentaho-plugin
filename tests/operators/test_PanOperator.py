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


import unittest

from airflow import settings
from airflow.models import Connection

from airflow_pentaho.operators.PanOperator import PanOperator
from tests.TestBase import TestBase


class TestPanOperator(TestBase):

    def setUp(self) -> None:
        super().setUp()
        conn = Connection(
            conn_id="pdi_default",
            login="admin",
            password="password",
            extra='{"rep": "Default", "pentaho_home": "/opt/pentaho"}'
        )
        session = settings.Session()
        session.add(conn)
        session.commit()

    def test_return_value(self):
        op = PanOperator(
            task_id="test_pan_operator",
            xcom_push=True,
            directory="/home",
            trans="test_trans",
            params={"a": "1"})

        return_value = op.execute(context={})
        self.assertTrue("ended successfully" in return_value)

    @unittest.expectedFailure  # Transformation XML is not valid error
    def test_return_value_file(self):
        op = PanOperator(
            task_id="test_pan_operator",
            xcom_push=True,
            file=self.TESTS_PATH + "/assets/test_trans.kjb",
            directory="/home",
            trans="test_trans",
            safemode=True,
            params={"a": "1"})

        return_value = op.execute(context={})
        self.assertTrue("ended successfully" in return_value)
