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


from unittest.mock import MagicMock

from airflow import settings  # noqa: F401

from airflow_pentaho.operators.PanOperator import PanOperator
from tests.OperatorTestBase import OperatorTestBase


class TestPanOperator(OperatorTestBase):

    def test_return_value(self):
        op = PanOperator(
            task_id="test_mocked_pan_operator",
            xcom_push=True,
            directory="/home",
            trans="test_trans",
            params={"a": "1"})

        mocked_cli = MagicMock()
        mocked_cli.build_command.return_value = \
            """echo This is a mocked result"""
        op._get_pentaho_client = MagicMock(return_value=mocked_cli)

        return_value = op.execute(context={})
        self.assertTrue("This is a mocked result" in return_value)
