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


from airflow import settings  # noqa: F401
from airflow import AirflowException

from airflow_pentaho.operators.CarteTransOperator import CarteTransOperator
from tests.OperatorTestBase import OperatorTestBase


class TestCarteTransOperator(OperatorTestBase):

    def test_execute(self):
        op = CarteTransOperator(
            task_id="test_carte_trans_operator",
            trans="/home/bi/test_trans",
            level="Debug")

        op.execute(context={})
        self.assertTrue(True)

    def test_execute_non_existent_job(self):
        op = CarteTransOperator(
            task_id="test_carte_trans_operator",
            trans="/home/bi/unknown_trans",
            level="Debug")

        with self.assertRaises(AirflowException) as context:
            op.execute(context={})

        print(context.exception)
        self.assertTrue("ERROR: Unable to find job [unknown_job]"
                        in str(context.exception))
