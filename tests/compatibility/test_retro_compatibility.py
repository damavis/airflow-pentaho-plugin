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
"""Import exposed operators to ensure retro compatibility"""

from airflow_pentaho.operators.KitchenOperator import KitchenOperator
from airflow_pentaho.operators.PanOperator import PanOperator
from airflow_pentaho.operators.CarteJobOperator import CarteJobOperator
from airflow_pentaho.operators.CarteTransOperator import CarteTransOperator

from tests.operator_test_base import OperatorTestBase


class TestCompatibility(OperatorTestBase):

    def test_execute(self):
        op1 = KitchenOperator(task_id='test_kitchen_operator',
                              xcom_push=True,
                              directory='/home',
                              job='test_job',
                              params={'a': '1'})
        self.assertIsNotNone(op1)

        op2 = PanOperator(task_id='test_mocked_pan_operator',
                          xcom_push=True,
                          directory='/home',
                          trans='test_trans',
                          params={'a': '1'})
        self.assertIsNotNone(op2)

        op3 = CarteJobOperator(task_id='test_carte_job_operator',
                               job='/home/bi/test_job',
                               level='Debug')
        self.assertIsNotNone(op3)

        op4 = CarteTransOperator(task_id='test_carte_trans_operator',
                                 trans='/home/bi/test_trans',
                                 level='Debug')
        self.assertIsNotNone(op4)
