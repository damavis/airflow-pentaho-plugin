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

from airflow import settings  # noqa: F401

from airflow_pentaho.operators.kettle import PanOperator
from tests.operator_test_base import OperatorTestBase


class TestPanOperator(OperatorTestBase):

    def test_return_value(self):
        op = PanOperator(
            task_id='test_pan_operator',
            xcom_push=True,
            directory='/home/test',
            trans='test_trans',
            params={'a': '1'})

        return_value = op.execute(context={})
        self.assertTrue('ended successfully' in return_value)

    @unittest.expectedFailure  # Transformation XML is not valid, error
    def test_return_value_file(self):
        op = PanOperator(
            task_id='test_pan_operator',
            xcom_push=True,
            file=self.TESTS_PATH + '/assets/test_trans.kjb',
            trans='test_trans',
            safemode=True,
            params={'a': '1'})

        return_value = op.execute(context={})
        self.assertTrue('ended successfully' in return_value)
