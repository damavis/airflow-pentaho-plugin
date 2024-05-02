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


from airflow_pentaho.operators.kettle import KitchenLocalOperator
from tests.operator_test_base_local import OperatorTestBaseLocal


class TestKitchenOperator(OperatorTestBaseLocal):
    """Test Kitchen Operator"""

    def test_return_value(self):
        op= KitchenLocalOperator(
            task_id='test_kitchen_operator',
            xcom_push=True,
            directory='/Users/rvalenzuela/Documents/Proyectos/airflow-pentaho-plugin-master/airflow-pentaho-plugin/tests/assets/',
            job='test_job',
            params={'a': '1'}
        )
        return_value = op.execute(context={})
        self.assertTrue('Kitchen - Processing ended' in return_value)
        # op = KitchenLocalOperator(
        #     task_id='test_kitchen_operator',
        #     xcom_push=True,
        #     directory='/home/test',
        #     job='test_job',
        #     params={'a': '1'})
        #
        # return_value = op.execute(context={})
        # self.assertTrue('Kitchen - Processing ended' in return_value)

    # def test_return_value_file(self):
    #     op = KitchenLocalOperator(
    #         task_id='test_kitchen_operator',
    #         xcom_push=True,
    #         file=self.TESTS_PATH + '/assets/test_job.kjb',
    #         job='test_job',
    #         params={'a': '1'})
    #
    #     return_value = op.execute(context={})
    #     self.assertTrue('Kitchen - Processing ended' in return_value)
