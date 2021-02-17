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


from unittest import mock
from airflow import settings  # noqa: F401

from airflow_pentaho.operators.carte import CarteTransOperator
from tests.operator_test_base import OperatorTestBase


class MockedCarteResponse:

    def __init__(self, response_body, status_code):
        self.content = response_body
        self.status_code = status_code


def mock_requests(**kwargs):
    if 'executeTrans' in kwargs['url']:
        return MockedCarteResponse('', 200)

    if 'transStatus' in kwargs['url']:
        return MockedCarteResponse("""
        <transstatus>
            <transname>dummy-trans</transname>
            <id>c56961b2-c848-49b8-abde-76c8015e29b0</id>
            <status_desc>Finished</status_desc>
            <error_desc/>
            <paused>N</paused>
            <stepstatuslist>
                <stepstatus><stepname>Dummy (do nothing)</stepname>
                <copy>0</copy><linesread>0</linesread>
                <lineswritten>0</lineswritten><linesinput>0</linesinput>
                <linesoutput>0</linesoutput><linesupdated>0</linesupdated>
                <linesrejected>0</linesrejected><errors>0</errors>
                <statusdescription>Stopped</statusdescription><seconds>0.0</seconds>
                <speed>-</speed><priority>-</priority><stopped>Y</stopped>
                <paused>N</paused>
                </stepstatus>
            </stepstatuslist>
            <first_log_line_nr>0</first_log_line_nr>
            <last_log_line_nr>37</last_log_line_nr>
            <result>
                <lines_input>0</lines_input>
                <lines_output>0</lines_output>
                <lines_read>0</lines_read>
                <lines_written>0</lines_written>
                <lines_updated>0</lines_updated>
                <lines_rejected>0</lines_rejected>
                <lines_deleted>0</lines_deleted>
                <nr_errors>0</nr_errors>
                <nr_files_retrieved>0</nr_files_retrieved>
                <entry_nr>0</entry_nr>
                <result>Y</result>
                <exit_status>0</exit_status>
                <is_stopped>Y</is_stopped>
                <log_channel_id>10e2c832-07da-409a-a5ba-4b90a234e957</log_channel_id>
                <log_text/>
                <result-file/>
                <result-rows/>
            </result>
            <logging_string><![CDATA[H4sIAAAAAAAAADMyMDTRNzTUNzJRMDSyMrC0MjFV0FVIKc3NrdQtKUrMKwbyXDKLCxJLkjMy89IViksSi0pSUxTS8osUwPJARm5iSWZ+nkI0kq5YXi4AQVH5bFoAAAA=]]></logging_string>
         </transstatus>
        """, 200)


class TestCarteTransOperator(OperatorTestBase):

    @mock.patch('requests.post', side_effect=mock_requests)
    @mock.patch('requests.get', side_effect=mock_requests)
    def test_execute(self, mock_get, mock_post):
        op = CarteTransOperator(
            task_id='test_carte_trans_operator',
            trans='/home/bi/test_trans',
            level='Debug')

        op.execute(context={})

        self.assertEqual('name=test_trans&xml=Y&from=0',
                         mock_post.call_args_list[0][1]['data'])
