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

from airflow_pentaho.operators.carte import CarteJobOperator
from tests.operator_test_base import OperatorTestBase


class MockedCarteResponse:

    def __init__(self, response_body, status_code):
        self.content = response_body
        self.status_code = status_code


def mock_requests(**kwargs):
    if 'executeJob' in kwargs['url']:
        return MockedCarteResponse("""
        <webresult>
            <result>OK</result>
            <message>Job started</message>
            <id>f8110ea1-2283-4a65-9398-5e3ed99cd3bc</id>
        </webresult>""", 200)

    if 'jobStatus' in kwargs['url']:
        return MockedCarteResponse("""
        <jobstatus>
            <jobname>dummy_job</jobname>
            <id>f8110ea1-2283-4a65-9398-5e3ed99cd3bc</id>
            <status_desc>Finished</status_desc>
            <error_desc/>
            <logging_string><![CDATA[H4sIAAAAAAAAADMyMDTRNzTUNzRXMDC3MjS2MjJQ0FVIKc3NrYzPyk8CsoNLEotKFPLTFEDc1IrU5NKSzPw8Xi4j4nRm5qUrpOaVFFUqRLuE+vpGxhKj0y0zL7M4IzUFYieybgWNotTi0pwS2+iSotLUWE1iTPNCdrhCGtRsXi4AOMIbLPwAAAA=]]></logging_string>
            <first_log_line_nr>0</first_log_line_nr>
            <last_log_line_nr>20</last_log_line_nr>
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
                <is_stopped>N</is_stopped>
                <log_channel_id/>
                <log_text>null</log_text>
                <result-file/>
                <result-rows/>
            </result>
        </jobstatus>
        """, 200)


class TestCarteJobOperator(OperatorTestBase):
    """Test Carte Job Operator"""

    @mock.patch('requests.get', side_effect=mock_requests)
    @mock.patch('requests.post', side_effect=mock_requests)
    def test_execute(self, mock_post, mock_get):  # pylint: disable=unused-argument
        op = CarteJobOperator(
            task_id='test_carte_job_operator',
            job='/home/bi/test_job',
            level='Debug')

        op.execute(context={})
        self.assertEqual(
            'name=test_job'
            '&id=f8110ea1-2283-4a65-9398-5e3ed99cd3bc'
            '&xml=Y'
            '&from=0',
            mock_post.call_args_list[0][1]['data'])
