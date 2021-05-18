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
"""Carte hook module"""


import requests
import xmltodict
from packaging import version
from urllib.parse import urlencode

from requests.auth import HTTPBasicAuth

import airflow

from airflow import AirflowException
if version.parse(airflow.__version__) >= version.parse('2.0'):
    from airflow.hooks.base import BaseHook
else:
    from airflow.hooks.base_hook import BaseHook


class PentahoCarteHook(BaseHook):
    """Implementation hook for interact with Carte Rest API"""

    class PentahoCarteClient:
        """Implementation for Carte calls"""

        RUN_JOB_ENDPOINT = '/kettle/executeJob/'
        JOB_STATUS_ENDPOINT = '/kettle/jobStatus/'
        RUN_TRANS_ENDPOINT = '/kettle/executeTrans/'
        TRANS_STATUS_ENDPOINT = '/kettle/transStatus/'

        def __init__(
                self,
                host,
                port,
                rep,
                username,
                password,
                carte_username,
                carte_password,
                *args,
                level='Basic',
                **kwargs):
            super().__init__(*args, **kwargs)
            self.host = host
            if not self.host.startswith('http'):
                self.host = 'http://{}'.format(self.host)
            self.port = port
            self.rep = rep
            self.username = username
            self.password = password
            self.carte_username = carte_username
            self.carte_password = carte_password
            self.level = level

        def __get_url(self, endpoint):
            return '{}:{}{}'.format(self.host, self.port, endpoint)

        def __get_auth(self):
            return HTTPBasicAuth(self.carte_username, self.carte_password)

        def job_status(self, job_name, job_id, previous_response=None):
            url = self.__get_url(self.JOB_STATUS_ENDPOINT)
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            from_line = previous_response['jobstatus']['last_log_line_nr'] \
                if previous_response \
                else 0

            payload = {
                'name': job_name,
                'id': job_id,
                'xml': 'Y',
                'from': from_line
            }

            rs = requests.post(url=url, headers=headers,
                               data=urlencode(payload), auth=self.__get_auth())
            if rs.status_code >= 400:
                result = xmltodict.parse(rs.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(rs.content)

        def run_job(self, job_path, params=None):
            url = self.__get_url(self.RUN_JOB_ENDPOINT)
            args = {
                'user': self.username,
                'pass': self.password,
                'rep': self.rep,
                'job': job_path,
                'level': self.level
            }

            if params:
                args.update(params)

            rs = requests.get(url=url, params=args, auth=self.__get_auth())
            if rs.status_code >= 400:
                result = xmltodict.parse(rs.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(rs.content)

        def trans_status(self, trans_name, previous_response=None):
            url = self.__get_url(self.TRANS_STATUS_ENDPOINT)
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            from_line = previous_response['transstatus']['last_log_line_nr'] \
                if previous_response \
                else 0

            payload = {
                'name': trans_name,
                'xml': 'Y',
                'from': from_line
            }

            rs = requests.post(url=url, headers=headers,
                               data=urlencode(payload), auth=self.__get_auth())
            if rs.status_code >= 400:
                result = xmltodict.parse(rs.content)
                raise AirflowException('{}: {}'.format(
                    result['webresult']['result'],
                    result['webresult']['message'])
                )
            else:
                return xmltodict.parse(rs.content)

        def run_trans(self, trans_path, params=None):
            url = self.__get_url(self.RUN_TRANS_ENDPOINT)
            args = {
                'user': self.username,
                'pass': self.password,
                'rep': self.rep,
                'trans': trans_path,
                'level': self.level
            }

            if params:
                args.update(params)

            rs = requests.get(url=url, params=args, auth=self.__get_auth())
            if rs.status_code >= 400:
                raise AirflowException(rs.content)

    def __init__(self, conn_id='pdi_default', level='Basic'):
        super().__init__(conn_id)
        self.conn_id = conn_id
        self.level = level
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.pentaho_cli = None

    def get_conn(self):
        """
        Provide required object to run jobs on Carte
        :return:
        """
        if self.pentaho_cli:
            return self.pentaho_cli

        self.pentaho_cli = self.PentahoCarteClient(
            host=self.connection.host,
            port=self.connection.port,
            rep=self.extras.get('rep'),
            username=self.connection.login,
            password=self.connection.password,
            carte_username=self.extras.get('carte_username'),
            carte_password=self.extras.get('carte_password'),
            level=self.level)

        return self.pentaho_cli
