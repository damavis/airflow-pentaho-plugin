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
"""Carte Base Operator module"""


import base64
import re
import time
import zlib

from airflow import AirflowException
from airflow.models import BaseOperator

from airflow_pentaho.hooks.carte import PentahoCarteHook


class CarteBaseOperator(BaseOperator):
    """Carte Base Operator"""

    FINISHED_STATUSES = ['Finished']
    ERRORS_STATUSES = [
        'Stopped',
        'Finished (with errors)',
        'Stopped (with errors)'
    ]
    END_STATUSES = FINISHED_STATUSES + ERRORS_STATUSES

    DEFAULT_CONN_ID = 'pdi_default'

    template_fields = ('params',)

    def _log_logging_string(self, raw_logging_string):
        logs = raw_logging_string
        cdata = re.match(r'\<\!\[CDATA\[([^\]]+)\]\]\>', logs)
        cdata = cdata.group(1) if cdata else raw_logging_string
        decoded_lines = zlib.decompress(base64.b64decode(cdata),
                                        16 + zlib.MAX_WBITS)
        if decoded_lines:
            for line in re.compile(r'\r\n|\n|\r').split(
                    decoded_lines.decode('utf-8')):
                self.log.info(line)


class CarteJobOperator(CarteBaseOperator):
    """Carte Job operator. Runs job on Carte service."""

    LOG_TEMPLATE = '%s: %s, with id %s'

    def __init__(self,
                 job,
                 *args,
                 params=None,
                 pdi_conn_id=None,
                 level='Basic',
                 **kwargs):
        """
        Execute a Job in a remote Carte server from a PDI repository.
        :param job: The full path of the job
        :type job: str
        :param params: Set a named parameter in a dict as input parameters.
        :type params: dict
        :param pdi_conn_id: Pentaho Data Integration connection ID.
        :type pdi_conn_id: str
        :param level: The logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing), default is Basic.
        :type level: str
        """
        super().__init__(*args, **kwargs)

        self.pdi_conn_id = pdi_conn_id
        if not self.pdi_conn_id:
            self.pdi_conn_id = self.DEFAULT_CONN_ID
        self.job = job
        self.level = level
        self.params = params

    def _get_pentaho_carte_client(self):
        return PentahoCarteHook(conn_id=self.pdi_conn_id,
                                level=self.level).get_conn()

    def _get_job_name(self):
        return self.job.split('/').pop()

    def execute(self, context):  # pylint: disable=unused-argument
        conn = self._get_pentaho_carte_client()

        exec_job_rs = conn.run_job(self.job, self.params)
        message = exec_job_rs['webresult']['message']
        job_id = exec_job_rs['webresult']['id']
        self.log.info('%s: %s, with id %s', message, self.job, job_id)

        status_job_rs = None
        status = None
        status_desc = None
        while not status_job_rs or status_desc not in self.END_STATUSES:
            status_job_rs = conn.job_status(self._get_job_name(), job_id,
                                            status_job_rs)
            status = status_job_rs['jobstatus']
            status_desc = status['status_desc']
            self.log.info(self.LOG_TEMPLATE, status_desc, self.job, job_id)
            self._log_logging_string(status['logging_string'])

            if status_desc not in self.END_STATUSES:
                self.log.info('Sleeping 5 seconds before ask again')
                time.sleep(5)

        if 'error_desc' in status and status['error_desc']:
            self.log.error(self.LOG_TEMPLATE, status['error_desc'],
                           self.job, job_id)
            raise AirflowException(status['error_desc'])

        if status_desc in self.ERRORS_STATUSES:
            self.log.error(self.LOG_TEMPLATE, status['status_desc'],
                           self.job, job_id)
            raise AirflowException(status['status_desc'])


class CarteTransOperator(CarteBaseOperator):
    """Cart Transformation operator. Runs job on Carte service."""

    LOG_TEMPLATE = '%s: %s'

    def __init__(self,
                 trans,
                 *args,
                 params=None,
                 pdi_conn_id=None,
                 level='Basic',
                 **kwargs):
        """
        Execute a Transformation in a remote Carte server from a PDI
            repository.
        :param trans: The full path of the transformation.
        :type trans: str
        :param params: Set a named parameter in a dict as input parameters.
        :type params: dict
        :param pdi_conn_id: Pentaho Data Integration connection ID.
        :type pdi_conn_id: str
        :param level: The logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing), default is Basic.
        :type level: str
        """
        super().__init__(*args, **kwargs)

        self.pdi_conn_id = pdi_conn_id
        if not self.pdi_conn_id:
            self.pdi_conn_id = self.DEFAULT_CONN_ID
        self.trans = trans
        self.level = level
        self.params = params

    def _get_pentaho_carte_client(self):
        return PentahoCarteHook(conn_id=self.pdi_conn_id,
                                level=self.level).get_conn()

    def _get_trans_name(self):
        return self.trans.split('/').pop()

    def execute(self, context):  # pylint: disable=unused-argument
        conn = self._get_pentaho_carte_client()

        conn.run_trans(self.trans, self.params)
        self.log.info('Executing {}'.format(self.trans))

        status_trans_rs = None
        status = None
        status_desc = None
        while not status_trans_rs or status_desc not in self.FINISHED_STATUSES:
            status_trans_rs = conn.trans_status(self._get_trans_name(),
                                                status_trans_rs)
            status = status_trans_rs['transstatus']
            status_desc = status['status_desc']
            self.log.info(self.LOG_TEMPLATE, status_desc, self.trans)
            self._log_logging_string(status['logging_string'])

            if status_desc not in self.FINISHED_STATUSES:
                self.log.info('Sleeping 5 seconds before ask again')
                time.sleep(5)

        if 'error_desc' in status and status['error_desc']:
            self.log.error(self.LOG_TEMPLATE, status['error_desc'], self.trans)
            raise AirflowException(status['error_desc'])
