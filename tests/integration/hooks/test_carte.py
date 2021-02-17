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


import time
from unittest import TestCase

from airflow_pentaho.hooks.carte import PentahoCarteHook

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8880
DEFAULT_REP = 'DEFAULT'
DEFAULT_CARTE_USERNAME = 'cluster'
DEFAULT_CARTE_PASSWORD = 'cluster'
DEFAULT_REP_USERNAME = 'admin'
DEFAULT_REP_PASSWORD = 'password'


class TestPentahoCarteClient(TestCase):

    def _get_cli(self):
        return PentahoCarteHook.PentahoCarteClient(DEFAULT_HOST,
                                                   DEFAULT_PORT,
                                                   DEFAULT_REP,
                                                   DEFAULT_REP_USERNAME,
                                                   DEFAULT_REP_PASSWORD,
                                                   DEFAULT_CARTE_USERNAME,
                                                   DEFAULT_CARTE_PASSWORD)

    def test_run_job_and_wait(self):
        cli = self._get_cli()
        rs = cli.run_job('/home/bi/test_job')
        job_id = rs['webresult']['id']

        rs = None
        while not rs or rs['jobstatus']['status_desc'] != 'Finished':
            rs = cli.job_status('test_job', job_id, rs)
            time.sleep(5)

        self.assertTrue('result' in rs['jobstatus'])

    def test_run_trans_and_wait(self):
        cli = self._get_cli()
        cli.run_trans('/home/bi/test_trans')

        rs = None
        while not rs or rs['transstatus']['status_desc'] != 'Finished':
            rs = cli.trans_status('test_trans', rs)
            time.sleep(5)

        self.assertTrue('result' in rs['transstatus'])
