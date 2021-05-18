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


from unittest import TestCase

from airflow_pentaho.hooks.carte import PentahoCarteHook

DEFAULT_HOST = 'https://localhost'
DEFAULT_PORT = 8880
DEFAULT_REP = 'DEFAULT'
DEFAULT_CARTE_USERNAME = 'cluster'
DEFAULT_CARTE_PASSWORD = 'cluster'
DEFAULT_REP_USERNAME = 'admin'
DEFAULT_REP_PASSWORD = 'password'


class TestPentahoCarteClient(TestCase):
    """Test Carte Client"""

    def test_cli_constructor(self):
        cli = PentahoCarteHook.PentahoCarteClient(DEFAULT_HOST,
                                                  DEFAULT_PORT,
                                                  DEFAULT_REP,
                                                  DEFAULT_REP_USERNAME,
                                                  DEFAULT_REP_PASSWORD,
                                                  DEFAULT_CARTE_USERNAME,
                                                  DEFAULT_CARTE_PASSWORD,
                                                  level='Basic')
        self.assertEqual(cli.host, DEFAULT_HOST)
        self.assertEqual(cli.port, DEFAULT_PORT)
        self.assertEqual(cli.rep, DEFAULT_REP)
        self.assertEqual(cli.username, DEFAULT_REP_USERNAME)
        self.assertEqual(cli.password, DEFAULT_REP_PASSWORD)
        self.assertEqual(cli.carte_username, DEFAULT_CARTE_USERNAME)
        self.assertEqual(cli.carte_password, DEFAULT_CARTE_PASSWORD)
        self.assertEqual(cli.host, DEFAULT_HOST)
        self.assertEqual(cli.level, 'Basic')
