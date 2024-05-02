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

from packaging import version
from unittest import TestCase

import airflow
from airflow import AirflowException

from airflow_pentaho.hooks.kettle import PentahoHook

if version.parse(airflow.__version__) >= version.parse('2.2'):
    from airflow.models.param import Param  # pylint: disable=ungrouped-imports


DEFAULT_HOME = '/Users/rvalenzuela/Documents/data-integration'


class TestPentahoLocalClient(TestCase):

    def _get_linux_client(self):
        return PentahoHook.PdiClient(DEFAULT_HOME,'Linux')


    def test__get_tool_command_template_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._get_tool_command_template()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '{}/{}.sh')


    def test__build_tool_command_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._build_tool_command('pan')  # pylint: disable=protected-access
        self.assertEqual(tmpl, f'{DEFAULT_HOME}/pan.sh')

    def test__get_argument_template_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._get_argument_template()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '-{}={}')


    def test__build_argument_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._build_argument('key', 'value')  # pylint: disable=protected-access
        self.assertEqual(tmpl, '-key=value')

    def test_build_command(self):
        cli = self._get_linux_client()  # pylint: disable=protected-access
        tmpl = cli.build_command('pan', {'file': 'test'}, {'version': '3'})
        self.assertEqual(tmpl, f'{DEFAULT_HOME}/pan.sh' ' -file=test' ' -param:version=3')

    def test_params_command(self):
        cli = self._get_linux_client()
        if version.parse(airflow.__version__) >= version.parse('2.2'):
            tmpl = cli.build_command('pan',
                                     {'file': 'test'},
                                     {'version': Param(5, type='integer', minimum=3)})
        else:
            tmpl = cli.build_command('pan', {'file': 'test'}, {'version': 5})
        self.assertEqual(tmpl, f'{DEFAULT_HOME}/pan.sh' ' -file=test'   ' -param:version=5')

    def test_empty_params_command(self):
        cli = self._get_linux_client()
        tmpl = cli.build_command('pan', {'file': 'test'}, None)
        self.assertEqual(tmpl, f'{DEFAULT_HOME}/pan.sh -file=test'
                               '')