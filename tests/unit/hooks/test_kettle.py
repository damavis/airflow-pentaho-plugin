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
    from airflow.models.param import Param

WINDOWS_PDI_HOME = 'C:\\pentaho'  # noqa: W605

DEFAULT_HOME = '/opt/pentaho'
DEFAULT_REP = 'test_repository'
DEFAULT_USERNAME = 'test'
DEFAULT_PASSWORD = 'secret'


class TestPentahoClient(TestCase):
    """Testing Kettle commands (pan and kitchen) client."""

    def _get_linux_client(self):
        return PentahoHook.PentahoClient(DEFAULT_HOME,
                                         DEFAULT_REP,
                                         DEFAULT_USERNAME,
                                         DEFAULT_PASSWORD,
                                         'Linux')

    def _get_windows_client(self):
        return PentahoHook.PentahoClient(WINDOWS_PDI_HOME,
                                         DEFAULT_REP,
                                         DEFAULT_USERNAME,
                                         DEFAULT_PASSWORD,
                                         'Windows')

    def test__get_tool_command_template_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._get_tool_command_template()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '{}/{}.sh')

    def test__get_tool_command_template_windows(self):
        cli = self._get_windows_client()
        tmpl = cli._get_tool_command_template()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '{}\\{}.bat')

    def test__get_tool_command_template_unknown(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        '')
        with self.assertRaises(AirflowException) as context:
            cli._get_tool_command_template()  # pylint: disable=protected-access

        self.assertTrue('Unsupported platform'
                        in str(context.exception))

    def test__build_tool_command_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._build_tool_command('pan')  # pylint: disable=protected-access
        self.assertEqual(tmpl, '/opt/pentaho/pan.sh')

    def test__build_tool_command_windows(self):
        cli = self._get_windows_client()
        tmpl = cli._build_tool_command('pan')  # pylint: disable=protected-access
        self.assertEqual(tmpl, 'C:\\pentaho\\pan.bat')  # noqa: W605

    def test__get_argument_template_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._get_argument_template()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '-{}={}')

    def test__get_argument_template_windows(self):
        cli = self._get_windows_client()
        tmpl = cli._get_argument_template()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '/{}:{}')

    def test__build_argument_linux(self):
        cli = self._get_linux_client()
        tmpl = cli._build_argument('key', 'value')  # pylint: disable=protected-access
        self.assertEqual(tmpl, '-key=value')

    def test__build_argument_windows(self):
        cli = self._get_windows_client()
        tmpl = cli._build_argument('key', 'value')  # pylint: disable=protected-access
        self.assertEqual(tmpl, '/key:value')

    def test__build_connection_arguments(self):
        cli = self._get_linux_client()
        tmpl = cli._build_connection_arguments()  # pylint: disable=protected-access
        self.assertEqual(tmpl, '-rep=test_repository -user=test -pass=secret')

    def test_build_command(self):
        cli = self._get_linux_client()  # pylint: disable=protected-access
        tmpl = cli.build_command('pan', {'trans': 'test'}, {'version': '3'})
        self.assertEqual(tmpl, '/opt/pentaho/pan.sh -rep=test_repository'
                               ' -user=test -pass=secret'
                               ' -trans=test'
                               ' -param:version=3')

    def test_params_command(self):
        cli = self._get_linux_client()
        if version.parse(airflow.__version__) >= version.parse('2.2'):
            tmpl = cli.build_command('pan', {'trans': 'test'}, {'version': Param(5, type="integer", minimum=3)})
        else:
            tmpl = cli.build_command('pan', {'trans': 'test'}, {'version': 5})
        self.assertEqual(tmpl, '/opt/pentaho/pan.sh -rep=test_repository'
                               ' -user=test -pass=secret'
                               ' -trans=test'
                               ' -param:version=5')

    def test_empty_params_command(self):
        cli = self._get_linux_client()
        tmpl = cli.build_command('pan', {'trans': 'test'}, None)
        self.assertEqual(tmpl, '/opt/pentaho/pan.sh -rep=test_repository'
                               ' -user=test -pass=secret'
                               ' -trans=test'
                               '')
