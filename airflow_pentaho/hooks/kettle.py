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
"""Kettle hook module"""


import platform
from packaging import version

import airflow
from airflow import AirflowException

if version.parse(airflow.__version__) >= version.parse('2.0'):
    from airflow.hooks.base import BaseHook
else:
    from airflow.hooks.base_hook import BaseHook


class PentahoHook(BaseHook):
    """Implementation hook for interact with Kettle commands"""

    class PentahoClient:
        """Implementation for Commands calls"""

        def __init__(
                self,
                pentaho_home,
                rep,
                username,
                password,
                system,
                *args,
                **kwargs):
            super().__init__(*args, **kwargs)
            self.pentaho_home = pentaho_home
            self.rep = rep
            self.username = username
            self.password = password
            self.system = system

        def _get_tool_command_template(self):
            if self.system == 'Windows':
                return '{}\\{}.bat'
            elif self.system == 'Linux':
                return """{}/{}.sh"""
            else:
                raise AirflowException(
                    "Unsupported platform for airflow_pentaho: '{}'"
                    .format(self.system))

        def _build_tool_command(self, command):
            return self._get_tool_command_template().format(self.pentaho_home,
                                                            command)

        def _get_argument_template(self):
            if self.system == 'Windows':
                return '/{}:{}'
            elif self.system == 'Linux':
                return '-{}={}'
            else:
                raise AirflowException(
                    "Unsupported platform for airflow_pentaho: '{}'"
                    .format(self.system))

        def _build_argument(self, key, value):
            return self._get_argument_template().format(key, value)

        def _build_connection_arguments(self):
            line = list()
            line.append(self._build_argument('rep', self.rep))
            line.append(self._build_argument('user', self.username))
            line.append(self._build_argument('pass', self.password))
            return ' '.join(line)

        def build_command(self, command, arguments, params):
            line = [self._build_tool_command(command),
                    self._build_connection_arguments()]
            for k, val in arguments.items():
                line.append(self._build_argument(k, val))
            for k, val in params.items():
                line.append(self._build_argument(f'param:{k}', val))

            command_line = ' '.join(line)
            return command_line

    def __init__(self, source, conn_id='pdi_default'):
        super().__init__(source)
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.pentaho_cli = None

    def get_conn(self):
        """
        Provide required object to run transformations and jobs
        :return:
        """
        if self.pentaho_cli:
            return self.pentaho_cli

        self.pentaho_cli = self.PentahoClient(
            self.extras.get('pentaho_home'),
            self.extras.get('rep'),
            self.connection.login,
            self.connection.password,
            platform.system())

        return self.pentaho_cli
