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

from airflow import AirflowException

from airflow_pentaho.hooks.PentahoHook import PentahoHook

WINDOWS_PDI_HOME = "C:\pentaho"

DEFAULT_HOME = "/opt/pentaho"
DEFAULT_REP = "test_repository"
DEFAULT_USERNAME = "test"
DEFAULT_PASSWORD = "secret"


class TestPentahoClient(TestCase):

    def test__get_tool_command_template_linux(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Linux")
        tmpl = cli._get_tool_command_template()
        self.assertEqual(tmpl, "{}/{}.sh")

    def test__get_tool_command_template_windows(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Windows")
        tmpl = cli._get_tool_command_template()
        self.assertEqual(tmpl, "{}\{}.sh")

    def test__get_tool_command_template_unknown(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "")
        with self.assertRaises(AirflowException) as context:
            cli._get_tool_command_template()
            self.assertTrue("Unsupported platform" in context.exception)

    def test__build_tool_command_linux(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Linux")
        tmpl = cli._build_tool_command("pan")
        self.assertEqual(tmpl, "/opt/pentaho/pan.sh")

    def test__build_tool_command_windows(self):
        cli = PentahoHook.PentahoClient(WINDOWS_PDI_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Windows")
        tmpl = cli._build_tool_command("pan")
        self.assertEqual(tmpl, "C:\pentaho\pan.bat")

    def test__get_argument_template_linux(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Linux")
        tmpl = cli._get_argument_template()
        self.assertEqual(tmpl, "-{}={}")

    def test__get_argument_template_windows(self):
        cli = PentahoHook.PentahoClient(WINDOWS_PDI_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Windows")
        tmpl = cli._get_argument_template()
        self.assertEqual(tmpl, "/{}:{}")

    def test__build_argument_linux(self):
        cli = PentahoHook.PentahoClient(WINDOWS_PDI_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Linux")
        tmpl = cli._build_argument("key", "value")
        self.assertEqual(tmpl, "-key=value")

    def test__build_argument_windows(self):
        cli = PentahoHook.PentahoClient(WINDOWS_PDI_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Windows")
        tmpl = cli._build_argument("key", "value")
        self.assertEqual(tmpl, "/key:value")

    def test__build_connection_arguments(self):
        cli = PentahoHook.PentahoClient(WINDOWS_PDI_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Linux")
        tmpl = cli._build_connection_arguments()
        self.assertEqual(tmpl, "-rep=test_repository -user=test -pass=secret")

    def test_build_command(self):
        cli = PentahoHook.PentahoClient(DEFAULT_HOME,
                                        DEFAULT_REP,
                                        DEFAULT_USERNAME,
                                        DEFAULT_PASSWORD,
                                        "Linux")
        tmpl = cli.build_command("pan", {"trans": "test"})
        self.assertEqual(tmpl, "/opt/pentaho/pan.sh -rep=test_repository"
                               " -user=test -pass=secret"
                               " -trans=test")
