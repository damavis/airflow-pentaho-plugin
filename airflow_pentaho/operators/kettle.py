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
"""kettle Operator module"""


import os
import re
import signal
from subprocess import Popen, PIPE, STDOUT
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory

from airflow import AirflowException
from airflow.models import BaseOperator

from airflow_pentaho.hooks.kettle import PentahoHook


class PDIBaseOperator(BaseOperator):
    """PDIBaseOperator is responsible to run commands and track logging."""

    DEFAULT_CONN_ID = 'pdi_default'

    def __init__(
            self,
            task_id,
            xcom_push=False,
            **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.sub_process = None
        self.xcom_push_flag = xcom_push
        self.command_line = None
        self.codes_map: dict = dict()

    def _run_command(self):

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.command_line, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    'Temporary script location: %s',
                    script_location
                )

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                command_line_log = PDIBaseOperator._hide_sensitive_data(
                    self.command_line)
                self.log.info('Running PDI: %s', command_line_log)
                self.sub_process = Popen(  # pylint: disable=W1509
                    ['bash', fname],
                    stdout=PIPE,
                    stderr=STDOUT,
                    cwd=tmp_dir,
                    preexec_fn=pre_exec)

                self.log.info('Output:')
                line = ''
                for line in iter(self.sub_process.stdout.readline, b''):
                    line = line.decode('utf-8').rstrip()
                    self.log.info(line)
                self.sub_process.wait()

                message = self.codes_map[self.sub_process.returncode]
                self.log.info(
                    'Status Code %s: ' + message,
                    self.sub_process.returncode
                )

                if self.sub_process.returncode:
                    raise AirflowException(message)

        if self.xcom_push_flag:
            return line

    @staticmethod
    def _hide_sensitive_data(text):
        return re.sub(r'(-|/)pass(=|:)([^\s]+)', '', text)

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to PDI process')
        if self.sub_process and hasattr(self.sub_process, 'pid'):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)


class PanOperator(PDIBaseOperator):
    """PanOperator runs pan.sh and track the logs and tracks logging."""

    STATUS_CODES = {
        0: 'The transformation ran without a problem.',
        1: 'Errors occurred during processing',
        2: 'An unexpected error occurred during loading / running of the'
           ' transformation',
        3: 'Unable to prepare and initialize this transformation',
        7: "The transformation couldn't be loaded from XML or the Repository",
        8: 'Error loading steps or plugins (error in loading one of the'
           ' plugins mostly)',
        9: 'Command line usage printing'
    }

    template_fields = ('params',)

    def __init__(self,
                 trans,
                 params,
                 *args,
                 directory=None,
                 file=None,
                 pdi_conn_id=None,
                 level='Basic',
                 logfile='/dev/stdout',
                 safemode=False,
                 maxloglines=0,
                 maxlogtimeout=0,
                 **kwargs):
        """
        Execute a Pan command (Pentaho Transformation). Pan runs
            transformations, either from a PDI repository (database
            or enterprise), or from a local file.

        :param trans: The name of the transformation (as it appears in
            the repository) to launch
        :type trans: str
        :param params: Set a named parameter in a dict as input parameters.
        :type params: dict
        :param directory: The repository directory that contains the
            transformation, including the leading slash.
        :param file: If you are calling a local KTR file, this is the filename,
            including the path (abspath).
        :type file: str
        :param pdi_conn_id: Pentaho Data Integration connection ID.
        :type pdi_conn_id: str
        :param level: The logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing), default is Basic.
        :type level: str
        :param logfile: A local filename to write log output to.
        :type: logfile: str
        :param safemode: Runs in safe mode, which enables extra checking.
        :type safemode: bool
        :param maxloglines: The maximum number of log lines that are kept
            internally by PDI. Set to 0 to keep all rows (default)
        :type maxloglines: int
        :param maxlogtimeout: The maximum age (in minutes) of a log line while
            being kept internally by PDI. Set to 0 to keep all rows
            indefinitely (default)
        """
        super().__init__(*args, **kwargs)

        self.pdi_conn_id = pdi_conn_id
        if not self.pdi_conn_id:
            self.pdi_conn_id = self.DEFAULT_CONN_ID
        self.dir = directory
        self.file = file
        self.trans = trans
        self.level = level
        self.logfile = logfile
        self.safemode = safemode
        self.params = params
        self.maxloglines = maxloglines
        self.maxlogtimeout = maxlogtimeout
        self.codes_map = self.STATUS_CODES

    def _get_pentaho_client(self):
        return PentahoHook(self.pdi_conn_id).get_conn()

    def execute(self, context):  # pylint: disable=unused-argument
        conn = self._get_pentaho_client()

        arguments = {
            'dir': self.dir,
            'trans': self.trans,
            'level': self.level,
            'logfile': self.logfile,
            'safemode': 'true' if self.safemode else 'false',
            'maxloglines': str(self.maxloglines),
            'maxlogtimeout': str(self.maxlogtimeout)
        }
        if self.file:
            arguments.update({'file': self.file})
            arguments.update({'norep': 'true'})

        self.command_line = conn.build_command('pan', arguments, self.params)
        output = self._run_command()

        if self.xcom_push_flag:
            return output


class KitchenOperator(PDIBaseOperator):
    """KitchenOperator runs kitchen.sh and tracks logging."""

    STATUS_CODES = {
        0: 'The job ran without a problem.',
        1: 'Errors occurred during processing',
        2: 'An unexpected error occurred during loading or running of the job',
        7: "The job couldn't be loaded from XML or the Repository",
        8: 'Error loading steps or plugins (error in loading one of the'
           ' plugins mostly)',
        9: 'Command line usage printing'
    }

    template_fields = ('params',)

    def __init__(self,
                 job,
                 params,
                 *args,
                 directory=None,
                 file=None,
                 pdi_conn_id=None,
                 level='Basic',
                 logfile='/dev/stdout',
                 safemode=False,
                 maxloglines=0,
                 maxlogtimeout=0,
                 **kwargs):
        """
        Execute a Kitchen command (Pentaho Job). Kitchen runs jobs, either from
            a PDI repository (database or enterprise), or from a local file.

        :param job: The name of the job (as it appears in the repository) to
            launch
        :type job: str
        :param params: Set a named parameter in a dict as input parameters.
        :type params: dict
        :param directory: The repository directory that contains the
            transformation, including the leading slash.
        :param file: If you are calling a local KJB file, this is the filename,
            including the path (abspath).
        :type file: str
        :param pdi_conn_id: Pentaho Data Integration connection ID.
        :type pdi_conn_id: str
        :param level: The logging level (Basic, Detailed, Debug, Rowlevel,
            Error, Nothing), default is Basic.
        :type level: str
        :param logfile: A local filename to write log output to.
        :type: logfile: str
        :param safemode: Runs in safe mode, which enables extra checking.
        :type safemode: bool
        :param maxloglines: The maximum number of log lines that are kept
            internally by PDI. Set to 0 to keep all rows (default)
        :type maxloglines: int
        :param maxlogtimeout: The maximum age (in minutes) of a log line while
            being kept internally by PDI. Set to 0 to keep all rows
            indefinitely (default)
        """
        super().__init__(*args, **kwargs)

        self.pdi_conn_id = pdi_conn_id
        if not self.pdi_conn_id:
            self.pdi_conn_id = self.DEFAULT_CONN_ID
        self.dir = directory
        self.file = file
        self.job = job
        self.level = level
        self.logfile = logfile
        self.safemode = safemode
        self.params = params
        self.maxloglines = maxloglines
        self.maxlogtimeout = maxlogtimeout
        self.codes_map = self.STATUS_CODES

    def _get_pentaho_client(self):
        return PentahoHook(self.pdi_conn_id).get_conn()

    def execute(self, context):  # pylint: disable=unused-argument
        conn = self._get_pentaho_client()

        arguments = {
            'dir': self.dir,
            'job': self.job,
            'level': self.level,
            'logfile': self.logfile,
            'safemode': 'true' if self.safemode else 'false',
            'maxloglines': str(self.maxloglines),
            'maxlogtimeout': str(self.maxlogtimeout)
        }
        if self.file:
            arguments.update({'file': self.file})
            arguments.update({'norep': 'true'})

        self.command_line = conn.build_command('kitchen', arguments, self.params)
        output = self._run_command()

        if self.xcom_push_flag:
            return output
