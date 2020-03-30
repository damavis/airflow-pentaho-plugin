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


from airflow_pentaho.hooks.PentahoHook import PentahoHook
from airflow_pentaho.operators.PDIBaseOperator import PDIBaseOperator


class KitchenOperator(PDIBaseOperator):

    STATUS_CODES = {
        0: "The job ran without a problem.",
        1: "Errors occurred during processing",
        2: "An unexpected error occurred during loading or running of the job",
        7: "The job couldn't be loaded from XML or the Repository",
        8: "Error loading steps or plugins (error in loading one of the"
           " plugins mostly)",
        9: "Command line usage printing"
    }

    template_fields = ('params',)

    def __init__(self,
                 job,
                 params,
                 directory=None,
                 file=None,
                 pdi_conn_id=None,
                 level="Basic",
                 logfile="/dev/stdout",
                 safemode=False,
                 maxloglines=0,
                 maxlogtimeout=0,
                 *args,
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

    def execute(self, context):
        conn = self._get_pentaho_client()

        arguments = {
            "dir": self.dir,
            "job": self.job,
            "level": self.level,
            "logfile": self.logfile,
            "safemode": "true" if self.safemode else "false",
            "maxloglines": str(self.maxloglines),
            "maxlogtimeout": str(self.maxlogtimeout)
        }
        arguments.update(self.params)
        if self.file:
            arguments.update({"file": self.file})
            arguments.update({"norep": "true"})

        self.command_line = conn.build_command("kitchen", arguments)
        output = self._run_command()

        if self.xcom_push_flag:
            return output
