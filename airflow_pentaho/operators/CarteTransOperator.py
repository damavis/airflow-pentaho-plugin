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

from airflow import AirflowException

from airflow_pentaho.hooks.PentahoCarteHook import PentahoCarteHook
from airflow_pentaho.operators.CarteBaseOperator import CarteBaseOperator


class CarteTransOperator(CarteBaseOperator):

    def __init__(self,
                 trans,
                 params=None,
                 pdi_conn_id=None,
                 level="Basic",
                 *args,
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
        return PentahoCarteHook(self.pdi_conn_id, self.level).get_conn()

    def _get_trans_name(self):
        return self.trans.split("/").pop()

    def execute(self, context):
        conn = self._get_pentaho_carte_client()

        conn.run_trans(self.trans, self.params)
        self.log.info("Executing {}".format(self.trans))

        status_trans_rs = None
        status = None
        status_desc = None
        while not status_trans_rs or status_desc not in self.FINISHED_STATUSES:
            status_trans_rs = conn.trans_status(self._get_trans_name(),
                                                status_trans_rs)
            status = status_trans_rs["transstatus"]
            status_desc = status["status_desc"]
            self.log.info("%s: %s", status_desc, self.trans)
            self._log_logging_string(status["logging_string"])

            if status_desc not in self.FINISHED_STATUSES:
                self.log.info("Sleeping 5 seconds before ask again")
                time.sleep(5)

        if "error_desc" in status and status["error_desc"]:
            self.log.error("%s: %s, with id %s", status["error_desc"],
                           self.trans)
            raise AirflowException(status["error_desc"])
