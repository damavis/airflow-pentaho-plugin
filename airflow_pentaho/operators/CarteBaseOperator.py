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


import base64
import re
import zlib

from airflow.models import BaseOperator


class CarteBaseOperator(BaseOperator):

    FINISHED_STATUSES = ["Finished", "Stopped"]
    DEFAULT_CONN_ID = "pdi_default"

    template_fields = ('params',)

    def _log_logging_string(self, raw_logging_string):
        logs = raw_logging_string
        cdata = re.match(r'\<\!\[CDATA\[([^\]]+)\]\]\>', logs)
        cdata = cdata.group(1) if cdata else raw_logging_string
        decoded_lines = zlib.decompress(base64.b64decode(cdata),
                                        16 + zlib.MAX_WBITS)
        if decoded_lines:
            for line in re.compile(r"\r\n|\n|\r").split(
                    decoded_lines.decode("utf-8")):
                self.log.info(line)
