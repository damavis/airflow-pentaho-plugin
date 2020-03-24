import os
import re
import signal
from subprocess import Popen, PIPE, STDOUT
from tempfile import NamedTemporaryFile

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.file import TemporaryDirectory


class PDIBaseOperator(BaseOperator):

    DEFAULT_CONN_ID = "pdi_default"

    def __init__(
            self,
            task_id,
            xcom_push=False,
            *args,
            **kwargs):
        super().__init__(task_id=task_id, *args, **kwargs)
        self.sub_process = None
        self.xcom_push_flag = xcom_push
        self.command_line = None
        self.codes_map = None

    def _run_command(self):

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.command_line, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
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
                self.log.info("Running PDI: %s", command_line_log)
                self.sub_process = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, preexec_fn=pre_exec)

                self.log.info("Output:")
                line = ''
                for line in iter(self.sub_process.stdout.readline, b''):
                    line = line.decode('utf-8').rstrip()
                    self.log.info(line)
                self.sub_process.wait()

                message = self.codes_map[self.sub_process.returncode]
                self.log.info(
                    "Status Code %s: " + message,
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
