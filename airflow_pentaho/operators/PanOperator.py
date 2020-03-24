from airflow_pentaho.hooks.PentahoHook import PentahoHook
from airflow_pentaho.operators.PDIBaseOperator import PDIBaseOperator


class PanOperator(PDIBaseOperator):

    STATUS_CODES = {
        0: "The transformation ran without a problem.",
        1: "Errors occurred during processing",
        2: "An unexpected error occurred during loading / running of the transformation",
        3: "Unable to prepare and initialize this transformation",
        7: "The transformation couldn't be loaded from XML or the Repository",
        8: "Error loading steps or plugins (error in loading one of the plugins mostly)",
        9: "Command line usage printing"
    }

    def __init__(self,
                 trans,
                 params,
                 pdi_conn_id=None,
                 level="Basic",
                 logfile="/dev/stdout",
                 safemode=False,
                 maxloglines=0,
                 maxlogtimeout=0,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.pdi_conn_id = pdi_conn_id
        if not self.pdi_conn_id:
            self.pdi_conn_id = self.DEFAULT_CONN_ID
        self.trans = trans
        self.level = level
        self.logfile = logfile
        self.safemode = safemode
        self.params = params
        self.maxloglines = maxloglines
        self.maxlogtimeout = maxlogtimeout
        self.codes_map = self.STATUS_CODES

    def execute(self, context):
        conn = PentahoHook(self.pdi_conn_id).get_conn()

        arguments = {
            "trans": self.trans,
            "level": self.level,
            "logfile": self.logfile,
            "safemode": "true" if self.safemode else "false",
            "maxloglines": str(self.maxloglines),
            "maxlogtimeout": str(self.maxlogtimeout)
        }
        arguments.update(self.params)

        self.command_line = conn.build_command("pan", arguments)
        output = self._run_command()

        if self.xcom_push_flag:
            return output
