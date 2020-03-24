from airflow.plugins_manager import AirflowPlugin


class PentahoPlugin(AirflowPlugin):
    name = "airflow_plugin"
    operators = []
    hooks = []