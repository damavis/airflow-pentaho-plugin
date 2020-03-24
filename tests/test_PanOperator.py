from unittest import TestCase

from airflow import settings
from airflow.models import Connection

from airflow_pentaho.operators.PanOperator import PanOperator


class TestPanOperator(TestCase):

    def setUp(self) -> None:
        super().setUp()
        conn = Connection(
            conn_id="pdi_default",
            login="test",
            password="secret",
            extra='{"rep": "test_repository", "pentaho_home": "/opt/pentaho"}'
        )
        session = settings.Session()
        session.add(conn)
        session.commit()

    def test_return_value(self):
        op = PanOperator(
            task_id="test_pan_operator",
            trans="test_trans",
            params={"a": "1"})

        return_value = op.execute(context={})
        self.assertTrue("Pan - Start of run." in return_value)
