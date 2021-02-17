# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""This module is deprecated. Please use `airflow_pentaho.operators.carte`."""

import warnings

# pylint: disable=unused-import
from airflow_pentaho.operators.carte import CarteJobOperator  # noqa

warnings.warn(
    'This module is deprecated. Please use `airflow_pentaho.operators.carte`.',
    DeprecationWarning,
    stacklevel=2)
