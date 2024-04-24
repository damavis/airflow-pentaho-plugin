# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""This module is deprecated. Please use `airflow_pentaho.operators.kettle`."""

import warnings

# pylint: disable=unused-import
from airflow_pentaho.operators.kettle import KitchenOperator  # noqa

warnings.warn(
    'This module is deprecated. Please use `airflow_pentaho.operators.kettle`.',
    DeprecationWarning,
    stacklevel=2)
