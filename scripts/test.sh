#!/bin/bash
pytest --cov=airflow_pentaho --cov-report xml tests --ignore=tests/integration
