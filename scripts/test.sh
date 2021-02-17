#!/bin/bash
rm .coverage  || echo "No previous coverage files found"
pytest --cov=airflow_pentaho --cov-report xml tests --ignore=tests/integration
