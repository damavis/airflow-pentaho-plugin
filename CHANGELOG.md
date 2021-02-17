# Changelog

## airflow-pentaho-plugin v1.0.0b1 - 2021-02-17

### Deprecations

- Operators have been reorganized in a few modules, and the old modules had been
  marked as deprecated and will be removed in the future.

  - Kettle
    - `airflow_pentaho.hooks.PentahoHook -> airflow_pentaho.hooks.kettle`
    - `airflow_pentaho.operators.PDIBaseOperator -> airflow_pentaho.operators.kettle_base`
    - `airflow_pentaho.operators.KitchenOperator -> airflow_pentaho.operators.kitchen`
    - `airflow_pentaho.operators.PanOperator -> airflow_pentaho.operators.pan`
  - Carte
    - `airflow_pentaho.hooks.PentahoCarteHook -> airflow_pentaho.hooks.carte`
    - `airflow_pentaho.operators.CarteBaseOperator -> airflow_pentaho.operators.carte`
    - `airflow_pentaho.operators.CarteJobOperator -> airflow_pentaho.operators.carte`
    - `airflow_pentaho.operators.CarteTransOperator -> airflow_pentaho.operators.carte`

### Fixes

- Minor fixes
