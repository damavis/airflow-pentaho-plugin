# Changelog

## airflow-pentaho-plugin v1.0.8 - 2021-05-18

- Fixes Log Level Problem [#9](https://github.com/damavis/airflow-pentaho-plugin/issues/9)

## airflow-pentaho-plugin v1.0.7 - 2021-04-29

- Advanced statuses handling. (by [@IM-SIM](https://github.com/IM-SIM))

## airflow-pentaho-plugin v1.0.6 - 2021-04-16

- Backported to Airflow version 1.10.x.
- Parameters format for kettle Operators fixed.
- Testing on pre-commit hook added.
- xmldtodict bumped from 0.10.0 to 0.12.0.
- Pypi classifiers fixed.
- Style fixes.
- Add CI/CD testing for python 3.6, 3.7, 3.8 and 3.9.
- Pinning version of SQLAchemy<1.4,>=1.3.18.

## airflow-pentaho-plugin v1.0.1 - 2021-02-17

### Deprecations

- Operators have been reorganized into 2 modules, and the old modules had been
  marked as deprecated and will be removed in the future.

  - Kettle
    - `airflow_pentaho.hooks.PentahoHook -> airflow_pentaho.hooks.kettle`
    - `airflow_pentaho.operators.PDIBaseOperator -> airflow_pentaho.operators.kettle`
    - `airflow_pentaho.operators.KitchenOperator -> airflow_pentaho.operators.kettle`
    - `airflow_pentaho.operators.PanOperator -> airflow_pentaho.operators.kettle`
  - Carte
    - `airflow_pentaho.hooks.PentahoCarteHook -> airflow_pentaho.hooks.carte`
    - `airflow_pentaho.operators.CarteBaseOperator -> airflow_pentaho.operators.carte`
    - `airflow_pentaho.operators.CarteJobOperator -> airflow_pentaho.operators.carte`
    - `airflow_pentaho.operators.CarteTransOperator -> airflow_pentaho.operators.carte`

### Fixes

- Allow users to choose http:// or https:// for Carte host.
- Other minor fixes.
