from setuptools import find_packages, setup

setup(
    name="airflow-pentaho-plugin",
    version='0.1.0b1',
    author='Damavis',
    author_email='info@damavis.com',
    long_description='Pentaho pan and kitchen plugin for Apache Airflow',
    url='',
    python_requires='>=3.6',
    test_suite='nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages('.', exclude=['tests', 'tests.*']),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: Unix',
        'Operating System :: Windows',
        'Environment :: Plugin'
    ],
    entry_points={
        'airflow.plugins': [
            'airflow_pentaho = airflow_pentaho.plugin:PentahoPlugin'
        ]
    }
)
