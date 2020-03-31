# -*- coding: utf-8 -*-
# Copyright 2020 Aneior Studio, SL
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from setuptools import find_packages, setup

setup(
    name="airflow-pentaho-plugin",
    version_format='{tag}.post{commitcount}',
    setup_requires=['setuptools-git-version'],
    author='Damavis',
    author_email='info@damavis.com',
    long_description='Pentaho pan and kitchen plugin for Apache Airflow',
    url='https://damavis.com',
    python_requires='>=3.6',
    test_suite='nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages('.', exclude=['tests', 'tests.*']),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: Unix'
    ],
    install_requires=[
      "xmltodict >= 0.10.0"
    ],
    entry_points={
        'airflow.plugins': [
            'pentaho = airflow_pentaho.plugin:PentahoPlugin'
        ]
    }
)
