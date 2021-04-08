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
"""Setup"""


from setuptools import find_packages, setup

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='airflow-pentaho-plugin',
    version_format='{tag}.post{commitcount}',
    license='Apache 2.0',
    setup_requires=['setuptools-git-version'],
    author='Damavis',
    author_email='info@damavis.com',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/damavis/airflow-pentaho-plugin',
    python_requires='>=3',
    test_suite='nose.collector',
    zip_safe=False,
    include_package_data=True,
    packages=find_packages('.', exclude=['tests', 'tests.*']),
    classifiers=[
        'Environment :: Plugins',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Unix',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    test_requires=[
        'apache-airflow >= 2.0.1'
    ],
    install_requires=[
      'xmltodict >= 0.12.0',
    ],
    entry_points={
        'airflow.plugins': [
            'airflow_pentaho = airflow_pentaho.plugin:PentahoPlugin'
        ]
    }
)
