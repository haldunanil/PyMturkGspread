#!/usr/bin/env python
import io
from setuptools import setup, find_packages

install_requirements = io.open('requirements.txt', encoding='utf-8').read().split('\n')

test_requirements = []

long_description = io.open('README.md', encoding='utf-8').read()

setup(
    name='PyMturkGspread',
    version='1.0.0',
    author='Haldun Anil',
    author_email='haldunanil@gmail.com',
    license='MIT',
    url='https://github.com/haldunanil/pymturkgspread',
    packages=find_packages(),
    install_requires=install_requirements,
    tests_require=test_requirements,
    description='A Python package to deploy and manage surveys through Amazon Mechanical Turk and Google Spreadsheets',
    long_description=long_description,
    download_url='https://github.com/haldunanil/pymturkgspread/archive/master.zip',
    keywords=['amazon', 'aws', 'boto', 'mturk', 'mechanical turk', 'mechanical', 'turk', 'survey', 'questionnaire', 'google spreadsheets'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',   
    ],
)