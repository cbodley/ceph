#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name='lua_tests',
    version='0.0.1',
    packages=find_packages(),

    author='Yuval Lifshitz',
    author_email='ylifshit@ibm.com',
    description='Lua scripting tests',
    license='MIT',
    keywords='lua testing',

    install_requires=[
        'boto3 >=1.0.0'
        ],
    )
