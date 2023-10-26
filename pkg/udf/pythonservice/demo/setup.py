# coding = utf-8
# -*- coding:utf-8 -*-
from setuptools import setup, find_packages

setup(
    name="detect",
    version="1.0.0",
    packages=find_packages(),
    package_data={
        'credit': ['model_with_scaler']
    },
)
