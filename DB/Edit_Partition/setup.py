#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-10-23


from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("saas_pro_add_partition2.pyx")
)

#  python3 setup.py build_ext --inplace # 执行编译
