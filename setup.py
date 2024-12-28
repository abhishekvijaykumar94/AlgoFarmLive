# from setuptools import setup, find_packages
#
# setup(
#     name="algoFarmLive",
#     version="0.1",
#     packages=find_packages(),
#     install_requires=['algoLibs','algoFarmAdapter'],  # List any dependencies
# )

from setuptools import setup, find_packages
import os

setup(
    name='AlgoFarmLive',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,  # Ensure non-Python files are included
    package_data={
        'algoFarmLive': ['lib/*.so'],  # Include .so files from the lib directory
    },
    install_requires=['algoFarmAdapter'],
    zip_safe=False,  # Set to False to ensure the .so files are extracted correctly
)
