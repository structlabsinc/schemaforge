from setuptools import setup, find_packages
import os

# Read version
with open(os.path.join('schemaforge', 'VERSION'), 'r') as f:
    version = f.read().strip()

setup(
    name='schemaforge',
    version=version,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'sqlparse',
    ],
    entry_points={
        'console_scripts': [
            'sf=schemaforge.main:main',
        ],
    },
    package_data={
        '': ['VERSION'],
    },
)
