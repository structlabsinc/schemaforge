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
        'sqlparse>=0.4.4',
        'sqlalchemy>=2.0.0',
        'pymysql>=1.0.0',
        'psycopg2-binary>=2.9.0',
        'snowflake-sqlalchemy>=1.4.0',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
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
