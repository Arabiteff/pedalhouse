"""Pedalhouse: a bike-share lakehouse built on Databricks, end to end.

This package holds the plain-Python logic shared by the labs, the
Declarative Pipeline, the job tasks and the unit tests:

- ``datagen``    : writes realistic (and deliberately messy) raw files
- ``transforms`` : PySpark column expressions and DataFrame functions
- ``config``     : names and paths used everywhere
"""

__version__ = "1.0.0"
