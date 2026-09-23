"""sparkmedic: automated root-cause analysis for slow Apache Spark and Databricks workloads.

Quick start (Python)::

    from sparkmedic import diagnose

    result = diagnose("http://localhost:4040")      # live Spark UI
    result = diagnose("notebook")                   # inside a Databricks notebook
    result = diagnose("databricks://0923-123456-abcd123")  # remote Databricks cluster
    result = diagnose("./spark-events")             # offline event logs
    print(result.to_markdown())
"""

from sparkmedic.api import analyze, collect, diagnose
from sparkmedic.version import __version__

__all__ = ["__version__", "analyze", "collect", "diagnose"]
