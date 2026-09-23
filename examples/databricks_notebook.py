# Databricks notebook source
# MAGIC %md
# MAGIC ### sparkmedic inside a Databricks notebook
# MAGIC Run this on the **same cluster** as the slow job (for example, attach this notebook to the job's
# MAGIC all-purpose cluster after the job ran, or add it as the last task of a multi-task job that shares
# MAGIC a job cluster). It reads the driver's own Spark UI REST API, so no token is needed.

# COMMAND ----------

# MAGIC %pip install sparkmedic
# MAGIC # or from a git checkout: %pip install git+https://github.com/brianzavareh/sparkmedic.git

# COMMAND ----------

from sparkmedic import analyze, collect, diagnose

result = diagnose("notebook")  # the current Spark application on this cluster
result.display()                # renders the Markdown report in the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC Focus on the stage or SQL query you care about (ids from the Spark UI):

# COMMAND ----------

result = diagnose("notebook", focus_stages=[26], focus_sql=[15])
result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Save the reports (Markdown + JSON + raw snapshot) to a Unity Catalog volume:

# COMMAND ----------

paths = result.save("/Volumes/main/default/perf_reports/")
print(paths)

# COMMAND ----------

# MAGIC %md
# MAGIC Analyze delivered event logs of a terminated job cluster (compute log delivery to a volume):

# COMMAND ----------

snap = collect("/Volumes/main/default/cluster_logs/0923-123456-abcdefgh/eventlog/")
analyze(snap).display()
