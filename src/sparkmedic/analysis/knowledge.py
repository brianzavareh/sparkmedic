"""Curated references to official Apache Spark and Databricks documentation.

Every recommendation the rules emit links back to one of these pages. Links were
reviewed against the live documentation (Apache Spark 4.2 docs, Databricks docs)
in September 2026. Please send a PR when a page moves.
"""

from __future__ import annotations

from dataclasses import dataclass

DOCS_REVIEWED = "2026-09"


@dataclass(frozen=True)
class DocRef:
    title: str
    url: str


SPARK = "https://spark.apache.org/docs/latest"
DBX = "https://docs.databricks.com/aws/en"

REFS: dict[str, DocRef] = {
    # Apache Spark
    "spark_rest": DocRef("Spark monitoring REST API", f"{SPARK}/monitoring.html#rest-api"),
    "spark_sql_tuning": DocRef("Spark SQL performance tuning", f"{SPARK}/sql-performance-tuning.html"),
    "spark_aqe": DocRef(
        "Spark adaptive query execution", f"{SPARK}/sql-performance-tuning.html#adaptive-query-execution"
    ),
    "spark_hints": DocRef("Spark SQL hints", f"{SPARK}/sql-ref-syntax-qry-select-hints.html"),
    "spark_tuning": DocRef("Tuning Spark (memory, GC, parallelism)", f"{SPARK}/tuning.html"),
    "spark_config": DocRef("Spark configuration", f"{SPARK}/configuration.html"),
    "spark_arrow": DocRef(
        "Arrow in PySpark (Arrow-optimized Python UDFs, pandas UDFs)",
        f"{SPARK}/api/python/tutorial/sql/arrow_pandas.html",
    ),
    # Databricks Spark UI diagnosis guide
    "dbx_ui_guide": DocRef("Diagnose cost and performance issues using the Spark UI", f"{DBX}/optimizations/spark-ui-guide/"),
    "dbx_skew_spill": DocRef("Skew and spill", f"{DBX}/optimizations/spark-ui-guide/long-spark-stage-page"),
    "dbx_io": DocRef("Spark stage high I/O", f"{DBX}/optimizations/spark-ui-guide/long-spark-stage-io"),
    "dbx_low_io": DocRef("Slow Spark stage with little I/O", f"{DBX}/optimizations/spark-ui-guide/slow-spark-stage-low-io"),
    "dbx_gaps": DocRef("Gaps between Spark jobs", f"{DBX}/optimizations/spark-ui-guide/spark-job-gaps"),
    "dbx_driver": DocRef("Spark driver overloaded", f"{DBX}/optimizations/spark-ui-guide/spark-driver-overloaded"),
    "dbx_failing": DocRef("Failing jobs or executors removed", f"{DBX}/optimizations/spark-ui-guide/failing-spark-jobs"),
    "dbx_small_jobs": DocRef("Many small Spark jobs", f"{DBX}/optimizations/spark-ui-guide/small-spark-jobs"),
    "dbx_workloads_guide": DocRef(
        "Comprehensive guide to optimize Databricks, Spark and Delta Lake workloads",
        "https://www.databricks.com/discover/pages/optimize-data-workloads-guide",
    ),
    # Databricks features
    "dbx_aqe": DocRef("Adaptive query execution on Databricks", f"{DBX}/optimizations/aqe"),
    "dbx_photon": DocRef("What is Photon?", f"{DBX}/compute/photon"),
    "dbx_liquid": DocRef("Use liquid clustering for tables", f"{DBX}/delta/clustering"),
    "dbx_predictive": DocRef("Predictive optimization", f"{DBX}/optimizations/predictive-optimization"),
    "dbx_disk_cache": DocRef("Optimize performance with caching", f"{DBX}/optimizations/disk-cache"),
    "dbx_file_size": DocRef("Control data file size", f"{DBX}/tables/tune-file-size"),
    "dbx_merge": DocRef("Upsert into a Delta table using MERGE", f"{DBX}/delta/merge"),
    "dbx_variant": DocRef("Query variant data", f"{DBX}/semi-structured/variant"),
    "dbx_udf": DocRef("What are user-defined functions (UDFs)?", f"{DBX}/udf/"),
    "dbx_foreach": DocRef("Use foreachBatch to write to arbitrary data sinks", f"{DBX}/structured-streaming/foreach"),
    "dbx_compute_bp": DocRef("Classic compute configuration best practices", f"{DBX}/compute/cluster-config-best-practices"),
    "dbx_perf_bp": DocRef(
        "Best practices for performance efficiency", f"{DBX}/lakehouse-architecture/performance-efficiency/best-practices"
    ),
    "dbx_log_delivery": DocRef("Compute log delivery", f"{DBX}/compute/configure#compute-log-delivery"),
}


def refs(*keys: str) -> list[DocRef]:
    return [REFS[k] for k in keys if k in REFS]
