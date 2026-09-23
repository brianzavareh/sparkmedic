"""Stage-level rules. They follow the Databricks Spark UI diagnosis flow:
longest stages -> skew / spill -> I/O bound? -> other causes (compute, parallelism, overhead)."""

from __future__ import annotations

import math
from collections import Counter

from sparkmedic.analysis.context import AnalysisContext
from sparkmedic.analysis.engine import Finding, rule, severity_from_share
from sparkmedic.analysis.engine import Recommendation as R
from sparkmedic.analysis.knowledge import refs
from sparkmedic.analysis.patterns import ROW_AT_A_TIME_PY, scan_plan_text
from sparkmedic.model import Stage
from sparkmedic.units import fmt_bytes, fmt_count, fmt_ms, fmt_pct, safe_div


def _stage_label(st: Stage) -> str:
    name = (st.name or "").split(" at ")[0]
    return f"Stage {st.stage_id}" + (f" ({name})" if name else "")


def _share_ev(ctx: AnalysisContext, st: Stage) -> str:
    wall = fmt_ms(st.wall_time_ms) if st.wall_time_ms else "n/a"
    return (
        f"{_stage_label(st)} used {fmt_ms(st.executor_run_time_ms)} of task time "
        f"({fmt_pct(ctx.stage_share(st))} of the whole app), wall time {wall}, {st.num_tasks:,} tasks."
    )


# --------------------------------------------------------------------------- skew
@rule("SD101", "Task skew / stragglers", "skew", "Max task duration far above the 75th percentile (Databricks heuristic).")
def skew(ctx: AnalysisContext) -> list[Finding]:
    out = []
    t = ctx.t
    for st in ctx.significant_stages:
        p75, p50, mx = st.q("duration", 0.75), st.q("duration", 0.5), st.q("duration", 1.0)
        if not p75 or not mx or st.num_tasks < 2:
            continue
        if mx < t.skew_max_over_p75 * p75 or (mx - p75) < t.skew_min_abs_gap_ms:
            continue
        ev = [
            _share_ev(ctx, st),
            f"Task duration: median {fmt_ms(p50)}, 75th pct {fmt_ms(p75)}, max {fmt_ms(mx)} "
            f"(max is {mx / p75:.1f}x the 75th percentile; Databricks flags skew above 1.5x).",
        ]
        data_keys = [
            ("shuffleReadBytes", "shuffle read", fmt_bytes),
            ("inputBytes", "input", fmt_bytes),
            ("shuffleReadRecords", "shuffle records", fmt_count),
            ("inputRecords", "input records", fmt_count),
        ]
        data_skew = False
        for key, label, fmt in data_keys:
            q75, qmax = st.q(key, 0.75), st.q(key, 1.0)
            if q75 and qmax and qmax >= 2 * q75:
                data_skew = True
                ev.append(f"Per-task {label}: 75th pct {fmt(q75)}, max {fmt(qmax)} ({qmax / q75:.1f}x).")
                break
        hosts = Counter(tk.host for tk in st.top_tasks if tk.host)
        if st.top_tasks:
            slow = st.top_tasks[0]
            ev.append(
                f"Slowest task {slow.task_id} (partition {slow.index}) on executor {slow.executor_id} "
                f"ran {fmt_ms(slow.duration_ms)}."
            )
        one_host = len(st.top_tasks) >= 3 and len(hosts) == 1 and ctx.num_workers > 1
        if data_skew:
            root = (
                "A few partitions hold much more data than the rest (data skew). Those tasks run long while the "
                "other cores sit idle, so the stage takes as long as its biggest partition."
            )
            recs = [
                R("config", "Make sure AQE skew handling is on (default in Spark 3.2+ and Databricks)",
                  "AQE splits skewed partitions of sort-merge and shuffle-hash joins at runtime.",
                  'spark.conf.set("spark.sql.adaptive.enabled", "true")\n'
                  'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")\n'
                  '# Lower the size threshold if partitions are skewed but under 256MB:\n'
                  'spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")',
                  "python"),
                R("code", "Filter or handle the hot keys (often NULL or a default value) before the join/aggregation",
                  "Check the key distribution first: df.groupBy(key).count().orderBy(F.desc('count')).show(20)."
                  " If NULL keys cannot match, filter them out before joining.",
                  "df = df.filter(F.col('join_key').isNotNull())", "python"),
                R("code", "Salt the skewed key as a last resort",
                  "Add a random salt to the hot key on the big side and replicate the small side per salt value.",
                  "SALT = 16\nbig = big.withColumn('salt', (F.rand() * SALT).cast('int'))\n"
                  "small = small.crossJoin(spark.range(SALT).withColumnRenamed('id', 'salt'))\n"
                  "joined = big.join(small, ['join_key', 'salt'])", "python"),
            ]
            if ctx.snap.is_databricks:
                recs.insert(2, R("code", "Databricks: add a skew hint for the known skewed table/column",
                                 snippet="SELECT /*+ SKEW('orders', 'customer_id') */ * FROM orders JOIN customers USING (customer_id)",
                                 language="sql"))
        else:
            root = (
                "Data per task looks even, but some tasks still run much longer. That points to slow nodes or "
                "executors (stragglers), GC pauses, or uneven cost per row rather than uneven data size."
            )
            if one_host:
                ev.append(f"All slowest tasks ran on host {next(iter(hosts))}: likely a slow or overloaded node.")
            recs = [
                R("config", "Enable speculative execution to re-launch stragglers",
                  "Useful when slow tasks come from slow nodes, not from data skew.",
                  'spark.speculation true\nspark.speculation.multiplier 3\nspark.speculation.quantile 0.9', "text"),
                R("practice", "Check executor logs and GC time of the slow executor",
                  "Look for long GC pauses, disk or network problems, or noisy neighbours on that node."),
            ]
        sev = severity_from_share(ctx.stage_share(st))
        out.append(Finding(
            rule_id="SD101", title=f"{'Data skew' if data_skew else 'Straggler tasks'} in {_stage_label(st)}",
            severity=sev, category="skew",
            summary=f"The slowest task takes {mx / p75:.1f}x longer than most tasks, so the stage waits on a few tasks.",
            evidence=ev, root_cause=root, recommendations=recs,
            references=refs("dbx_skew_spill", "spark_aqe", "dbx_workloads_guide"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st), confidence="high" if data_skew else "medium",
        ))
    return out


# --------------------------------------------------------------------------- spill
@rule("SD102", "Spill to disk", "memory", "Execution memory ran out and data was written to disk.")
def spill(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for st in ctx.significant_stages:
        if st.disk_bytes_spilled < ctx.t.spill_min_disk_bytes:
            continue
        ev = [
            _share_ev(ctx, st),
            f"Spill: {fmt_bytes(st.memory_bytes_spilled)} in memory, {fmt_bytes(st.disk_bytes_spilled)} on disk.",
        ]
        q_max, q50 = st.q("diskBytesSpilled", 1.0), st.q("diskBytesSpilled", 0.5)
        concentrated = bool(q_max) and not q50
        if st.task_quantiles.get("diskBytesSpilled"):
            ev.append(f"Per-task disk spill: median {fmt_bytes(q50)}, max {fmt_bytes(q_max)}"
                      + (" (only a few tasks spill: likely skew)." if concentrated else "."))
        per_task_shuffle = safe_div(st.shuffle_read_bytes, st.num_tasks)
        target = 128 * 1024**2
        recs = []
        if per_task_shuffle and per_task_shuffle > target:
            n = int(math.ceil(st.shuffle_read_bytes / target))
            ev.append(f"Each task reads about {fmt_bytes(per_task_shuffle)} of shuffle data (target is 128-200 MB).")
            recs.append(R("config", "Use more, smaller shuffle partitions",
                          f"About {n:,} partitions would give ~128 MB per task for this stage "
                          "(shuffle bytes / 128 MB)." + (" On Databricks, 'auto' lets AQE choose." if ctx.snap.is_databricks else ""),
                          f'spark.conf.set("spark.sql.shuffle.partitions", "{"auto" if ctx.snap.is_databricks else n}")', "python"))
        if concentrated:
            recs.append(R("practice", "Fix the skew first", "Spill limited to a few tasks is a symptom of skew (see the skew finding)."))
        recs += [
            R("code", "Make rows narrower before wide operations",
              "Select only the columns you need before joins, sorts, windows and aggregations. Wide rows (many or big "
              "columns such as JSON strings) fill execution memory fast."),
            R("cluster", "Give each core more memory",
              "Use memory-optimized workers (e.g. Azure E-series, AWS r-family) or fewer cores per executor so each "
              "task gets a bigger share of execution memory."),
        ]
        sev = severity_from_share(ctx.stage_share(st), strong=st.disk_bytes_spilled > 1024**3)
        moved = max(ctx.stage_input_bytes(st), st.shuffle_read_bytes, st.shuffle_write_bytes, st.output_bytes) or 1.0
        spill_ratio = st.disk_bytes_spilled / moved
        if spill_ratio < 0.05:
            sev = "low"
            ev.append(f"Disk spill is only {fmt_pct(spill_ratio)} of the data this stage moves, so it is a minor cost here.")
        elif spill_ratio < 0.25 and sev in ("high", "critical"):
            sev = "medium"
        out.append(Finding(
            rule_id="SD102", title=f"Spill to disk in {_stage_label(st)}", severity=sev, category="memory",
            summary="Tasks ran out of execution memory and wrote intermediate data to disk, which is slow.",
            evidence=ev,
            root_cause="Partitions are too big for the memory each task gets (too few partitions, skew, or very wide rows).",
            recommendations=recs, references=refs("dbx_skew_spill", "dbx_workloads_guide", "dbx_compute_bp", "spark_tuning"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st), confidence="high",
        ))
    return out


# --------------------------------------------------------------------------- GC
@rule("SD103", "Garbage collection pressure", "memory", "JVM GC time is a large part of task time.")
def gc_pressure(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for st in ctx.significant_stages:
        ratio = ctx.gc_ratio(st)
        if ratio is None or ratio < ctx.t.gc_warn_ratio:
            continue
        sev = "high" if ratio >= ctx.t.gc_high_ratio and ctx.stage_share(st) >= 0.1 else "medium"
        if ctx.stage_share(st) < 0.1:
            sev = "low"
        out.append(Finding(
            rule_id="SD103", title=f"High GC time in {_stage_label(st)}", severity=sev, category="memory",
            summary=f"{fmt_pct(ratio)} of task time is spent in JVM garbage collection.",
            evidence=[_share_ev(ctx, st), f"GC time {fmt_ms(st.jvm_gc_time_ms)} of {fmt_ms(st.executor_run_time_ms)} task time."],
            root_cause="Tasks create more objects than the heap handles comfortably: big partitions, large cached data, "
                       "collect_list/collect_set on big groups, or UDFs that allocate a lot.",
            recommendations=[
                R("config", "Use more, smaller partitions so each task holds less data at once"),
                R("code", "Avoid caching large DataFrames you only use once; unpersist() when done"),
                R("cluster", "Use workers with more memory per core, or fewer cores per executor"),
                R("config", "For very large heaps, tune G1 (default in Spark 4)", snippet="spark.executor.extraJavaOptions -XX:G1HeapRegionSize=32m", language="text"),
            ],
            references=refs("spark_tuning", "dbx_compute_bp"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st) * min(1.0, ratio * 2), confidence="high",
        ))
    return out


# --------------------------------------------------------------------------- I/O vs compute
def classify(ctx: AnalysisContext, st: Stage) -> str:
    """'io' | 'compute' | 'unknown' following the Databricks decision flow."""
    rate = ctx.io_rate_mb_per_core_s(st)
    if rate is None:
        return "unknown"
    return "io" if rate >= ctx.t.io_bound_mb_per_core_s else "compute"


@rule("SD104", "Slow stage with little I/O (compute-bound)", "cpu",
      "Long stage that moves little data per core: the time goes into computation (expressions, UDFs, parsing).")
def compute_bound(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for st in ctx.significant_stages:
        if classify(ctx, st) != "compute":
            continue
        if st.disk_bytes_spilled >= ctx.t.spill_min_disk_bytes and st.disk_bytes_spilled > st.shuffle_write_bytes * 0.5:
            continue  # spill rule explains it
        rate = ctx.io_rate_mb_per_core_s(st)
        cpu = ctx.cpu_ratio(st)
        gc = ctx.gc_ratio(st) or 0.0
        rows_s = ctx.rows_per_task_second(st)
        text, specific = ctx.stage_plan_text(st)
        nodes = ctx.stage_nodes(st)
        hits = scan_plan_text(text, [n.name for n in nodes] if nodes else None)
        ev = [
            _share_ev(ctx, st),
            f"I/O rate: {rate:.2f} MB/s per core (Databricks treats ~{ctx.t.io_bound_mb_per_core_s:g} MB/s per core as I/O bound), so this stage is not I/O bound.",
        ]
        if cpu is not None:
            ev.append(f"CPU time is {fmt_pct(cpu)} of task time; GC is {fmt_pct(gc)}.")
        if rows_s:
            ev.append(f"Throughput: about {fmt_count(rows_s)} rows per second per task.")
        recs: list[R] = []
        root_parts = []
        conf = "medium"
        if hits.json_total:
            (fn, col, cnt) = hits.top_json_columns(1)[0]
            ev.append(f"The plan{' for this stage' if specific else ''} calls {fn}() {cnt:,} times on column '{col}' "
                      f"({hits.json_total:,} JSON/variant extractions in total).")
            if cnt >= ctx.t.repeated_json_calls:
                conf = "high"
                root_parts.append(
                    f"Each row is searched {cnt:,} separate times: one {fn}() call per field. With wide JSON/variant values "
                    "this repeated lookup dominates the CPU time.")
                if fn in ("variant_get", "try_variant_get"):
                    recs.append(R("code", "Extract all fields in one pass: cast the VARIANT to a STRUCT once",
                                  "One cast walks the object once instead of one lookup per field. Build the struct schema "
                                  "with the same field names and target types you extract today, and check results match "
                                  "on a sample (exceptAll) before switching.",
                                  "from pyspark.sql.types import StructType, StructField, StringType\n"
                                  "fields = [...]  # the field names you extract today\n"
                                  "schema = StructType([StructField(f, StringType()) for f in fields])\n"
                                  f"df = (df.withColumn('_parsed', F.col('{col}').cast(schema))\n"
                                  "        .select('*', '_parsed.*').drop('_parsed'))", "python"))
                else:
                    recs.append(R("code", "Parse the JSON once with from_json and a schema, then select the fields",
                                  "get_json_object/json_tuple re-parse the whole string for every call. from_json parses it "
                                  "once per row. On Databricks, consider storing the column as VARIANT.",
                                  f"parsed = df.withColumn('_j', F.from_json('{col}', schema))\n"
                                  "df = parsed.select('*', '_j.*').drop('_j')", "python"))
        if hits.python_udf_nodes:
            kinds = ", ".join(f"{k} x{v}" for k, v in hits.python_udf_nodes.items())
            ev.append(f"Python UDF operators in this stage: {kinds}. Python time is not counted as JVM CPU time.")
            root_parts.append("Rows are shipped to Python workers and processed there, which is much slower than built-in functions.")
            if any(k in ROW_AT_A_TIME_PY for k in hits.python_udf_nodes):
                conf = "high"
        if hits.regex_calls >= 5:
            ev.append(f"{hits.regex_calls} regular-expression calls in the plan.")
            root_parts.append("Many regex evaluations per row are CPU heavy.")
        if hits.max_output_width >= 300:
            ev.append(f"Rows are very wide: up to {hits.max_output_width:,} columns per row in this stage.")
            root_parts.append("Very wide rows make every sort, shuffle and window more expensive.")
        if hits.window_nodes and hits.sort_nodes:
            ev.append("The stage sorts rows for a window function (e.g. ROW_NUMBER dedup).")
        if not root_parts and gc >= ctx.t.gc_high_ratio:
            continue  # the GC rule explains this stage better
        if not root_parts:
            root_parts.append(
                "Time goes into computing expressions rather than reading or shuffling data. Look at the Project/Filter "
                "expressions of this stage in the SQL tab (UDFs, parsing, regex, complex CASE chains, explode).")
            conf = "low" if cpu is None or cpu < ctx.t.compute_cpu_ratio else "medium"
        if cpu is not None and cpu < 0.5 and not hits.python_udf_nodes:
            ev.append("Low CPU share: tasks may be waiting on external systems, locks, or Python workers.")
        recs.append(R("practice", "Replace UDFs and repeated per-row work with built-in functions",
                      "Built-in functions run inside Spark's code generation (and Photon on Databricks)."))
        if ctx.snap.is_databricks and not ctx.photon_enabled:
            recs.append(R("cluster", "Try Photon for this job",
                          "Photon speeds up expression evaluation, JSON/variant handling, scans, joins and aggregations. "
                          "It does not run Python UDFs. Compare runtime and DBU cost on a test run."))
        waves = math.ceil(st.num_tasks / ctx.total_cores) if ctx.total_cores else None
        recs.append(R("cluster", "Scale out only after fixing the code",
                      "This work is parallel: more cores cut wall time roughly linearly"
                      + (f" (now {st.num_tasks:,} tasks on {ctx.total_cores} cores = {waves} wave(s))." if waves else ".")))
        out.append(Finding(
            rule_id="SD104", title=f"Compute-bound {_stage_label(st)}",
            severity=severity_from_share(ctx.stage_share(st)), category="cpu",
            summary="The stage moves little data but runs a long time: the cost is in per-row computation.",
            evidence=ev, root_cause=" ".join(root_parts), recommendations=recs,
            references=refs("dbx_low_io", "dbx_udf", "dbx_variant", "dbx_photon"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st), confidence=conf,
        ))
    return out


@rule("SD105", "I/O-bound stage", "io", "Stage reads/writes/shuffles near the per-core I/O rate from the Databricks guide.")
def io_bound(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for st in ctx.significant_stages:
        if classify(ctx, st) != "io":
            continue
        rate = ctx.io_rate_mb_per_core_s(st) or 0
        inp = ctx.stage_input_bytes(st)
        cols = {"input": inp, "output": st.output_bytes, "shuffle read": st.shuffle_read_bytes, "shuffle write": st.shuffle_write_bytes}
        main = max(cols, key=cols.get)
        ev = [_share_ev(ctx, st), f"Largest I/O is {main}: {fmt_bytes(cols[main])} ({rate:.1f} MB/s per core)."]
        recs: list[R]
        if main == "input":
            recs = [
                R("code", "Read less data: filter early on partition/clustering columns and select only needed columns"),
                R("table", "Improve data skipping with liquid clustering on your common filter columns",
                  snippet="ALTER TABLE my_table CLUSTER BY (event_date, device_id);\nOPTIMIZE my_table;", language="sql"),
                R("table", "Let Databricks maintain layout and stats automatically (predictive optimization)",
                  snippet="ALTER SCHEMA my_catalog.my_schema ENABLE PREDICTIVE OPTIMIZATION;", language="sql"),
            ]
            if ctx.snap.is_databricks and not ctx.photon_enabled:
                recs.append(R("cluster", "Enable Photon (faster scans, especially for wide tables)"))
        elif main == "output":
            recs = [
                R("code", "Avoid rewriting unchanged data (MERGE with tight match conditions, deletion vectors)"),
                R("table", "Enable deletion vectors so updates/deletes do not rewrite whole files",
                  snippet="ALTER TABLE my_table SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);", language="sql"),
            ]
        else:
            recs = [
                R("code", "Shuffle less data: drop unused columns and pre-aggregate before joins/groupBy"),
                R("code", "Broadcast small join sides", snippet="big.join(F.broadcast(small), 'key')", language="python"),
                R("config", "Let AQE size shuffle partitions" + (" (Databricks: auto)" if ctx.snap.is_databricks else ""),
                  snippet='spark.conf.set("spark.sql.shuffle.partitions", "auto")' if ctx.snap.is_databricks
                  else 'spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")', language="python"),
            ]
        out.append(Finding(
            rule_id="SD105", title=f"I/O-bound {_stage_label(st)} ({main})", severity=severity_from_share(ctx.stage_share(st), strong=False),
            category="io", summary=f"This stage is limited by how much data it has to {('read' if main == 'input' else 'write' if main == 'output' else 'shuffle')}.",
            evidence=ev, root_cause="The stage moves a lot of data per core; reducing the bytes moved is the main lever.",
            recommendations=recs, references=refs("dbx_io", "dbx_liquid", "dbx_predictive", "dbx_photon"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st), confidence="medium",
        ))
    return out


# --------------------------------------------------------------------------- parallelism
@rule("SD106", "Low parallelism / oversized partitions", "parallelism",
      "Fewer tasks than cores, or partitions far above ~128-200 MB.")
def low_parallelism(ctx: AnalysisContext) -> list[Finding]:
    out = []
    cores = ctx.total_cores
    for st in ctx.significant_stages:
        ev, recs = [], []
        idle = cores and st.num_tasks < cores and not ctx.is_local_mode
        per_task = max(safe_div(ctx.stage_input_bytes(st), st.num_tasks) or 0, safe_div(st.shuffle_read_bytes, st.num_tasks) or 0)
        big = per_task >= ctx.t.large_partition_bytes
        if not idle and not big:
            continue
        ev.append(_share_ev(ctx, st))
        if idle:
            ev.append(f"Only {st.num_tasks} tasks for {cores} cores: {cores - st.num_tasks} cores sit idle during this stage.")
        if big:
            ev.append(f"Average data per task is {fmt_bytes(per_task)} (aim for about 128-200 MB).")
        if st.shuffle_read_bytes:
            n = max(cores * 2 if cores else 0, int(math.ceil(st.shuffle_read_bytes / (128 * 1024**2))))
            recs.append(R("config", "Raise the number of shuffle partitions" + (" or use 'auto'" if ctx.snap.is_databricks else ""),
                          snippet=f'spark.conf.set("spark.sql.shuffle.partitions", "{"auto" if ctx.snap.is_databricks else n}")', language="python"))
        else:
            recs.append(R("config", "Split input into more partitions",
                          "Lower the max bytes per file split, or repartition after reading when files are few and large.",
                          'spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")\n# or: df = df.repartition(%d)' % max(cores * 2, 2),
                          "python"))
        recs.append(R("code", "Avoid coalesce(1)/repartition(small n) before heavy work; use it only right before writing"))
        out.append(Finding(
            rule_id="SD106", title=f"Not enough parallelism in {_stage_label(st)}", severity=severity_from_share(ctx.stage_share(st), strong=False),
            category="parallelism", summary="The stage cannot use all cores, or each task handles too much data.",
            evidence=ev, root_cause="Partition count does not match cluster size or data volume.",
            recommendations=recs, references=refs("spark_tuning", "spark_sql_tuning", "dbx_aqe"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st), confidence="medium",
        ))
    return out


@rule("SD107", "Scheduling overhead / tiny tasks", "parallelism", "Many very short tasks or high scheduler delay.")
def tiny_tasks(ctx: AnalysisContext) -> list[Finding]:
    out = []
    cores = ctx.total_cores or 1
    for st in ctx.snap.stages:
        med = st.q("duration", 0.5)
        if med is None or st.num_tasks < max(4 * cores, 200):
            continue
        delay = (st.q("schedulerDelay", 0.5) or 0) + (st.q("executorDeserializeTime", 0.5) or 0)
        overhead = safe_div(delay, med) or 0
        if med >= ctx.t.tiny_task_ms and overhead < ctx.t.scheduler_overhead_ratio:
            continue
        if (st.wall_time_ms or 0) < 30_000:
            continue
        out.append(Finding(
            rule_id="SD107", title=f"Too many tiny tasks in {_stage_label(st)}", severity="medium" if (st.wall_time_ms or 0) > 120_000 else "low",
            category="parallelism",
            summary=f"{st.num_tasks:,} tasks with a median of {fmt_ms(med)}: scheduling overhead eats a big part of each task.",
            evidence=[f"{_stage_label(st)}: {st.num_tasks:,} tasks, median duration {fmt_ms(med)}, "
                      f"median scheduler delay + deserialization {fmt_ms(delay)} ({fmt_pct(overhead)} of task time), wall {fmt_ms(st.wall_time_ms)}."],
            root_cause="Too many small partitions (small files, too many shuffle partitions) make Spark spend its time scheduling.",
            recommendations=[
                R("config", "Let AQE coalesce small shuffle partitions", snippet='spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")', language="python"),
                R("table", "Compact small files (OPTIMIZE / auto compaction)", snippet="OPTIMIZE my_table;", language="sql"),
                R("config", "Pack more small files per read task", snippet='spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")', language="python"),
            ],
            references=refs("spark_tuning", "spark_aqe", "dbx_file_size"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st), confidence="medium",
        ))
    return out


@rule("SD108", "Shuffle fetch wait", "shuffle", "Tasks wait a long time for shuffle blocks from other nodes.")
def fetch_wait(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for st in ctx.significant_stages:
        ratio = safe_div(st.shuffle_fetch_wait_ms, st.executor_run_time_ms)
        if not ratio or ratio < ctx.t.fetch_wait_ratio:
            continue
        out.append(Finding(
            rule_id="SD108", title=f"Waiting on shuffle data in {_stage_label(st)}", severity=severity_from_share(ctx.stage_share(st) * ratio * 2, strong=False),
            category="shuffle", summary=f"{fmt_pct(ratio)} of task time is spent waiting for shuffle blocks over the network.",
            evidence=[_share_ev(ctx, st), f"Fetch wait {fmt_ms(st.shuffle_fetch_wait_ms)}; remote shuffle read {fmt_bytes(st.shuffle_remote_bytes_read)} of {fmt_bytes(st.shuffle_read_bytes)}."],
            root_cause="The network or the serving executors cannot deliver shuffle data fast enough (large shuffles, few big nodes, busy disks, lost executors).",
            recommendations=[
                R("code", "Shuffle less: prune columns early, pre-aggregate, broadcast small tables"),
                R("cluster", "Use instance types with better network/disk throughput, or fewer larger workers"),
            ],
            references=refs("dbx_io", "spark_sql_tuning"),
            stage_ids=[st.stage_id], sql_ids=[st.sql_execution_id] if st.sql_execution_id is not None else [],
            impact_share=ctx.stage_share(st) * ratio, confidence="medium",
        ))
    return out
