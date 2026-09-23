"""Rules based on SQL physical plans and their metrics (SQL / DataFrame tab)."""

from __future__ import annotations

from collections import defaultdict

from sparkmedic.analysis.context import AnalysisContext
from sparkmedic.analysis.engine import Finding, rule, severity_from_share
from sparkmedic.analysis.engine import Recommendation as R
from sparkmedic.analysis.knowledge import refs
from sparkmedic.analysis.patterns import PYTHON_UDF_NODES, ROW_AT_A_TIME_PY, scan_plan_text, scan_relation_name
from sparkmedic.model import SqlExecution, SqlNode
from sparkmedic.units import fmt_bytes, fmt_count, fmt_ms, fmt_pct


def _q(e: SqlExecution) -> str:
    d = (e.description or "").strip().splitlines()[0][:80] if e.description else ""
    return f"Query {e.execution_id}" + (f" ({d})" if d else "")


def _heavy_sql(ctx: AnalysisContext, min_share: float = 0.02) -> list[SqlExecution]:
    out = [e for e in ctx.snap.sql if ctx.sql_share(e) >= min_share or (e.duration_ms or 0) >= 60_000]
    if ctx.cfg.focus_sql_ids:
        out += [e for e in ctx.snap.sql if e.execution_id in ctx.cfg.focus_sql_ids and e not in out]
    return out


def _covered_by_stage_rule(ctx: AnalysisContext, e: SqlExecution) -> bool:
    """True when SD104 already analysed a stage of this query (avoid duplicate findings)."""
    from sparkmedic.analysis.rules.stage_rules import classify

    return any(
        st.sql_execution_id == e.execution_id and classify(ctx, st) == "compute" for st in ctx.significant_stages
    )


@rule("SD201", "Python UDFs", "code", "Python UDF operators move rows between the JVM and Python workers.")
def python_udfs(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for e in _heavy_sql(ctx):
        names = [n.name.split(" ")[0] for n in e.nodes if n.name.split(" ")[0] in PYTHON_UDF_NODES]
        if not names:
            continue
        row_at_a_time = any(n in ROW_AT_A_TIME_PY for n in names)
        share = ctx.sql_share(e)
        recs = [
            R("code", "Rewrite the logic with built-in Spark SQL functions when possible",
              "Built-in functions are the fastest option (and run in Photon on Databricks)."),
        ]
        if row_at_a_time:
            recs.append(R("code", "If you must use Python, use a vectorized (pandas/Arrow) UDF",
                          "Arrow batches rows and avoids per-row serialization. Arrow-optimized Python UDFs are on by "
                          "default since Spark 4.2; on older versions enable them explicitly.",
                          "@F.udf('string', useArrow=True)\ndef clean(s):\n    return s.upper() if s else s\n\n"
                          "# or session-wide:\nspark.conf.set('spark.sql.execution.pythonUDF.arrow.enabled', 'true')",
                          "python"))
        out.append(Finding(
            rule_id="SD201", title=f"{'Row-at-a-time ' if row_at_a_time else ''}Python UDF in {_q(e)}",
            severity=severity_from_share(share, strong=row_at_a_time) if share >= 0.05 else "low", category="code",
            summary="Rows are sent to Python processes and back, which is much slower than built-in functions.",
            evidence=[f"Plan operators: {', '.join(sorted(set(names)))}.", f"This query used {fmt_pct(share)} of the app's task time, duration {fmt_ms(e.duration_ms)}."],
            root_cause="Python UDF execution adds serialization, inter-process transfer, and runs outside Spark's optimizer and Photon.",
            recommendations=recs, references=refs("dbx_udf", "spark_arrow", "dbx_low_io"),
            sql_ids=[e.execution_id], stage_ids=ctx.sql_stage_ids(e), impact_share=share,
            confidence="high" if row_at_a_time else "medium",
        ))
    return out


@rule("SD202", "Repeated JSON / VARIANT extraction", "code", "The same column is parsed once per extracted field.")
def repeated_json(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for e in _heavy_sql(ctx):
        if _covered_by_stage_rule(ctx, e):
            continue
        hits = scan_plan_text(e.plan_description, [n.name for n in e.nodes])
        top = hits.top_json_columns(1)
        if not top or top[0][2] < ctx.t.repeated_json_calls:
            continue
        fn, col, cnt = top[0]
        share = ctx.sql_share(e)
        out.append(Finding(
            rule_id="SD202", title=f"{cnt:,} separate {fn}() calls on '{col}' in {_q(e)}",
            severity=severity_from_share(share) if share >= 0.05 else "low", category="code",
            summary="Each extracted field searches the whole JSON/VARIANT value again.",
            evidence=[f"{cnt:,} calls of {fn}() on column '{col}'.", f"Query duration {fmt_ms(e.duration_ms)}, {fmt_pct(share)} of task time."],
            root_cause="Per-field extraction repeats the parse or lookup work for every field.",
            recommendations=[
                R("code", "Parse once: from_json(col, schema) for strings, or cast a VARIANT to a STRUCT once, then select fields",
                  snippet=f"df = df.withColumn('_p', F.from_json('{col}', schema)).select('*', '_p.*').drop('_p')", language="python"),
            ],
            references=refs("dbx_variant", "dbx_low_io"),
            sql_ids=[e.execution_id], stage_ids=ctx.sql_stage_ids(e), impact_share=share, confidence="medium",
        ))
    return out


@rule("SD203", "Nested-loop or cartesian join", "join", "BroadcastNestedLoopJoin / CartesianProduct compare every row pair.")
def nested_loop(ctx: AnalysisContext) -> list[Finding]:
    out = []
    for e in ctx.snap.sql:
        bad = [n for n in e.nodes if n.name.split(" ")[0].replace("Photon", "") in ("BroadcastNestedLoopJoin", "CartesianProduct")]
        if not bad:
            continue
        share = ctx.sql_share(e)
        if share < 0.02 and (e.duration_ms or 0) < 60_000:
            continue
        rows = [n.total("number of output rows") for n in bad]
        out.append(Finding(
            rule_id="SD203", title=f"Nested-loop/cartesian join in {_q(e)}",
            severity="high" if share >= 0.1 else "medium", category="join",
            summary="The join compares every row of one side with every row of the other side.",
            evidence=[f"Operators: {', '.join(n.name for n in bad)}."] + [f"Output rows: {fmt_count(r)}" for r in rows if r],
            root_cause="The join has no equality condition Spark can hash on (e.g. only <, >, OR, or a function of both sides).",
            recommendations=[
                R("code", "Add an equality condition to the join (e.g. a bucketed range key) so Spark can hash/sort-merge"),
                R("code", "If one side is small, make sure it is the broadcast side", snippet="big.join(F.broadcast(small), cond)", language="python"),
            ],
            references=refs("dbx_low_io", "spark_hints"), sql_ids=[e.execution_id], stage_ids=ctx.sql_stage_ids(e),
            impact_share=share, confidence="high",
        ))
    return out


def _exchange_size_below(ctx: AnalysisContext, e: SqlExecution, node: SqlNode, depth: int = 0) -> float | None:
    """Find the shuffle size feeding a join input (walks through Sort/AQE wrappers)."""
    if depth > 6:
        return None
    base = node.name.split(" ")[0]
    if base in ("Exchange", "ShuffleQueryStage", "AQEShuffleRead", "PhotonShuffleExchangeSink", "PhotonShuffleExchangeSource"):
        size = node.total("data size", "shuffle bytes written", "partition data size")
        if size:
            return size
    for c in ctx.node_children(e, node):
        s = _exchange_size_below(ctx, e, c, depth + 1)
        if s is not None:
            return s
    return None


@rule("SD204", "Sort-merge join with a small side", "join", "A shuffled join where one input is small enough to broadcast.")
def broadcast_candidates(ctx: AnalysisContext) -> list[Finding]:
    out = []
    thr = ctx.t.broadcast_candidate_bytes
    auto = ctx.snap.conf("spark.sql.autoBroadcastJoinThreshold")
    for e in _heavy_sql(ctx, 0.03):
        for n in e.nodes:
            base = n.name.split(" ")[0].replace("Photon", "")
            if base not in ("SortMergeJoin", "ShuffledHashJoin"):
                continue
            kids = ctx.node_children(e, n)
            if len(kids) != 2:
                continue
            sizes = [_exchange_size_below(ctx, e, k) for k in kids]
            if None in sizes:
                continue
            small, large = min(sizes), max(sizes)
            if small > thr or large < 4 * small or large < 1024**3:
                continue
            ev = [f"{n.name} in {_q(e)}: inputs shuffle {fmt_bytes(small)} and {fmt_bytes(large)}."]
            if auto == "-1":
                ev.append("spark.sql.autoBroadcastJoinThreshold is -1, so automatic broadcast is disabled.")
            out.append(Finding(
                rule_id="SD204", title=f"Shuffle join could be a broadcast join in {_q(e)}", severity="medium", category="join",
                summary=f"Both sides are shuffled even though one side is only {fmt_bytes(small)}.",
                evidence=ev, root_cause="The small side was above the broadcast threshold (or statistics were missing), so Spark shuffled both sides.",
                recommendations=[
                    R("code", "Broadcast the small side explicitly", snippet="result = big.join(F.broadcast(small), 'key')", language="python"),
                    R("config", "Or raise the automatic threshold (driver needs enough memory)",
                      snippet='spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")', language="python"),
                ],
                references=refs("spark_hints", "spark_sql_tuning", "dbx_workloads_guide"),
                sql_ids=[e.execution_id], stage_ids=ctx.sql_stage_ids(e), impact_share=ctx.sql_share(e), confidence="medium",
            ))
    return out


@rule("SD205", "Row explosion", "join", "An operator outputs many times more rows than it receives.")
def explosion(ctx: AnalysisContext) -> list[Finding]:
    out = []
    t = ctx.t
    for e in _heavy_sql(ctx, 0.03):
        for n in e.nodes:
            base = n.name.split(" ")[0].replace("Photon", "")
            if base not in ("Generate", "SortMergeJoin", "ShuffledHashJoin", "BroadcastHashJoin", "BroadcastNestedLoopJoin"):
                continue
            rows_out = n.total("number of output rows")
            kids = ctx.node_children(e, n)
            rows_in = max([k.total("number of output rows") or 0 for k in kids] or [0])
            if not rows_out or not rows_in or rows_out < t.explosion_min_rows or rows_out < t.explosion_ratio * rows_in:
                continue
            ratio = rows_out / rows_in
            is_gen = base == "Generate"
            out.append(Finding(
                rule_id="SD205", title=f"{'explode()' if is_gen else 'Join'} multiplies rows {ratio:.0f}x in {_q(e)}",
                severity="medium", category="join",
                summary=f"{n.name} turns {fmt_count(rows_in)} input rows into {fmt_count(rows_out)} rows.",
                evidence=[f"{n.name}: input {fmt_count(rows_in)} rows, output {fmt_count(rows_out)} rows."],
                root_cause="Exploding arrays or many-to-many join keys create far more rows than the input; later stages then carry that volume."
                if is_gen else "Duplicate join keys on both sides (many-to-many) multiply the rows.",
                recommendations=[
                    R("code", "Deduplicate or aggregate before the join/explode, and check the join keys are unique where you expect them"),
                    R("config", "Give the exploding stage more, smaller input partitions",
                      snippet='spark.conf.set("spark.sql.files.maxPartitionBytes", "16MB")', language="python"),
                ],
                references=refs("dbx_low_io", "dbx_workloads_guide"),
                sql_ids=[e.execution_id], stage_ids=ctx.sql_stage_ids(e), impact_share=ctx.sql_share(e), confidence="medium",
            ))
    return out


@rule("SD206", "Small files", "io", "Scans that open many small files, or writes that create them.")
def small_files(ctx: AnalysisContext) -> list[Finding]:
    out = []
    t = ctx.t
    reads: dict[str, list] = defaultdict(lambda: [0.0, 0.0, set()])
    writes: list[tuple[SqlExecution, SqlNode, float, float]] = []
    for e in ctx.snap.sql:
        for n in e.nodes:
            files = n.total("number of files read", "files read")
            size = n.total("size of files read", "files size read")
            if "Scan" in n.name and files and size is not None:
                rel = scan_relation_name(n.name, n.description)
                reads[rel][0] += files
                reads[rel][1] += size
                reads[rel][2].add(e.execution_id)
            wfiles = n.total("number of written files", "num files written")
            wsize = n.total("written output", "bytes written")
            if wfiles and wsize is not None:
                writes.append((e, n, wfiles, wsize))
    for rel, (files, size, execs) in reads.items():
        avg = size / files if files else 0
        if files < t.small_file_min_count or avg >= t.small_file_bytes:
            continue
        out.append(Finding(
            rule_id="SD206", title=f"Reading many small files from {rel}", severity="high" if files >= 10_000 else "medium",
            category="io", summary=f"{fmt_count(files)} files with an average size of {fmt_bytes(avg)} (Databricks recommends at least 8 MB).",
            evidence=[f"{rel}: {fmt_count(files)} files, {fmt_bytes(size)} total, read by {len(execs)} query(ies)."],
            root_cause="Opening and listing many tiny files costs more than reading them. Usual causes: over-partitioned tables, frequent small appends/streaming writes.",
            recommendations=[
                R("table", "Compact the table", snippet="OPTIMIZE my_table;", language="sql"),
                R("table", "Prefer liquid clustering over fine-grained partitioning", snippet="ALTER TABLE my_table CLUSTER BY (col1);", language="sql"),
                R("table", "Enable optimized writes and auto compaction (or predictive optimization)",
                  snippet="ALTER TABLE my_table SET TBLPROPERTIES (\n  'delta.autoOptimize.optimizeWrite' = 'true',\n  'delta.autoOptimize.autoCompact' = 'auto');", language="sql"),
            ],
            references=refs("dbx_low_io", "dbx_file_size", "dbx_predictive", "dbx_liquid"),
            sql_ids=sorted(execs), impact_share=None, confidence="high",
        ))
    for e, n, wfiles, wsize in writes:
        avg = wsize / wfiles if wfiles else 0
        if wfiles < t.small_file_min_count or avg >= t.small_file_bytes:
            continue
        out.append(Finding(
            rule_id="SD206", title=f"Writing many small files in {_q(e)}", severity="medium", category="io",
            summary=f"The write produced {fmt_count(wfiles)} files averaging {fmt_bytes(avg)}.",
            evidence=[f"{n.name}: {fmt_count(wfiles)} files, {fmt_bytes(wsize)} written."],
            root_cause="Too many output partitions or high-cardinality partition columns create tiny files that slow every later read.",
            recommendations=[
                R("table", "Enable optimized writes", snippet="ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');", language="sql"),
                R("code", "Repartition by the table's partition columns before writing, or reduce partition-column cardinality"),
            ],
            references=refs("dbx_file_size", "dbx_low_io"), sql_ids=[e.execution_id], stage_ids=ctx.sql_stage_ids(e),
            impact_share=ctx.sql_share(e), confidence="high",
        ))
    return out


@rule("SD207", "Same data read repeatedly", "io", "One source is scanned by several queries/actions.")
def repeated_scans(ctx: AnalysisContext) -> list[Finding]:
    groups: dict[str, dict] = defaultdict(
        lambda: {"execs": set(), "bytes": 0.0, "rows": 0.0, "scan_times": [], "stage_ids": set()}
    )
    for e in ctx.snap.sql:
        for n in e.nodes:
            if "Scan" not in n.name:
                continue
            rel = scan_relation_name(n.name, n.description)
            g = groups[rel]
            g["execs"].add(e.execution_id)
            g["bytes"] += n.total("size of files read", "files size read") or 0
            g["rows"] += n.total("number of output rows") or 0
            g["scan_times"].append(n.total("scan time", "scan time total") or 0)
            g["stage_ids"].update(n.stage_ids)
    out = []
    for rel, g in groups.items():
        count = len(g["execs"])
        is_rdd = rel.startswith("ExistingRDD")
        if count < (2 if is_rdd else ctx.t.repeated_scan_min):
            continue
        if not is_rdd and g["bytes"] < 1024**3 and g["rows"] < 50_000_000:
            continue
        execs = sorted(g["execs"])
        # Impact = the repeated part only: all reads minus the single most expensive one.
        times = sorted(g["scan_times"])
        if any(times) and not is_rdd:
            extra = sum(times) - times[-1]
            share = extra / ctx.total_task_time_ms if ctx.total_task_time_ms else 0.0
        else:
            shares = sorted(ctx.stage_share(s) for s in ctx.snap.stages if s.stage_id in g["stage_ids"])
            share = sum(shares) - (shares[-1] if shares else 0)
        if share < 0.02 and not is_rdd:
            continue
        ev = [f"{rel} is scanned by {count} different queries: {', '.join(map(str, execs[:12]))}."]
        if g["bytes"]:
            ev.append(f"Total read across those scans: {fmt_bytes(g['bytes'])}, {fmt_count(g['rows'])} rows.")
        if is_rdd:
            root = ("The same pre-computed DataFrame (typically the micro-batch inside foreachBatch) is re-computed for every "
                    "action on it. Spark cannot push column pruning into it, so each action re-reads the full source, "
                    "including large columns you do not need for that action.")
            recs = [
                R("code", "Persist the micro-batch once, run every action on the persisted copy, then unpersist",
                  "Persist after cheap narrowing (filters/column selection) but before expensive work you reuse.",
                  "def process(batch_df, batch_id):\n    df = batch_df.filter(...).persist()\n    try:\n"
                  "        keys = df.select('dlog_date').distinct().collect()\n        # ... MERGE using df ...\n"
                  "    finally:\n        df.unpersist()", "python"),
                R("code", "Compute small side values (key lists, counts) from narrow columns only, and avoid extra actions where possible"),
            ]
        else:
            root = "Several actions re-read and re-compute the same input instead of reusing it."
            recs = [
                R("code", "Reuse the result: persist() a DataFrame used by several actions, and unpersist() it after",
                  snippet="df = df.persist()\n# ... several actions ...\ndf.unpersist()", language="python"),
                R("practice", "Combine several aggregations into one pass (one groupBy/agg with many expressions)"),
            ]
            if ctx.snap.is_databricks:
                recs.append(R("config", "Keep the Databricks disk cache on for repeated reads of the same files",
                              snippet='spark.conf.set("spark.databricks.io.cache.enabled", "true")', language="python"))
        out.append(Finding(
            rule_id="SD207", title=f"The same input is read {count} times ({rel})", severity=severity_from_share(share, strong=False),
            category="io", summary="Work is repeated because a source is re-scanned by several actions.",
            evidence=ev, root_cause=root, recommendations=recs, references=refs("dbx_foreach", "dbx_disk_cache", "spark_tuning"),
            sql_ids=execs, impact_share=share, confidence="medium",
        ))
    return out
