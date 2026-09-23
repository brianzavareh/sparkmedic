"""Spark / Databricks configuration checks, grounded in what actually happened in the run."""

from __future__ import annotations

from sparkmedic.analysis.context import AnalysisContext
from sparkmedic.analysis.engine import Finding, rule
from sparkmedic.analysis.engine import Recommendation as R
from sparkmedic.analysis.knowledge import refs
from sparkmedic.units import fmt_bytes, fmt_pct, safe_div


def _false(v) -> bool:
    return str(v).strip().lower() == "false"


@rule("SD401", "Adaptive query execution features disabled", "config", "AQE or its sub-features are turned off.")
def aqe_disabled(ctx: AnalysisContext) -> list[Finding]:
    conf = ctx.snap.conf
    off = [k for k in (
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.localShuffleReader.enabled",
    ) if _false(conf(k))]
    if not off:
        return []
    main_off = "spark.sql.adaptive.enabled" in off
    shuffle_heavy = sum(s.shuffle_read_bytes for s in ctx.snap.stages) > 1024**3
    return [Finding(
        rule_id="SD401", title="Adaptive Query Execution is " + ("disabled" if main_off else "partly disabled"),
        severity="high" if main_off and shuffle_heavy else "medium" if main_off or shuffle_heavy else "low", category="config",
        summary="AQE re-optimizes queries at runtime (partition sizes, skew, join strategy). Turning it off removes those fixes.",
        evidence=[f"{k} = false" for k in off],
        root_cause="Configuration explicitly disables AQE features that are on by default in Spark 3.2+ and Databricks.",
        recommendations=[R("config", "Remove these overrides (use the defaults)",
                           snippet="\n".join(f'spark.conf.set("{k}", "true")' for k in off), language="python")],
        references=refs("spark_aqe", "dbx_aqe"), impact_share=None, confidence="high",
    )]


@rule("SD402", "Shuffle partition count does not fit the data", "config",
      "Fixed spark.sql.shuffle.partitions creating partitions far from ~128 MB.")
def shuffle_partitions(ctx: AnalysisContext) -> list[Finding]:
    setting = ctx.snap.conf("spark.sql.shuffle.partitions", "200")
    if str(setting).lower() == "auto":
        return []
    coalesce_on = not _false(ctx.snap.conf("spark.sql.adaptive.coalescePartitions.enabled")) and not _false(
        ctx.snap.conf("spark.sql.adaptive.enabled"))
    big, small = [], []
    for st in ctx.snap.stages:
        if not st.shuffle_read_bytes or not st.num_tasks or not ctx.is_significant(st):
            continue
        per = st.shuffle_read_bytes / st.num_tasks
        if per >= 512 * 1024**2:
            big.append((st, per))
        elif per < 1024**2 and st.num_tasks >= 4 * max(ctx.total_cores, 1) and not coalesce_on:
            small.append((st, per))
    if not big and not small:
        return []
    ev = [f"spark.sql.shuffle.partitions = {setting}."]
    ev += [f"Stage {s.stage_id}: {s.num_tasks} shuffle partitions of ~{fmt_bytes(p)} each." for s, p in big[:5]]
    ev += [f"Stage {s.stage_id}: {s.num_tasks} shuffle partitions of only ~{fmt_bytes(p)} each." for s, p in small[:5]]
    total = sum(s.shuffle_read_bytes for s, _ in big) if big else 0
    n = int(max((s.shuffle_read_bytes for s, _ in big), default=0) / (128 * 1024**2)) + 1 if big else None
    recs = []
    if ctx.snap.is_databricks:
        recs.append(R("config", "Databricks: let AQE choose the partition count",
                      snippet='spark.conf.set("spark.sql.shuffle.partitions", "auto")', language="python"))
    if n:
        recs.append(R("config", f"Or size it for the biggest shuffle (~128 MB per partition): about {n:,}",
                      snippet=f'spark.conf.set("spark.sql.shuffle.partitions", "{n}")', language="python"))
    if small:
        recs.append(R("config", "Turn AQE partition coalescing back on",
                      snippet='spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")', language="python"))
    share = sum(ctx.stage_share(s) for s, _ in big + small)
    return [Finding(
        rule_id="SD402", title="Shuffle partitions are " + ("too large" if big else "too small"), severity="medium" if share >= 0.1 else "low",
        category="config", summary="The fixed shuffle partition count does not match how much data is shuffled.",
        evidence=ev + ([f"Total shuffle in those stages: {fmt_bytes(total)}."] if total else []),
        root_cause="One global partition number is used for every shuffle; large shuffles get huge partitions (spill), small ones get tiny tasks.",
        recommendations=recs, references=refs("dbx_aqe", "spark_aqe", "dbx_workloads_guide"),
        stage_ids=[s.stage_id for s, _ in big + small], impact_share=share, confidence="medium",
    )]


@rule("SD403", "Photon not enabled", "config", "Databricks workload that would likely benefit from Photon.")
def photon(ctx: AnalysisContext) -> list[Finding]:
    if not ctx.snap.is_databricks or ctx.photon_enabled:
        return []
    sql_time = sum(ctx.sql_share(e) for e in ctx.snap.sql)
    if sql_time < 0.3 or ctx.total_task_time_ms < 30 * 60_000:
        return []
    py_udf = any(n.name.split(" ")[0] in ("BatchEvalPython", "ArrowEvalPython") for e in ctx.snap.sql for n in e.nodes)
    return [Finding(
        rule_id="SD403", title="Photon is not enabled", severity="low", category="config",
        summary=f"{fmt_pct(min(sql_time, 1.0))} of task time is DataFrame/SQL work that Photon can usually run faster.",
        evidence=["No Photon operators in the query plans.", f"Total task time {ctx.total_task_time_ms / 3_600_000:.1f} h."],
        root_cause="The cluster runs the standard Spark engine.",
        recommendations=[R("cluster", "Test the job with Photon enabled (compute setting 'Use Photon Acceleration')",
                           "Photon has a higher DBU rate, so compare total cost, not only runtime."
                           + (" Python UDF parts will still run outside Photon." if py_udf else ""))],
        references=refs("dbx_photon", "dbx_perf_bp"), impact_share=None, confidence="low",
    )]


@rule("SD404", "Broadcast joins disabled", "config", "autoBroadcastJoinThreshold = -1 forces shuffle joins.")
def broadcast_disabled(ctx: AnalysisContext) -> list[Finding]:
    if ctx.snap.conf("spark.sql.autoBroadcastJoinThreshold") != "-1":
        return []
    smj = sum(1 for e in ctx.snap.sql for n in e.nodes if n.name.split(" ")[0].replace("Photon", "") in ("SortMergeJoin", "ShuffledHashJoin"))
    if not smj:
        return []
    return [Finding(
        rule_id="SD404", title="Automatic broadcast joins are disabled", severity="low", category="config",
        summary=f"spark.sql.autoBroadcastJoinThreshold is -1 and the app ran {smj} shuffle join(s).",
        evidence=["spark.sql.autoBroadcastJoinThreshold = -1"],
        root_cause="Every join shuffles both sides, even when one side is tiny.",
        recommendations=[R("config", "Restore the default (10MB) or set a value that fits your driver memory",
                           snippet='spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")', language="python")],
        references=refs("spark_sql_tuning", "spark_hints"), impact_share=None, confidence="medium",
    )]


@rule("SD405", "Low CPU efficiency across the app", "cpu", "JVM CPU time far below task time for the whole app.")
def cpu_efficiency(ctx: AnalysisContext) -> list[Finding]:
    run = ctx.total_task_time_ms
    cpu = sum(s.executor_cpu_time_ns for s in ctx.snap.stages) / 1e6
    ratio = safe_div(cpu, run)
    if ratio is None or run < 10 * 60_000 or ratio >= 0.4:
        return []
    py = any(n.name.split(" ")[0] in ("BatchEvalPython", "ArrowEvalPython", "MapInPandas", "FlatMapGroupsInPandas")
             for e in ctx.snap.sql for n in e.nodes)
    return [Finding(
        rule_id="SD405", title="Tasks spend most of their time waiting, not computing", severity="low", category="cpu",
        summary=f"JVM CPU time is only {fmt_pct(ratio)} of task time for the whole app.",
        evidence=[f"CPU {cpu / 3_600_000:.1f} h vs task time {run / 3_600_000:.1f} h."],
        root_cause="Time goes to waiting: reading remote storage, shuffle fetches, GC, external calls"
                   + (", or Python UDF workers (their CPU is not counted here)." if py else "."),
        recommendations=[R("practice", "Look at the stage findings above for where tasks wait (I/O, fetch wait, GC, UDFs)")],
        references=refs("dbx_ui_guide"), impact_share=None, confidence="low",
    )]
