"""Application-level rules: utilization, driver gaps, failures, executor loss, memory headroom."""

from __future__ import annotations

import re
from collections import Counter

from sparkmedic.analysis.context import AnalysisContext
from sparkmedic.analysis.engine import Finding, rule
from sparkmedic.analysis.engine import Recommendation as R
from sparkmedic.analysis.knowledge import refs
from sparkmedic.units import fmt_bytes, fmt_ms, fmt_pct, safe_div


def _job_intervals(ctx: AnalysisContext) -> list[tuple[int, int]]:
    ivs = []
    for j in ctx.snap.jobs:
        if j.submission_time_ms is not None and j.completion_time_ms is not None:
            ivs.append((j.submission_time_ms, j.completion_time_ms))
    return sorted(ivs)


@rule("SD301", "Idle cluster / driver-side time", "driver",
      "Long gaps with no Spark jobs, or low use of the available cores.")
def utilization(ctx: AnalysisContext) -> list[Finding]:
    snap = ctx.snap
    dur = snap.duration_ms
    if not dur or dur < 5 * 60_000 or ctx.is_local_mode:
        return []
    ivs = _job_intervals(ctx)
    gaps = []
    if ivs:
        cur_end = ivs[0][1]
        for s, e in ivs[1:]:
            if s > cur_end:
                gaps.append((cur_end, s))
            cur_end = max(cur_end, e)
        if snap.start_time_ms is not None and ivs[0][0] > snap.start_time_ms:
            gaps.insert(0, (snap.start_time_ms, ivs[0][0]))
    big_gaps = [(a, b) for a, b in gaps if b - a >= ctx.t.gap_min_ms]
    idle_ms = sum(b - a for a, b in big_gaps)
    util = safe_div(ctx.total_task_time_ms, (ctx.total_cores or 0) * dur) if ctx.total_cores else None
    out = []
    if big_gaps and idle_ms / dur >= 0.15:
        longest = max(b - a for a, b in big_gaps)
        out.append(Finding(
            rule_id="SD301", title="Long gaps with no Spark work (driver-side time)", severity="high" if idle_ms / dur >= 0.4 else "medium",
            category="driver",
            summary=f"For {fmt_ms(idle_ms)} ({fmt_pct(idle_ms / dur)} of the run) no Spark job was running while the cluster was up.",
            evidence=[f"{len(big_gaps)} gap(s) of 1 minute or more; the longest is {fmt_ms(longest)}."],
            root_cause="The driver was busy with non-Spark work (Python loops, pandas, collect() + local processing, API calls, "
                       "sleeping/polling), building very large query plans, or simply idle between notebook commands.",
            recommendations=[
                R("code", "Move driver-side Python/pandas work into Spark (DataFrame operations, pandas UDFs)"),
                R("code", "Avoid building huge plans with long withColumn() loops; use a single select() / selectExpr()",
                  snippet="df = df.select('*', *[F.expr(e).alias(n) for n, e in new_cols.items()])", language="python"),
                R("cluster", "For interactive or bursty work, use job clusters or serverless so you do not pay for idle time"),
            ],
            references=refs("dbx_gaps", "dbx_driver"), impact_share=idle_ms / dur, confidence="medium",
        ))
    elif util is not None and util < ctx.t.low_utilization:
        out.append(Finding(
            rule_id="SD301", title="Cluster cores mostly idle", severity="medium" if util < 0.15 else "low", category="cluster",
            summary=f"Tasks used only {fmt_pct(util)} of the available core-time ({ctx.total_cores} cores over {fmt_ms(dur)}).",
            evidence=[f"Total task time {fmt_ms(ctx.total_task_time_ms)} vs {ctx.total_cores} cores x {fmt_ms(dur)}."],
            root_cause="The cluster is larger than the work needs, stages have too few tasks, or the driver is the bottleneck.",
            recommendations=[
                R("cluster", "Right-size the cluster or enable autoscaling for batch jobs"),
                R("config", "Make sure big stages have at least 2-3 tasks per core"),
            ],
            references=refs("dbx_compute_bp", "spark_tuning"), impact_share=None, confidence="low",
        ))
    return out


_OOM = re.compile(r"OutOfMemory|OOM|exit code 137|exit status 137|Container killed|memory limit|GC overhead", re.I)
_SPOT = re.compile(r"spot|preempt|decommission|instance.*(lost|terminat)|node.*lost", re.I)
_AUTOSCALE = re.compile(r"autoscal|idle|killed by driver|requested by driver|dynamic allocation|downscal", re.I)


def _classify_failure(text: str) -> str:
    if _OOM.search(text):
        return "out of memory"
    if "FetchFailed" in text:
        return "shuffle fetch failed"
    if _SPOT.search(text):
        return "node lost (spot/preemption/decommission)"
    if "ExecutorLost" in text or "executor lost" in text.lower():
        return "executor lost"
    if "TaskKilled" in text or "killed" in text.lower():
        return "killed"
    if "Exception" in text or "Error" in text:
        return "application exception"
    return "other"


@rule("SD302", "Task and stage failures", "reliability", "Failed tasks/stages waste work and trigger retries.")
def failures(ctx: AnalysisContext) -> list[Finding]:
    failed_stages = [s for s in ctx.snap.stages if s.num_failed_tasks or s.status == "FAILED"]
    if not failed_stages:
        return []
    total_failed = sum(s.num_failed_tasks for s in failed_stages)
    reasons: Counter = Counter()
    examples: dict[str, str] = {}
    for s in failed_stages:
        texts = [t.error for t in s.failed_task_samples if t.error] or ([s.failure_reason] if s.failure_reason else [])
        for tx in texts:
            c = _classify_failure(tx)
            reasons[c] += 1
            examples.setdefault(c, tx.strip().splitlines()[0][:300])
    ev = [f"{total_failed:,} failed task attempts across {len(failed_stages)} stage(s): "
          + ", ".join(str(s.stage_id) for s in failed_stages[:15]) + "."]
    for c, n in reasons.most_common():
        ev.append(f"{c}: {n} sample(s). Example: {examples[c]}")
    recs = []
    if "out of memory" in reasons:
        recs += [
            R("config", "Reduce memory per task: more shuffle partitions, fewer cores per executor"),
            R("cluster", "Use memory-optimized workers or raise executor memory overhead for Python/off-heap use",
              snippet="spark.executor.memoryOverhead 4g", language="text"),
        ]
    if "shuffle fetch failed" in reasons or "node lost (spot/preemption/decommission)" in reasons or "executor lost" in reasons:
        recs += [
            R("cluster", "Use on-demand instances for the driver and for SLA-critical jobs; keep spot for flexible work"),
            R("config", "Enable graceful decommissioning so shuffle data is migrated off nodes that are being removed",
              snippet="spark.decommission.enabled true\nspark.storage.decommission.enabled true\n"
                      "spark.storage.decommission.shuffleBlocks.enabled true", language="text"),
        ]
    if "application exception" in reasons:
        recs.append(R("code", "Fix the exception shown in the failed task (bad data, schema mismatch, null handling)"))
    if not recs:
        recs.append(R("practice", "Open the failed stage in the Spark UI and read the task error messages"))
    wasted = sum(s.executor_run_time_ms for s in failed_stages if s.status == "FAILED")
    out = [Finding(
        rule_id="SD302", title="Tasks or stages failed and were retried", severity="high" if wasted or total_failed >= 10 else "medium",
        category="reliability", summary=f"{total_failed:,} task attempts failed. Each failure wastes work and can re-run whole stages.",
        evidence=ev, root_cause="See the failure categories in the evidence; memory and lost nodes are the most common causes.",
        recommendations=recs, references=refs("dbx_failing", "spark_tuning"),
        stage_ids=[s.stage_id for s in failed_stages], impact_share=safe_div(wasted, ctx.total_task_time_ms), confidence="high",
    )]
    return out


@rule("SD303", "Executors removed", "reliability", "Executors lost during the run (OOM, spot loss, autoscaling).")
def executor_loss(ctx: AnalysisContext) -> list[Finding]:
    removed = [e for e in ctx.snap.executors if not e.is_driver and e.remove_reason]
    if not removed:
        return []
    cats = Counter()
    ex = {}
    for e in removed:
        r = e.remove_reason
        c = "autoscaling" if _AUTOSCALE.search(r) else _classify_failure(r)
        cats[c] += 1
        ex.setdefault(c, r.strip().splitlines()[0][:300])
    unexpected = {k: v for k, v in cats.items() if k != "autoscaling"}
    if not unexpected:
        return []
    ev = [f"{len(removed)} executor(s) removed: " + ", ".join(f"{k} x{v}" for k, v in cats.most_common()) + "."]
    ev += [f"{k}: {ex[k]}" for k in unexpected]
    recs = []
    if "out of memory" in unexpected:
        recs.append(R("cluster", "Give executors more memory (or memory overhead) or use memory-optimized nodes"))
    if any("lost" in k or "spot" in k for k in unexpected):
        recs.append(R("cluster", "Check spot interruptions in the cluster event log; use on-demand for critical jobs"))
    recs.append(R("practice", "Read the cluster event log and executor logs for the removed executors"))
    return [Finding(
        rule_id="SD303", title="Executors were lost during the run", severity="high" if "out of memory" in unexpected else "medium",
        category="reliability", summary="Losing executors throws away their cached and shuffle data and forces recomputation.",
        evidence=ev, root_cause="Executors died or were reclaimed for reasons other than autoscaling.",
        recommendations=recs, references=refs("dbx_failing"), impact_share=None, confidence="medium",
    )]


@rule("SD304", "Many small jobs", "driver", "A long run made of many tiny Spark jobs.")
def small_jobs(ctx: AnalysisContext) -> list[Finding]:
    jobs = [j for j in ctx.snap.jobs if j.submission_time_ms is not None and j.completion_time_ms is not None]
    if len(jobs) < ctx.t.many_small_jobs:
        return []
    durs = sorted(j.completion_time_ms - j.submission_time_ms for j in jobs)
    med = durs[len(durs) // 2]
    if med > 5_000:
        return []
    return [Finding(
        rule_id="SD304", title=f"{len(jobs):,} short Spark jobs", severity="medium", category="driver",
        summary=f"The app ran {len(jobs):,} jobs with a median duration of {fmt_ms(med)}. Per-job overhead adds up.",
        evidence=[f"{len(jobs):,} jobs, median {fmt_ms(med)}, total job time {fmt_ms(sum(durs))}."],
        root_cause="Many operations on small data run one after another (loops over tables/files, per-row actions, count() checks).",
        recommendations=[
            R("code", "Batch the work: process many tables/partitions in one DataFrame operation instead of a Python loop"),
            R("practice", "Run independent pieces in parallel (Databricks multi-task jobs / for-each tasks, or Lakeflow pipelines)"),
            R("code", "Remove debug actions (count(), show(), collect()) from production code paths"),
        ],
        references=refs("dbx_small_jobs"), impact_share=None, confidence="medium",
    )]


@rule("SD305", "Large results collected to the driver", "driver", "Big task results or collect() calls.")
def large_results(ctx: AnalysisContext) -> list[Finding]:
    heavy = [s for s in ctx.snap.stages if s.result_size >= 512 * 1024**2]
    if not heavy:
        return []
    total = sum(s.result_size for s in heavy)
    return [Finding(
        rule_id="SD305", title="Large results sent to the driver", severity="medium" if total < 4 * 1024**3 else "high",
        category="driver", summary=f"Stages returned {fmt_bytes(total)} of results to the driver.",
        evidence=[f"Stage {s.stage_id} ({s.name.split(' at ')[0]}): {fmt_bytes(s.result_size)} of task results." for s in heavy[:5]],
        root_cause="collect()/toPandas() on large data, or big broadcast builds, push data through the single driver.",
        recommendations=[
            R("code", "Write results to a table/file instead of collecting them, or collect only aggregates/limited rows"),
            R("config", "If you must collect, raise the limit and give the driver enough memory", snippet="spark.driver.maxResultSize 8g", language="text"),
        ],
        references=refs("dbx_driver", "dbx_workloads_guide"), stage_ids=[s.stage_id for s in heavy], impact_share=None, confidence="high",
    )]


@rule("SD306", "Executor memory headroom", "memory", "Peak JVM heap close to the executor limit.")
def memory_headroom(ctx: AnalysisContext) -> list[Finding]:
    mem = ctx.executor_memory_bytes
    if not mem:
        return []
    peaks = [e.peak_memory.get("JVMHeapMemory", 0) for e in ctx.workers if e.peak_memory]
    if not peaks:
        return []
    peak = max(peaks)
    ratio = peak / mem
    if ratio < 0.9:
        return []
    return [Finding(
        rule_id="SD306", title="Executor heap is almost full", severity="medium", category="memory",
        summary=f"Peak JVM heap reached {fmt_pct(ratio)} of spark.executor.memory ({fmt_bytes(peak)} of {fmt_bytes(mem)}).",
        evidence=[f"Highest executor peak heap: {fmt_bytes(peak)}."],
        root_cause="Executors run close to their memory limit, which leads to long GC, spill and out-of-memory failures.",
        recommendations=[
            R("cluster", "Use more memory per core (memory-optimized nodes or fewer cores per executor)"),
            R("code", "Reduce per-task memory: narrower rows, more partitions, avoid collect_list on large groups"),
        ],
        references=refs("spark_tuning", "dbx_compute_bp"), impact_share=None, confidence="medium",
    )]
