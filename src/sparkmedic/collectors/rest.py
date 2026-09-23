"""Collector for the documented Spark monitoring REST API (``/api/v1``).

Works against a live driver UI (``http://driver:4040``), a Spark History Server
(``http://host:18080``) or any proxy that forwards to them (for example the
Databricks driver proxy). Only documented, read-only GET endpoints are used:
https://spark.apache.org/docs/latest/monitoring.html#rest-api
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Optional

from sparkmedic.http import HttpClient, SparkApiError
from sparkmedic.model import (
    QUANTILES,
    Executor,
    Job,
    MetricValue,
    Snapshot,
    SourceInfo,
    SqlExecution,
    SqlNode,
    Stage,
    TaskSample,
    redact_conf,
)
from sparkmedic.units import parse_metric_text, parse_spark_time

ProgressFn = Callable[[str], None]


@dataclass
class CollectOptions:
    top_stages: int = 25  # stages (by task time) that get task distributions + slowest tasks
    task_samples: int = 5  # slowest tasks fetched per detailed stage
    max_sql: int = 500  # max SQL executions to fetch
    include_plans: bool = True
    parallelism: int = 6
    stage_ids: Optional[list[int]] = None  # always include these stages in detail


@dataclass
class AppRef:
    app_id: str
    name: str
    attempt_id: Optional[str]
    completed: bool
    start_ms: Optional[int]
    end_ms: Optional[int]

    def as_dict(self) -> dict:
        return {
            "app_id": self.app_id,
            "name": self.name,
            "attempt_id": self.attempt_id,
            "completed": self.completed,
            "start_time_ms": self.start_ms,
            "end_time_ms": self.end_ms,
        }


class RestCollector:
    """Collects a :class:`Snapshot` through the Spark REST API."""

    def __init__(self, client: HttpClient, source: SourceInfo, progress: Optional[ProgressFn] = None):
        self.client = client
        self.source = source
        self.progress = progress or (lambda _msg: None)

    # ------------------------------------------------------------------ apps
    def list_applications(self, limit: int = 50) -> list[AppRef]:
        apps = self.client.get_json("applications", {"limit": limit}) or []
        refs = []
        for app in apps:
            attempts = app.get("attempts") or [{}]
            last = attempts[0]  # REST returns most recent attempt first
            refs.append(
                AppRef(
                    app_id=app.get("id", ""),
                    name=app.get("name", ""),
                    attempt_id=last.get("attemptId"),
                    completed=bool(last.get("completed", False)),
                    start_ms=last.get("startTimeEpoch") or parse_spark_time(last.get("startTime")),
                    end_ms=(last.get("endTimeEpoch") or parse_spark_time(last.get("endTime")))
                    if last.get("completed")
                    else None,
                )
            )
        return refs

    def resolve_app(self, app_id: Optional[str] = None) -> AppRef:
        apps = self.list_applications(limit=200)
        if not apps:
            raise SparkApiError("No applications found at this endpoint.")
        if app_id:
            for a in apps:
                if a.app_id == app_id:
                    return a
            raise SparkApiError(f"Application {app_id!r} not found. Available: {[a.app_id for a in apps[:10]]}")
        running = [a for a in apps if not a.completed]
        if running:
            return running[0]
        return max(apps, key=lambda a: a.start_ms or 0)

    def base_path(self, app: AppRef) -> str:
        if app.attempt_id:
            return f"applications/{app.app_id}/{app.attempt_id}/"
        return f"applications/{app.app_id}/"

    # --------------------------------------------------------------- collect
    def collect(self, app_id: Optional[str] = None, options: Optional[CollectOptions] = None) -> Snapshot:
        opts = options or CollectOptions()
        app = self.resolve_app(app_id)
        base = self.base_path(app)
        notes: list[str] = []
        self.progress(f"Collecting application {app.app_id} ({app.name})")

        version = ""
        try:
            version = (self.client.get_json("version") or {}).get("spark", "")
        except SparkApiError:
            pass

        conf: dict[str, str] = {}
        try:
            env = self.client.get_json(base + "environment")
            conf = {k: v for k, v in (env.get("sparkProperties") or [])}
            if not version:
                version = (env.get("runtime") or {}).get("sparkVersion", "") or conf.get("spark.version", "")
        except SparkApiError as exc:
            notes.append(f"Could not read environment: {exc}")

        self.progress("Reading executors, jobs and stages")
        executors = [self._executor(e) for e in self._get_list(base + "allexecutors", notes)]
        jobs = [self._job(j) for j in self._get_list(base + "jobs", notes)]
        stages = [self._stage(s) for s in self._get_list(base + "stages", notes)]

        sql: list[SqlExecution] = []
        try:
            sql = self._collect_sql(base, opts)
        except SparkApiError as exc:
            notes.append(f"SQL endpoint unavailable ({exc}); plan-level rules are skipped.")

        # link jobs/stages to SQL executions
        job_to_sql: dict[int, int] = {}
        for e in sql:
            for jid in e.job_ids:
                job_to_sql[jid] = e.execution_id
        stage_to_jobs: dict[int, list[int]] = {}
        for j in jobs:
            j.sql_execution_id = job_to_sql.get(j.job_id)
            for sid in j.stage_ids:
                stage_to_jobs.setdefault(sid, []).append(j.job_id)
        for s in stages:
            s.job_ids = sorted(stage_to_jobs.get(s.stage_id, []))
            s.sql_execution_id = next((job_to_sql[j] for j in s.job_ids if j in job_to_sql), None)

        self._detail_stages(base, stages, opts, notes)

        retained = conf.get("spark.ui.retainedStages")
        if retained and retained.isdigit() and len(stages) >= int(retained):
            notes.append(
                f"The UI retained only the last {retained} stages (spark.ui.retainedStages); older stages are missing."
            )

        return Snapshot(
            app_id=app.app_id,
            app_name=app.name,
            spark_version=version,
            source=self.source,
            start_time_ms=app.start_ms,
            end_time_ms=app.end_ms,
            completed=app.completed,
            collected_at_ms=int(time.time() * 1000),
            spark_conf=redact_conf(conf),
            executors=executors,
            jobs=jobs,
            stages=stages,
            sql=sql,
            notes=notes,
        )

    # ---------------------------------------------------------------- helpers
    def _get_list(self, path: str, notes: list[str], params: Optional[dict] = None) -> list:
        try:
            return self.client.get_json(path, params) or []
        except SparkApiError as exc:
            notes.append(f"Could not read {path}: {exc}")
            return []

    def _collect_sql(self, base: str, opts: CollectOptions) -> list[SqlExecution]:
        out: list[SqlExecution] = []
        page = 100
        offset = 0
        self.progress("Reading SQL executions and plans")
        while len(out) < opts.max_sql:
            batch = self.client.get_json(
                base + "sql",
                {
                    "details": "true",
                    "planDescription": "true" if opts.include_plans else "false",
                    "offset": offset,
                    "length": page,
                },
            )
            if not batch:
                break
            out.extend(self._sql(e) for e in batch)
            if len(batch) < page:
                break
            offset += page
        if len(out) >= opts.max_sql:
            out.sort(key=lambda e: e.duration_ms or 0, reverse=True)
            out = out[: opts.max_sql]
        return out

    def _detail_stages(self, base: str, stages: list[Stage], opts: CollectOptions, notes: list[str]) -> None:
        ran = [s for s in stages if s.status not in ("SKIPPED", "PENDING")]
        ranked = sorted(ran, key=lambda s: s.executor_run_time_ms, reverse=True)
        chosen = {s.key: s for s in ranked[: opts.top_stages]}
        for s in ran:
            if s.num_failed_tasks > 0 or s.status == "FAILED":
                chosen.setdefault(s.key, s)
            if opts.stage_ids and s.stage_id in opts.stage_ids:
                chosen.setdefault(s.key, s)
        if not chosen:
            return
        self.progress(f"Reading task distributions for {len(chosen)} stages")

        def work(stage: Stage) -> None:
            path = f"{base}stages/{stage.stage_id}/{stage.attempt_id}/"
            try:
                summary = self.client.get_json(
                    path + "taskSummary", {"quantiles": ",".join(str(q) for q in QUANTILES)}
                )
                stage.task_quantiles = _flatten_task_summary(summary)
            except SparkApiError as exc:
                notes.append(f"taskSummary unavailable for stage {stage.key}: {exc}")
            try:
                tasks = self.client.get_json(
                    path + "taskList", {"sortBy": "-runtime", "length": opts.task_samples, "offset": 0}
                )
                stage.top_tasks = [_task_sample(t) for t in tasks or []]
            except SparkApiError as exc:
                notes.append(f"taskList unavailable for stage {stage.key}: {exc}")
            if stage.num_failed_tasks:
                try:
                    failed = self.client.get_json(path + "taskList", {"status": "failed", "length": 5})
                    stage.failed_task_samples = [_task_sample(t) for t in failed or []]
                except SparkApiError:
                    pass

        with ThreadPoolExecutor(max_workers=max(1, opts.parallelism)) as pool:
            list(pool.map(work, chosen.values()))

    # ---------------------------------------------------------------- mappers
    @staticmethod
    def _executor(e: dict) -> Executor:
        return Executor(
            executor_id=str(e.get("id", "")),
            host=(e.get("hostPort") or "").rsplit(":", 1)[0],
            is_active=bool(e.get("isActive", True)),
            total_cores=int(e.get("totalCores") or 0),
            max_tasks=int(e.get("maxTasks") or 0),
            max_memory=float(e.get("maxMemory") or 0),
            total_duration_ms=float(e.get("totalDuration") or 0),
            total_gc_time_ms=float(e.get("totalGCTime") or 0),
            total_input_bytes=float(e.get("totalInputBytes") or 0),
            total_shuffle_read=float(e.get("totalShuffleRead") or 0),
            total_shuffle_write=float(e.get("totalShuffleWrite") or 0),
            completed_tasks=int(e.get("completedTasks") or 0),
            failed_tasks=int(e.get("failedTasks") or 0),
            add_time_ms=parse_spark_time(e.get("addTime")),
            remove_time_ms=parse_spark_time(e.get("removeTime")),
            remove_reason=e.get("removeReason") or "",
            peak_memory={k: float(v) for k, v in (e.get("peakMemoryMetrics") or {}).items()},
        )

    @staticmethod
    def _job(j: dict) -> Job:
        return Job(
            job_id=int(j.get("jobId")),
            name=j.get("name") or "",
            status=j.get("status") or "",
            submission_time_ms=parse_spark_time(j.get("submissionTime")),
            completion_time_ms=parse_spark_time(j.get("completionTime")),
            stage_ids=list(j.get("stageIds") or []),
            num_tasks=int(j.get("numTasks") or 0),
            num_failed_tasks=int(j.get("numFailedTasks") or 0),
            num_skipped_stages=int(j.get("numSkippedStages") or 0),
        )

    @staticmethod
    def _stage(s: dict) -> Stage:
        g = s.get
        return Stage(
            stage_id=int(g("stageId")),
            attempt_id=int(g("attemptId") or 0),
            name=g("name") or "",
            description=g("description") or "",
            status=g("status") or "",
            num_tasks=int(g("numTasks") or 0),
            num_complete_tasks=int(g("numCompleteTasks") or 0),
            num_failed_tasks=int(g("numFailedTasks") or 0),
            num_killed_tasks=int(g("numKilledTasks") or 0),
            submission_time_ms=parse_spark_time(g("submissionTime")),
            first_task_launched_ms=parse_spark_time(g("firstTaskLaunchedTime")),
            completion_time_ms=parse_spark_time(g("completionTime")),
            failure_reason=(g("failureReason") or "")[:2000],
            executor_run_time_ms=float(g("executorRunTime") or 0),
            executor_cpu_time_ns=float(g("executorCpuTime") or 0),
            executor_deserialize_time_ms=float(g("executorDeserializeTime") or 0),
            jvm_gc_time_ms=float(g("jvmGcTime") or 0),
            result_size=float(g("resultSize") or 0),
            input_bytes=float(g("inputBytes") or 0),
            input_records=float(g("inputRecords") or 0),
            output_bytes=float(g("outputBytes") or 0),
            output_records=float(g("outputRecords") or 0),
            shuffle_read_bytes=float(g("shuffleReadBytes") or 0),
            shuffle_read_records=float(g("shuffleReadRecords") or 0),
            shuffle_remote_bytes_read=float(g("shuffleRemoteBytesRead") or 0),
            shuffle_fetch_wait_ms=float(g("shuffleFetchWaitTime") or 0),
            shuffle_write_bytes=float(g("shuffleWriteBytes") or 0),
            shuffle_write_records=float(g("shuffleWriteRecords") or 0),
            shuffle_write_time_ns=float(g("shuffleWriteTime") or 0),
            memory_bytes_spilled=float(g("memoryBytesSpilled") or 0),
            disk_bytes_spilled=float(g("diskBytesSpilled") or 0),
            peak_execution_memory=float(g("peakExecutionMemory") or 0),
        )

    @staticmethod
    def _sql(e: dict) -> SqlExecution:
        nodes = []
        for n in e.get("nodes") or []:
            metrics = {}
            for m in n.get("metrics") or []:
                p = parse_metric_text(str(m.get("value", "")))
                metrics[m.get("name", "")] = MetricValue(
                    unit=p.unit, total=p.total, min=p.min, med=p.med, max=p.max, text=p.text
                )
            nodes.append(
                SqlNode(
                    node_id=int(n.get("nodeId", 0)),
                    name=n.get("nodeName", ""),
                    codegen_id=n.get("wholeStageCodegenId"),
                    metrics=metrics,
                )
            )
        by_id = {n.node_id: n for n in nodes}
        for edge in e.get("edges") or []:
            parent = by_id.get(edge.get("toId"))
            if parent is not None and edge.get("fromId") is not None:
                parent.children.append(int(edge["fromId"]))
        _assign_node_stages(nodes)
        plan = e.get("planDescription") or ""
        _attach_node_descriptions(nodes, plan)
        job_ids = sorted(
            set(e.get("successJobIds") or []) | set(e.get("failedJobIds") or []) | set(e.get("runningJobIds") or [])
        )
        return SqlExecution(
            execution_id=int(e.get("id")),
            description=e.get("description") or "",
            status=e.get("status") or "",
            submission_time_ms=parse_spark_time(e.get("submissionTime")),
            duration_ms=float(e.get("duration") or 0),
            job_ids=job_ids,
            root_execution_id=e.get("rootExecutionId"),
            plan_description=plan[:400_000],
            nodes=nodes,
            error_message=(e.get("errorMessage") or "")[:2000],
        )


_STAGE_IN_METRIC = re.compile(r"stage (\d+)\.(\d+)")


def _assign_node_stages(nodes: list[SqlNode]) -> None:
    """Map nodes to stages using the "(stage X.Y: task Z)" hints in metric strings, then
    propagate through whole-stage-codegen blocks (members of one block run in one stage)."""
    for n in nodes:
        ids = set()
        for m in n.metrics.values():
            for sid, _attempt in _STAGE_IN_METRIC.findall(m.text or ""):
                ids.add(int(sid))
        n.stage_ids = sorted(ids)
    propagate_codegen_stages(nodes)


def propagate_codegen_stages(nodes: list[SqlNode]) -> None:
    by_codegen: dict[int, set[int]] = {}
    for n in nodes:
        m = re.match(r"^WholeStageCodegen \((\d+)\)", n.name)
        cg = int(m.group(1)) if m else n.codegen_id
        if cg is not None and n.stage_ids:
            by_codegen.setdefault(cg, set()).update(n.stage_ids)
    for n in nodes:
        m = re.match(r"^WholeStageCodegen \((\d+)\)", n.name)
        cg = int(m.group(1)) if m else n.codegen_id
        if cg is not None and cg in by_codegen:
            n.stage_ids = sorted(set(n.stage_ids) | by_codegen[cg])


def _attach_node_descriptions(nodes: list[SqlNode], plan: str) -> None:
    """Attach the formatted-plan detail block ("(3) Project ...") to nodes, by node name order."""
    if not plan or not nodes:
        return
    blocks = _plan_blocks(plan)
    # Match nodes to blocks by (normalized) name in order of appearance.
    used: set[int] = set()
    for node in nodes:
        base = node.name.split(" (")[0].strip()
        for idx, (name, text) in enumerate(blocks):
            if idx in used:
                continue
            if name == base or name.startswith(base) or base.startswith(name):
                node.description = text[:20_000]
                used.add(idx)
                break


def _plan_blocks(plan: str) -> list[tuple[str, str]]:
    """Split the formatted physical plan into ``(operator name, detail text)`` blocks."""
    import re

    blocks: list[tuple[str, str]] = []
    pattern = re.compile(r"^\((\d+)\)\s+(.+?)(?:\s+\[codegen id : \d+\])?\s*$", re.M)
    matches = list(pattern.finditer(plan))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(plan)
        blocks.append((m.group(2).strip(), plan[m.start() : end].strip()))
    return blocks


def _flatten_task_summary(summary: Optional[dict]) -> dict[str, list[float]]:
    if not summary:
        return {}
    out: dict[str, list[float]] = {}
    simple = [
        "duration",
        "executorRunTime",
        "executorCpuTime",
        "jvmGcTime",
        "schedulerDelay",
        "executorDeserializeTime",
        "gettingResultTime",
        "resultSize",
        "peakExecutionMemory",
        "memoryBytesSpilled",
        "diskBytesSpilled",
    ]
    for k in simple:
        if k in summary:
            out[k] = [float(x) for x in summary[k]]
    nested = {
        ("inputMetrics", "bytesRead"): "inputBytes",
        ("inputMetrics", "recordsRead"): "inputRecords",
        ("outputMetrics", "bytesWritten"): "outputBytes",
        ("outputMetrics", "recordsWritten"): "outputRecords",
        ("shuffleReadMetrics", "readBytes"): "shuffleReadBytes",
        ("shuffleReadMetrics", "readRecords"): "shuffleReadRecords",
        ("shuffleReadMetrics", "fetchWaitTime"): "shuffleFetchWaitTime",
        ("shuffleReadMetrics", "remoteBytesRead"): "shuffleRemoteBytesRead",
        ("shuffleWriteMetrics", "writeBytes"): "shuffleWriteBytes",
        ("shuffleWriteMetrics", "writeRecords"): "shuffleWriteRecords",
        ("shuffleWriteMetrics", "writeTime"): "shuffleWriteTime",
    }
    for (group, key), name in nested.items():
        vals = (summary.get(group) or {}).get(key)
        if vals:
            out[name] = [float(x) for x in vals]
    return out


def _task_sample(t: dict) -> TaskSample:
    m: dict[str, Any] = t.get("taskMetrics") or {}
    sr = m.get("shuffleReadMetrics") or {}
    inp = m.get("inputMetrics") or {}
    return TaskSample(
        task_id=int(t.get("taskId", 0)),
        index=int(t.get("index", 0)),
        attempt=int(t.get("attempt", 0)),
        executor_id=str(t.get("executorId", "")),
        host=t.get("host", ""),
        status=t.get("status", ""),
        duration_ms=t.get("duration"),
        run_time_ms=m.get("executorRunTime"),
        gc_time_ms=m.get("jvmGcTime"),
        input_bytes=inp.get("bytesRead"),
        input_records=inp.get("recordsRead"),
        shuffle_read_bytes=(sr.get("remoteBytesRead") or 0) + (sr.get("localBytesRead") or 0) if sr else None,
        shuffle_read_records=sr.get("recordsRead"),
        memory_spilled=m.get("memoryBytesSpilled"),
        disk_spilled=m.get("diskBytesSpilled"),
        locality=t.get("taskLocality", ""),
        speculative=bool(t.get("speculative", False)),
        error=(t.get("errorMessage") or "")[:1500],
    )
