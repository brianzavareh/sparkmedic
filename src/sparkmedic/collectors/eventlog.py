"""Offline collector: replays Spark event logs into a :class:`Snapshot`.

Supported layouts
-----------------
* Single-file logs: ``<appId>``, ``<appId>.inprogress``, optionally ``.gz`` / ``.zstd`` compressed
* Rolling logs (Spark 3.0+): ``eventlog_v2_<appId>/events_<n>_<appId>[.zstd]``
* Databricks cluster log delivery: ``<dest>/<clusterId>/eventlog/<clusterId>_<ip>/<contextId>/``
  containing ``eventlog`` plus rolled ``eventlog-YYYY-MM-DD--HH-MM.gz`` files
* ``.zip`` archives downloaded from the REST endpoint ``/applications/<id>/logs``

Spark's LZ4/Snappy/LZF event-log framings are JVM-specific and not supported; use
``spark.eventLog.compression.codec=zstd`` or analyze through a History Server instead.
"""

from __future__ import annotations

import gzip
import heapq
import io
import json
import os
import re
import tempfile
import time
import zipfile
from array import array
from dataclasses import dataclass, field
from typing import IO, Callable, Iterator, Optional

from sparkmedic.model import (
    QUANTILES,
    TASK_QUANTILE_KEYS,
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
from sparkmedic.units import fmt_bytes, fmt_count, fmt_ms

_DBX_ROLLED_RE = re.compile(r"^eventlog-(\d{4}-\d{2}-\d{2}--\d{2}-\d{2})(\.\w+)?$")
_V2_FILE_RE = re.compile(r"^events_(\d+)_(.+?)(\.\w+)?$")
_UNSUPPORTED = (".lz4", ".snappy", ".lzf")
_SKIP_SUFFIXES = (".crc",)


class EventLogError(RuntimeError):
    pass


@dataclass
class EventLogRef:
    """One application's event log (possibly split into several rolled files)."""

    files: list[str]
    kind: str  # single | rolling-v2 | databricks
    app_id: str = ""
    app_name: str = ""
    start_ms: Optional[int] = None
    mtime: float = 0.0

    def as_dict(self) -> dict:
        return {
            "app_id": self.app_id,
            "name": self.app_name,
            "kind": self.kind,
            "files": self.files,
            "start_time_ms": self.start_ms,
        }


# ---------------------------------------------------------------------------- discovery
def discover(path: str, max_depth: int = 8) -> list[EventLogRef]:
    """Find event logs under ``path`` (file, directory or zip)."""
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        raise EventLogError(f"Path not found: {path}")
    if os.path.isfile(path) and path.lower().endswith(".zip"):
        tmp = tempfile.mkdtemp(prefix="sparkmedic-zip-")
        with zipfile.ZipFile(path) as zf:
            for member in zf.namelist():  # guard against zip-slip
                target = os.path.realpath(os.path.join(tmp, member))
                if not target.startswith(os.path.realpath(tmp) + os.sep):
                    raise EventLogError(f"Unsafe path in zip archive: {member}")
            zf.extractall(tmp)
        return discover(tmp, max_depth)
    refs: list[EventLogRef] = []
    if os.path.isfile(path):
        refs.append(EventLogRef(files=[path], kind="single", mtime=os.path.getmtime(path)))
    else:
        _walk(path, refs, max_depth)
    for r in refs:
        _peek_header(r)
    refs.sort(key=lambda r: (r.start_ms or 0, r.mtime), reverse=True)
    return refs


def _walk(directory: str, refs: list[EventLogRef], depth: int) -> None:
    if depth < 0:
        return
    try:
        entries = sorted(os.listdir(directory))
    except OSError:
        return
    base = os.path.basename(directory.rstrip(os.sep))
    files = [e for e in entries if os.path.isfile(os.path.join(directory, e)) and not e.startswith(".")]
    files = [f for f in files if not f.endswith(_SKIP_SUFFIXES)]

    if base.startswith("eventlog_v2_"):
        parts = []
        for f in files:
            m = _V2_FILE_RE.match(f)
            if m:
                parts.append((int(m.group(1)), os.path.join(directory, f)))
        if parts:
            parts.sort()
            paths = [p for _, p in parts]
            refs.append(
                EventLogRef(
                    files=paths,
                    kind="rolling-v2",
                    app_id=base[len("eventlog_v2_") :],
                    mtime=max(os.path.getmtime(p) for p in paths),
                )
            )
        return

    dbx_rolled = sorted((m.group(1), f) for f in files if (m := _DBX_ROLLED_RE.match(f)))
    if "eventlog" in files or dbx_rolled:
        # Rolled files carry UTC timestamps; the undated "eventlog" file is always the newest.
        ordered = [os.path.join(directory, f) for _, f in dbx_rolled]
        if "eventlog" in files:
            ordered.append(os.path.join(directory, "eventlog"))
        refs.append(
            EventLogRef(files=ordered, kind="databricks", mtime=max(os.path.getmtime(p) for p in ordered))
        )
        files = [f for f in files if f != "eventlog" and not _DBX_ROLLED_RE.match(f)]

    for f in files:
        if f.startswith("appstatus_"):
            continue
        full = os.path.join(directory, f)
        if _looks_like_single_log(full):
            refs.append(EventLogRef(files=[full], kind="single", mtime=os.path.getmtime(full)))

    for e in entries:
        full = os.path.join(directory, e)
        if os.path.isdir(full) and not e.startswith("."):
            _walk(full, refs, depth - 1)


def _looks_like_single_log(path: str) -> bool:
    name = os.path.basename(path)
    if re.match(r"^(app|local|application|spark|driver)[-_]", name) or name.startswith("eventlog"):
        return True
    try:
        with _open(path) as fh:
            first = fh.readline(4096)
        return first.startswith("{") and '"Event"' in first
    except Exception:
        return False


def _peek_header(ref: EventLogRef, max_lines: int = 200) -> None:
    try:
        with _open(ref.files[0]) as fh:
            for i, line in enumerate(fh):
                if i > max_lines:
                    break
                if '"SparkListenerApplicationStart"' in line:
                    ev = json.loads(line)
                    ref.app_id = ref.app_id or ev.get("App ID", "")
                    ref.app_name = ev.get("App Name", "")
                    ref.start_ms = ev.get("Timestamp")
                    break
    except Exception:
        pass
    if not ref.app_id:
        name = os.path.basename(ref.files[0])
        ref.app_id = re.sub(r"(\.inprogress)?(\.\w+)?$", "", name) if ref.kind == "single" else name


# ---------------------------------------------------------------------------- reading
def _open(path: str) -> IO[str]:
    lower = path.lower()
    if lower.endswith(_UNSUPPORTED) or lower.endswith(tuple(s + ".inprogress" for s in _UNSUPPORTED)):
        raise EventLogError(
            f"{os.path.basename(path)} uses a JVM-specific compression framing (lz4/snappy/lzf). "
            "Set spark.eventLog.compression.codec=zstd, or analyze the app through a Spark History Server URL."
        )
    stripped = lower[: -len(".inprogress")] if lower.endswith(".inprogress") else lower
    if stripped.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="replace")
    if stripped.endswith((".zstd", ".zst")):
        try:
            import zstandard
        except ImportError as exc:
            raise EventLogError("zstd event logs need: pip install 'sparkmedic[zstd]'") from exc
        raw = open(path, "rb")
        reader = zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True, closefd=True)
        return io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def iter_events(files: list[str]) -> Iterator[dict]:
    for path in files:
        with _open(path) as fh:
            try:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue  # truncated last line of an in-progress log
            except EOFError:
                continue  # truncated compressed stream (in-progress)


# ---------------------------------------------------------------------------- replay
@dataclass
class _StageAcc:
    stage: Stage
    values: dict[str, array] = field(default_factory=dict)
    top: list = field(default_factory=list)  # min-heap of (duration, task_id, TaskSample)
    failed: list[TaskSample] = field(default_factory=list)
    accum_values: dict[int, float] = field(default_factory=dict)


class EventLogReplayer:
    def __init__(self, top_tasks: int = 5):
        self.top_tasks = top_tasks
        self.app_id = ""
        self.app_name = ""
        self.spark_version = ""
        self.start_ms: Optional[int] = None
        self.end_ms: Optional[int] = None
        self.conf: dict[str, str] = {}
        self.executors: dict[str, Executor] = {}
        self.jobs: dict[int, Job] = {}
        self.stages: dict[tuple[int, int], _StageAcc] = {}
        self.stage_jobs: dict[int, set[int]] = {}
        self.sql: dict[int, SqlExecution] = {}
        self.sql_plans: dict[int, dict] = {}
        self.driver_accums: dict[int, float] = {}
        self.accum_totals: dict[int, float] = {}
        self.accum_stages: dict[int, set[int]] = {}
        self.last_ts: Optional[int] = None
        self.events = 0

    # dispatch ------------------------------------------------------------
    def handle(self, ev: dict) -> None:
        self.events += 1
        name = ev.get("Event", "")
        short = name.rsplit(".", 1)[-1]
        fn = getattr(self, "_on_" + short, None)
        if fn:
            fn(ev)

    def _ts(self, ts: Optional[int]) -> None:
        if ts:
            self.last_ts = max(self.last_ts or 0, int(ts))

    # app -------------------------------------------------------------------
    def _on_SparkListenerLogStart(self, ev: dict) -> None:
        self.spark_version = ev.get("Spark Version", "")

    def _on_SparkListenerApplicationStart(self, ev: dict) -> None:
        self.app_id = ev.get("App ID", "") or self.app_id
        self.app_name = ev.get("App Name", "")
        self.start_ms = ev.get("Timestamp")
        self._ts(self.start_ms)

    def _on_SparkListenerApplicationEnd(self, ev: dict) -> None:
        self.end_ms = ev.get("Timestamp")
        self._ts(self.end_ms)

    def _on_SparkListenerEnvironmentUpdate(self, ev: dict) -> None:
        props = ev.get("Spark Properties") or {}
        if isinstance(props, list):
            props = {k: v for k, v in props}
        self.conf.update({str(k): str(v) for k, v in props.items()})

    # executors -------------------------------------------------------------
    def _exec(self, eid: str) -> Executor:
        if eid not in self.executors:
            self.executors[eid] = Executor(executor_id=eid)
        return self.executors[eid]

    def _on_SparkListenerExecutorAdded(self, ev: dict) -> None:
        info = ev.get("Executor Info") or {}
        ex = self._exec(str(ev.get("Executor ID")))
        ex.host = info.get("Host", "")
        ex.total_cores = int(info.get("Total Cores") or 0)
        ex.max_tasks = ex.total_cores
        ex.add_time_ms = ev.get("Timestamp")
        ex.is_active = True
        self._ts(ev.get("Timestamp"))

    def _on_SparkListenerExecutorRemoved(self, ev: dict) -> None:
        ex = self._exec(str(ev.get("Executor ID")))
        ex.is_active = False
        ex.remove_time_ms = ev.get("Timestamp")
        ex.remove_reason = str(ev.get("Removed Reason", ""))[:1000]
        self._ts(ev.get("Timestamp"))

    def _on_SparkListenerBlockManagerAdded(self, ev: dict) -> None:
        bm = ev.get("Block Manager ID") or {}
        eid = str(bm.get("Executor ID", ""))
        if eid:
            ex = self._exec(eid)
            ex.max_memory = float(ev.get("Maximum Memory") or ex.max_memory)
            ex.host = ex.host or bm.get("Host", "")
            if eid == "driver" and not ex.add_time_ms:
                ex.add_time_ms = ev.get("Timestamp")

    def _on_SparkListenerStageExecutorMetrics(self, ev: dict) -> None:
        ex = self._exec(str(ev.get("Executor ID")))
        for k, v in (ev.get("Executor Metrics") or {}).items():
            ex.peak_memory[k] = max(ex.peak_memory.get(k, 0.0), float(v))

    # jobs ------------------------------------------------------------------
    def _on_SparkListenerJobStart(self, ev: dict) -> None:
        jid = int(ev.get("Job ID"))
        props = ev.get("Properties") or {}
        infos = ev.get("Stage Infos") or []
        stage_ids = list(ev.get("Stage IDs") or [s.get("Stage ID") for s in infos])
        name = props.get("callSite.short") or (infos[-1].get("Stage Name") if infos else "")
        sql_id = props.get("spark.sql.execution.id")
        job = Job(
            job_id=jid,
            name=name or "",
            status="RUNNING",
            submission_time_ms=ev.get("Submission Time"),
            stage_ids=stage_ids,
            num_tasks=sum(int(s.get("Number of Tasks") or 0) for s in infos),
            sql_execution_id=int(sql_id) if sql_id not in (None, "") else None,
        )
        self.jobs[jid] = job
        for sid in stage_ids:
            self.stage_jobs.setdefault(int(sid), set()).add(jid)
        self._ts(ev.get("Submission Time"))

    def _on_SparkListenerJobEnd(self, ev: dict) -> None:
        job = self.jobs.get(int(ev.get("Job ID")))
        if not job:
            return
        job.completion_time_ms = ev.get("Completion Time")
        result = (ev.get("Job Result") or {}).get("Result", "")
        job.status = "SUCCEEDED" if result == "JobSucceeded" else "FAILED"
        self._ts(ev.get("Completion Time"))

    # stages ----------------------------------------------------------------
    def _stage_acc(self, info: dict) -> _StageAcc:
        key = (int(info.get("Stage ID")), int(info.get("Stage Attempt ID") or 0))
        acc = self.stages.get(key)
        if acc is None:
            st = Stage(stage_id=key[0], attempt_id=key[1])
            acc = _StageAcc(stage=st, values={k: array("d") for k in TASK_QUANTILE_KEYS})
            self.stages[key] = acc
        st = acc.stage
        st.name = info.get("Stage Name", st.name)
        st.description = (info.get("Details") or st.description or "")[:4000]
        st.num_tasks = int(info.get("Number of Tasks") or st.num_tasks)
        st.submission_time_ms = info.get("Submission Time") or st.submission_time_ms
        st.completion_time_ms = info.get("Completion Time") or st.completion_time_ms
        rdds = info.get("RDD Info") or []
        if rdds and not st.rdd_names:
            st.rdd_names = [r.get("Name", "") for r in rdds][:20]
        return acc

    def _on_SparkListenerStageSubmitted(self, ev: dict) -> None:
        acc = self._stage_acc(ev.get("Stage Info") or {})
        acc.stage.status = "ACTIVE"
        props = ev.get("Properties") or {}
        sql_id = props.get("spark.sql.execution.id")
        if sql_id not in (None, ""):
            acc.stage.sql_execution_id = int(sql_id)
        self._ts(acc.stage.submission_time_ms)

    def _on_SparkListenerStageCompleted(self, ev: dict) -> None:
        info = ev.get("Stage Info") or {}
        acc = self._stage_acc(info)
        reason = info.get("Failure Reason")
        acc.stage.status = "FAILED" if reason else "COMPLETE"
        acc.stage.failure_reason = (reason or "")[:2000]
        for a in info.get("Accumulables") or []:
            try:
                aid = int(a.get("ID"))
                val = float(a.get("Value"))
            except (TypeError, ValueError):
                continue
            acc.accum_values[aid] = val
            if a.get("Metadata") == "sql":
                self.accum_stages.setdefault(aid, set()).add(acc.stage.stage_id)
        self._ts(acc.stage.completion_time_ms)

    def _on_SparkListenerTaskEnd(self, ev: dict) -> None:
        key = (int(ev.get("Stage ID")), int(ev.get("Stage Attempt ID") or 0))
        acc = self.stages.get(key)
        if acc is None:
            acc = self._stage_acc({"Stage ID": key[0], "Stage Attempt ID": key[1]})
        st = acc.stage
        info = ev.get("Task Info") or {}
        m = ev.get("Task Metrics") or {}
        reason = ev.get("Task End Reason") or {}
        ok = reason.get("Reason") == "Success"
        killed = reason.get("Reason") == "TaskKilled" or info.get("Killed")
        launch, finish = info.get("Launch Time") or 0, info.get("Finish Time") or 0
        duration = float(max(0, finish - launch)) if finish else 0.0
        self._ts(finish)

        sr = m.get("Shuffle Read Metrics") or {}
        sw = m.get("Shuffle Write Metrics") or {}
        im = m.get("Input Metrics") or {}
        om = m.get("Output Metrics") or {}
        run = float(m.get("Executor Run Time") or 0)
        deser = float(m.get("Executor Deserialize Time") or 0)
        ser = float(m.get("Result Serialization Time") or 0)
        grt_start = info.get("Getting Result Time") or 0
        getting = float(finish - grt_start) if grt_start and finish else 0.0
        vals = {
            "duration": duration,
            "executorRunTime": run,
            "executorCpuTime": float(m.get("Executor CPU Time") or 0),
            "jvmGcTime": float(m.get("JVM GC Time") or 0),
            "schedulerDelay": max(0.0, duration - run - deser - ser - getting),
            "executorDeserializeTime": deser,
            "gettingResultTime": getting,
            "resultSize": float(m.get("Result Size") or 0),
            "peakExecutionMemory": float(m.get("Peak Execution Memory") or 0),
            "memoryBytesSpilled": float(m.get("Memory Bytes Spilled") or 0),
            "diskBytesSpilled": float(m.get("Disk Bytes Spilled") or 0),
            "inputBytes": float(im.get("Bytes Read") or 0),
            "inputRecords": float(im.get("Records Read") or 0),
            "outputBytes": float(om.get("Bytes Written") or 0),
            "outputRecords": float(om.get("Records Written") or 0),
            "shuffleReadBytes": float((sr.get("Remote Bytes Read") or 0) + (sr.get("Local Bytes Read") or 0)),
            "shuffleReadRecords": float(sr.get("Total Records Read") or 0),
            "shuffleFetchWaitTime": float(sr.get("Fetch Wait Time") or 0),
            "shuffleRemoteBytesRead": float(sr.get("Remote Bytes Read") or 0),
            "shuffleWriteBytes": float(sw.get("Shuffle Bytes Written") or 0),
            "shuffleWriteRecords": float(sw.get("Shuffle Records Written") or 0),
            "shuffleWriteTime": float(sw.get("Shuffle Write Time") or 0),
        }
        # stage totals (all attempts of tasks, like the REST API)
        st.executor_run_time_ms += run
        st.executor_cpu_time_ns += vals["executorCpuTime"]
        st.executor_deserialize_time_ms += deser
        st.jvm_gc_time_ms += vals["jvmGcTime"]
        st.result_size += vals["resultSize"]
        st.input_bytes += vals["inputBytes"]
        st.input_records += vals["inputRecords"]
        st.output_bytes += vals["outputBytes"]
        st.output_records += vals["outputRecords"]
        st.shuffle_read_bytes += vals["shuffleReadBytes"]
        st.shuffle_read_records += vals["shuffleReadRecords"]
        st.shuffle_remote_bytes_read += vals["shuffleRemoteBytesRead"]
        st.shuffle_fetch_wait_ms += vals["shuffleFetchWaitTime"]
        st.shuffle_write_bytes += vals["shuffleWriteBytes"]
        st.shuffle_write_records += vals["shuffleWriteRecords"]
        st.shuffle_write_time_ns += vals["shuffleWriteTime"]
        st.memory_bytes_spilled += vals["memoryBytesSpilled"]
        st.disk_bytes_spilled += vals["diskBytesSpilled"]
        st.peak_execution_memory = max(st.peak_execution_memory, vals["peakExecutionMemory"])
        if st.first_task_launched_ms is None or (launch and launch < st.first_task_launched_ms):
            st.first_task_launched_ms = launch or st.first_task_launched_ms

        eid = str(info.get("Executor ID", ""))
        ex = self._exec(eid)
        ex.host = ex.host or info.get("Host", "")
        ex.total_duration_ms += duration
        ex.total_gc_time_ms += vals["jvmGcTime"]
        ex.total_input_bytes += vals["inputBytes"]
        ex.total_shuffle_read += vals["shuffleReadBytes"]
        ex.total_shuffle_write += vals["shuffleWriteBytes"]
        st.executor_task_time_ms[eid] = st.executor_task_time_ms.get(eid, 0.0) + duration
        for k, v in (ev.get("Task Executor Metrics") or {}).items():
            try:
                ex.peak_memory[k] = max(ex.peak_memory.get(k, 0.0), float(v))
            except (TypeError, ValueError):
                pass

        sample = TaskSample(
            task_id=int(info.get("Task ID", 0)),
            index=int(info.get("Index", 0)),
            attempt=int(info.get("Attempt", 0)),
            executor_id=eid,
            host=info.get("Host", ""),
            status="SUCCESS" if ok else ("KILLED" if killed else "FAILED"),
            duration_ms=duration,
            run_time_ms=run,
            gc_time_ms=vals["jvmGcTime"],
            input_bytes=vals["inputBytes"],
            input_records=vals["inputRecords"],
            shuffle_read_bytes=vals["shuffleReadBytes"],
            shuffle_read_records=vals["shuffleReadRecords"],
            memory_spilled=vals["memoryBytesSpilled"],
            disk_spilled=vals["diskBytesSpilled"],
            locality=info.get("Locality", ""),
            speculative=bool(info.get("Speculative", False)),
        )
        if ok:
            st.num_complete_tasks += 1
            ex.completed_tasks += 1
            for k, v in vals.items():
                acc.values[k].append(v)
            item = (duration, sample.task_id, sample)
            if len(acc.top) < self.top_tasks:
                heapq.heappush(acc.top, item)
            elif duration > acc.top[0][0]:
                heapq.heapreplace(acc.top, item)
        elif killed:
            st.num_killed_tasks += 1
        else:
            st.num_failed_tasks += 1
            ex.failed_tasks += 1
            if len(acc.failed) < 5:
                sample.error = _reason_text(reason)
                acc.failed.append(sample)

        for a in info.get("Accumulables") or []:
            if a.get("Metadata") == "sql" and a.get("Update") is not None:
                try:
                    aid = int(a["ID"])
                    self.accum_totals[aid] = self.accum_totals.get(aid, 0.0) + float(a["Update"])
                    self.accum_stages.setdefault(aid, set()).add(st.stage_id)
                except (TypeError, ValueError, KeyError):
                    pass

    # SQL -------------------------------------------------------------------
    def _on_SparkListenerSQLExecutionStart(self, ev: dict) -> None:
        eid = int(ev.get("executionId"))
        self.sql[eid] = SqlExecution(
            execution_id=eid,
            description=(ev.get("description") or "")[:2000],
            status="RUNNING",
            submission_time_ms=ev.get("time"),
            root_execution_id=ev.get("rootExecutionId"),
            plan_description=(ev.get("physicalPlanDescription") or "")[:400_000],
        )
        self.sql_plans[eid] = ev.get("sparkPlanInfo") or {}
        self._ts(ev.get("time"))

    def _on_SparkListenerSQLAdaptiveExecutionUpdate(self, ev: dict) -> None:
        eid = int(ev.get("executionId"))
        if ev.get("sparkPlanInfo"):
            self.sql_plans[eid] = ev["sparkPlanInfo"]
        if eid in self.sql and ev.get("physicalPlanDescription"):
            self.sql[eid].plan_description = ev["physicalPlanDescription"][:400_000]

    def _on_SparkListenerSQLExecutionEnd(self, ev: dict) -> None:
        e = self.sql.get(int(ev.get("executionId")))
        if not e:
            return
        end = ev.get("time")
        if end and e.submission_time_ms:
            e.duration_ms = float(end - e.submission_time_ms)
        err = ev.get("errorMessage")
        e.status = "FAILED" if err else "COMPLETED"
        e.error_message = (err or "")[:2000]
        self._ts(end)

    def _on_SparkListenerDriverAccumUpdates(self, ev: dict) -> None:
        for pair in ev.get("accumUpdates") or []:
            try:
                self.driver_accums[int(pair[0])] = float(pair[1])
            except (TypeError, ValueError, IndexError):
                continue

    # build -----------------------------------------------------------------
    def build(self, source: SourceInfo) -> Snapshot:
        stages: list[Stage] = []
        stage_accum: dict[int, float] = {}
        for acc in self.stages.values():
            st = acc.stage
            st.job_ids = sorted(self.stage_jobs.get(st.stage_id, set()))
            if st.sql_execution_id is None:
                st.sql_execution_id = next(
                    (self.jobs[j].sql_execution_id for j in st.job_ids if j in self.jobs and self.jobs[j].sql_execution_id is not None),
                    None,
                )
            if st.status in ("", "ACTIVE") and self.end_ms:
                st.status = st.status or "PENDING"
            st.task_quantiles = {k: _quantiles(v) for k, v in acc.values.items() if len(v)}
            st.top_tasks = [s for _, _, s in sorted(acc.top, key=lambda x: -x[0])]
            st.failed_task_samples = acc.failed
            # StageInfo accumulables carry the driver-side cumulative value, so take the max
            # (summing would double count metrics updated by several stages).
            for aid, v in acc.accum_values.items():
                stage_accum[aid] = max(stage_accum.get(aid, 0.0), v)
            stages.append(st)
        stages.sort(key=lambda s: (s.stage_id, s.attempt_id))

        # SQL metric totals: completed stage accumulables > task updates, plus driver-side updates.
        totals = dict(self.accum_totals)
        totals.update(stage_accum)
        totals.update(self.driver_accums)
        for eid, e in self.sql.items():
            e.nodes = _flatten_plan(self.sql_plans.get(eid) or {}, totals, self.accum_stages)
            e.job_ids = sorted(j.job_id for j in self.jobs.values() if j.sql_execution_id == eid)
            if e.status == "RUNNING" and self.end_ms:
                e.status = "UNKNOWN"
        _attach_descriptions(self.sql.values())

        for job in self.jobs.values():
            job.num_failed_tasks = sum(
                acc.stage.num_failed_tasks for (sid, _), acc in self.stages.items() if sid in job.stage_ids
            )
            submitted = {sid for (sid, _) in self.stages}
            job.num_skipped_stages = sum(1 for sid in job.stage_ids if sid not in submitted)

        return Snapshot(
            app_id=self.app_id,
            app_name=self.app_name,
            spark_version=self.spark_version or self.conf.get("spark.version", ""),
            source=source,
            start_time_ms=self.start_ms,
            end_time_ms=self.end_ms,
            completed=self.end_ms is not None,
            collected_at_ms=self.end_ms or self.last_ts or int(time.time() * 1000),
            spark_conf=redact_conf(self.conf),
            executors=list(self.executors.values()),
            jobs=sorted(self.jobs.values(), key=lambda j: j.job_id),
            stages=stages,
            sql=sorted(self.sql.values(), key=lambda e: e.execution_id),
        )


def _reason_text(reason: dict) -> str:
    parts = [reason.get("Reason", "")]
    for k in ("Class Name", "Description", "Loss Reason", "Message", "Kill Reason"):
        if reason.get(k):
            parts.append(str(reason[k]))
    return ": ".join(p for p in parts if p)[:1500]


def _quantiles(values: array) -> list[float]:
    """Same indexing rule as Spark's AppStatusStore task summaries."""
    data = sorted(values)
    n = len(data)
    return [data[min(int(q * n), n - 1)] for q in QUANTILES]


_METRIC_UNITS = {"size": "bytes", "timing": "ms", "nsTiming": "ms", "sum": "count", "average": "count"}


def _flatten_plan(root: dict, totals: dict[int, float], accum_stages: Optional[dict[int, set[int]]] = None) -> list[SqlNode]:
    from sparkmedic.collectors.rest import propagate_codegen_stages

    accum_stages = accum_stages or {}
    nodes: list[SqlNode] = []
    counter = [0]

    def visit(p: dict, codegen: Optional[int], parent: Optional[SqlNode]) -> None:
        name = p.get("nodeName", "")
        cg = codegen
        m = re.match(r"^WholeStageCodegen \((\d+)\)", name)
        if name == "InputAdapter":
            cg = None
        elif m:
            cg = int(m.group(1))
        node: Optional[SqlNode] = None
        if name != "InputAdapter":
            metrics = {}
            stage_ids: set[int] = set()
            for md in p.get("metrics") or []:
                stage_ids.update(accum_stages.get(int(md.get("accumulatorId", -1)), set()))
                unit = _METRIC_UNITS.get(md.get("metricType", "sum"), "count")
                raw = totals.get(int(md.get("accumulatorId", -1)))
                total = None
                if raw is not None:
                    total = raw / 1e6 if md.get("metricType") == "nsTiming" else raw
                text = (
                    fmt_bytes(total) if unit == "bytes" else fmt_ms(total) if unit == "ms" else fmt_count(total)
                ) if total is not None else ""
                metrics[md.get("name", "")] = MetricValue(unit=unit, total=total, text=text)
            node = SqlNode(
                node_id=counter[0],
                name=name,
                description=(p.get("simpleString") or "")[:20_000],
                codegen_id=None if m else codegen,
                metrics=metrics,
                stage_ids=sorted(stage_ids),
            )
            nodes.append(node)
            counter[0] += 1
            if parent is not None:
                parent.children.append(node.node_id)
        for child in p.get("children") or []:
            visit(child, cg, node if node is not None else parent)

    if root:
        visit(root, None, None)
    propagate_codegen_stages(nodes)
    return nodes


def _attach_descriptions(executions) -> None:
    from sparkmedic.collectors.rest import _attach_node_descriptions

    for e in executions:
        # Prefer the detailed formatted-plan block when available; keep simpleString otherwise.
        simple = {id(n): n.description for n in e.nodes}
        _attach_node_descriptions(e.nodes, e.plan_description)
        for n in e.nodes:
            if not n.description:
                n.description = simple[id(n)]


# ---------------------------------------------------------------------------- public API
class EventLogCollector:
    def __init__(self, path: str, progress: Optional[Callable[[str], None]] = None, top_tasks: int = 5):
        self.path = path
        self.progress = progress or (lambda _m: None)
        self.top_tasks = top_tasks

    def list_applications(self) -> list[EventLogRef]:
        return discover(self.path)

    def collect(self, app_id: Optional[str] = None) -> Snapshot:
        refs = discover(self.path)
        if not refs:
            raise EventLogError(f"No Spark event logs found under {self.path}")
        ref = refs[0]
        if app_id:
            match = [r for r in refs if r.app_id == app_id]
            if not match:
                raise EventLogError(f"App {app_id!r} not found. Found: {[r.app_id for r in refs[:10]]}")
            ref = match[0]
        self.progress(f"Replaying {len(ref.files)} event log file(s) for {ref.app_id or 'application'}")
        rep = EventLogReplayer(top_tasks=self.top_tasks)
        for ev in iter_events(ref.files):
            rep.handle(ev)
        if rep.events == 0:
            raise EventLogError(f"No events could be read from {ref.files}")
        snap = rep.build(SourceInfo(kind="eventlog", target=self.path, detail=f"{ref.kind}: {len(ref.files)} file(s)"))
        if len(refs) > 1 and not app_id:
            others = ", ".join(r.app_id for r in refs[1:6])
            snap.notes.append(
                f"Found {len(refs)} applications; analyzed the most recent ({snap.app_id}). Others: {others}. "
                "Use --app-id to choose."
            )
        if not snap.completed:
            snap.notes.append("The event log has no ApplicationEnd event (still running or truncated).")
        return snap
