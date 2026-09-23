"""Normalized, source-independent snapshot of a Spark application.

Every collector (live REST API, History Server, Databricks, event logs) produces
the same :class:`Snapshot`, so the analysis rules never care where data came from.
Field names follow the Spark monitoring REST API where possible.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from typing import Any, Optional, get_args, get_origin, get_type_hints

SNAPSHOT_SCHEMA_VERSION = "1.0"

# Quantile levels used for every task-metric distribution.
QUANTILES = [0.0, 0.25, 0.5, 0.75, 1.0]

# Keys used in Stage.task_quantiles (flattened from the REST TaskMetricDistributions).
TASK_QUANTILE_KEYS = [
    "duration",
    "executorRunTime",
    "executorCpuTime",  # nanoseconds
    "jvmGcTime",
    "schedulerDelay",
    "executorDeserializeTime",
    "gettingResultTime",
    "resultSize",
    "peakExecutionMemory",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "inputBytes",
    "inputRecords",
    "outputBytes",
    "outputRecords",
    "shuffleReadBytes",
    "shuffleReadRecords",
    "shuffleFetchWaitTime",
    "shuffleRemoteBytesRead",
    "shuffleWriteBytes",
    "shuffleWriteRecords",
    "shuffleWriteTime",  # nanoseconds
]

_SECRET_KEY_RE = re.compile(
    r"(?i)(secret|password|passwd|token|credential|keytab|private[._-]?key|access[._-]?key|account[._-]?key|"
    r"client[._-]?key|\.key(\.|$)|sas|signature|auth)"
)
REDACTED = "*********(redacted)"


def redact_conf(conf: dict[str, str]) -> dict[str, str]:
    """Return a copy of a Spark conf with secret-looking values redacted."""
    out = {}
    for k, v in conf.items():
        if _SECRET_KEY_RE.search(k) or (isinstance(v, str) and v.startswith("{{secrets/")):
            out[k] = REDACTED
        else:
            out[k] = v
    return out


@dataclass
class MetricValue:
    unit: str = "count"  # bytes | ms | count
    total: Optional[float] = None
    min: Optional[float] = None
    med: Optional[float] = None
    max: Optional[float] = None
    text: str = ""


@dataclass
class SqlNode:
    node_id: int
    name: str
    description: str = ""
    codegen_id: Optional[int] = None
    metrics: dict[str, MetricValue] = field(default_factory=dict)
    children: list[int] = field(default_factory=list)  # node ids of direct inputs
    stage_ids: list[int] = field(default_factory=list)  # stages that updated this node's metrics

    def metric(self, *names: str) -> Optional[MetricValue]:
        for n in names:
            if n in self.metrics:
                return self.metrics[n]
        lowered = {k.lower(): v for k, v in self.metrics.items()}
        for n in names:
            if n.lower() in lowered:
                return lowered[n.lower()]
        return None

    def total(self, *names: str) -> Optional[float]:
        m = self.metric(*names)
        return m.total if m else None


@dataclass
class SqlExecution:
    execution_id: int
    description: str = ""
    status: str = ""
    submission_time_ms: Optional[int] = None
    duration_ms: Optional[float] = None
    job_ids: list[int] = field(default_factory=list)
    root_execution_id: Optional[int] = None
    plan_description: str = ""
    nodes: list[SqlNode] = field(default_factory=list)
    error_message: str = ""


@dataclass
class TaskSample:
    task_id: int
    index: int = 0
    attempt: int = 0
    executor_id: str = ""
    host: str = ""
    status: str = ""
    duration_ms: Optional[float] = None
    run_time_ms: Optional[float] = None
    gc_time_ms: Optional[float] = None
    input_bytes: Optional[float] = None
    input_records: Optional[float] = None
    shuffle_read_bytes: Optional[float] = None
    shuffle_read_records: Optional[float] = None
    memory_spilled: Optional[float] = None
    disk_spilled: Optional[float] = None
    locality: str = ""
    speculative: bool = False
    error: str = ""


@dataclass
class Stage:
    stage_id: int
    attempt_id: int = 0
    name: str = ""
    description: str = ""
    status: str = ""
    num_tasks: int = 0
    num_complete_tasks: int = 0
    num_failed_tasks: int = 0
    num_killed_tasks: int = 0
    submission_time_ms: Optional[int] = None
    first_task_launched_ms: Optional[int] = None
    completion_time_ms: Optional[int] = None
    failure_reason: str = ""
    executor_run_time_ms: float = 0.0
    executor_cpu_time_ns: float = 0.0
    executor_deserialize_time_ms: float = 0.0
    jvm_gc_time_ms: float = 0.0
    result_size: float = 0.0
    input_bytes: float = 0.0
    input_records: float = 0.0
    output_bytes: float = 0.0
    output_records: float = 0.0
    shuffle_read_bytes: float = 0.0
    shuffle_read_records: float = 0.0
    shuffle_remote_bytes_read: float = 0.0
    shuffle_fetch_wait_ms: float = 0.0
    shuffle_write_bytes: float = 0.0
    shuffle_write_records: float = 0.0
    shuffle_write_time_ns: float = 0.0
    memory_bytes_spilled: float = 0.0
    disk_bytes_spilled: float = 0.0
    peak_execution_memory: float = 0.0
    job_ids: list[int] = field(default_factory=list)
    sql_execution_id: Optional[int] = None
    rdd_names: list[str] = field(default_factory=list)
    task_quantiles: dict[str, list[float]] = field(default_factory=dict)
    top_tasks: list[TaskSample] = field(default_factory=list)
    failed_task_samples: list[TaskSample] = field(default_factory=list)
    executor_task_time_ms: dict[str, float] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.stage_id}.{self.attempt_id}"

    @property
    def wall_time_ms(self) -> Optional[float]:
        start = self.first_task_launched_ms or self.submission_time_ms
        if start is None or self.completion_time_ms is None:
            return None
        return max(0.0, float(self.completion_time_ms - start))

    def q(self, key: str, level: float) -> Optional[float]:
        values = self.task_quantiles.get(key)
        if not values:
            return None
        try:
            return values[QUANTILES.index(level)]
        except (ValueError, IndexError):
            return None


@dataclass
class Job:
    job_id: int
    name: str = ""
    status: str = ""
    submission_time_ms: Optional[int] = None
    completion_time_ms: Optional[int] = None
    stage_ids: list[int] = field(default_factory=list)
    num_tasks: int = 0
    num_failed_tasks: int = 0
    num_skipped_stages: int = 0
    sql_execution_id: Optional[int] = None


@dataclass
class Executor:
    executor_id: str
    host: str = ""
    is_active: bool = True
    total_cores: int = 0
    max_tasks: int = 0
    max_memory: float = 0.0
    total_duration_ms: float = 0.0
    total_gc_time_ms: float = 0.0
    total_input_bytes: float = 0.0
    total_shuffle_read: float = 0.0
    total_shuffle_write: float = 0.0
    completed_tasks: int = 0
    failed_tasks: int = 0
    add_time_ms: Optional[int] = None
    remove_time_ms: Optional[int] = None
    remove_reason: str = ""
    peak_memory: dict[str, float] = field(default_factory=dict)

    @property
    def is_driver(self) -> bool:
        return self.executor_id == "driver"


@dataclass
class SourceInfo:
    kind: str  # rest | databricks-remote | notebook | eventlog | snapshot
    target: str
    detail: str = ""


@dataclass
class Snapshot:
    app_id: str
    app_name: str = ""
    spark_version: str = ""
    source: SourceInfo = field(default_factory=lambda: SourceInfo(kind="unknown", target=""))
    start_time_ms: Optional[int] = None
    end_time_ms: Optional[int] = None
    completed: bool = False
    collected_at_ms: Optional[int] = None
    spark_conf: dict[str, str] = field(default_factory=dict)
    executors: list[Executor] = field(default_factory=list)
    jobs: list[Job] = field(default_factory=list)
    stages: list[Stage] = field(default_factory=list)
    sql: list[SqlExecution] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    schema_version: str = SNAPSHOT_SCHEMA_VERSION

    # ---- convenience -------------------------------------------------
    @property
    def duration_ms(self) -> Optional[float]:
        if self.start_time_ms is None:
            return None
        end = self.end_time_ms or self.collected_at_ms
        return float(end - self.start_time_ms) if end else None

    def stage(self, stage_id: int, attempt_id: Optional[int] = None) -> Optional[Stage]:
        matches = [s for s in self.stages if s.stage_id == stage_id]
        if attempt_id is not None:
            matches = [s for s in matches if s.attempt_id == attempt_id]
        return max(matches, key=lambda s: s.attempt_id) if matches else None

    def sql_execution(self, execution_id: int) -> Optional[SqlExecution]:
        return next((e for e in self.sql if e.execution_id == execution_id), None)

    def conf(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.spark_conf.get(key, default)

    @property
    def is_databricks(self) -> bool:
        return any(k.startswith("spark.databricks.") for k in self.spark_conf) or self.source.kind == "databricks-remote"

    # ---- serialization -----------------------------------------------
    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_dict(cls, data: dict) -> "Snapshot":
        return _from_dict(cls, data)

    @classmethod
    def load(cls, path: str) -> "Snapshot":
        with open(path, encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    def save(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_json(indent=1))


def _from_dict(tp: Any, data: Any) -> Any:
    """Minimal recursive dataclass loader (no third-party dependency)."""
    if data is None:
        return None
    if is_dataclass(tp):
        hints = get_type_hints(tp)
        kwargs = {}
        for f in fields(tp):
            if f.name in data:
                kwargs[f.name] = _from_dict(hints[f.name], data[f.name])
        return tp(**kwargs)
    origin = get_origin(tp)
    args = get_args(tp)
    if origin is list:
        return [_from_dict(args[0], x) for x in data]
    if origin is dict:
        return {k: _from_dict(args[1], v) for k, v in data.items()}
    if origin is not None and type(None) in args:  # Optional[X]
        inner = next(a for a in args if a is not type(None))
        return _from_dict(inner, data)
    return data
