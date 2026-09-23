"""Derived metrics shared by all rules."""

from __future__ import annotations

import re
from functools import cached_property
from typing import Optional

from sparkmedic.analysis.engine import AnalysisConfig, Thresholds
from sparkmedic.model import Snapshot, SqlExecution, SqlNode, Stage
from sparkmedic.units import safe_div

_MEM_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgtp]?)b?\s*$", re.I)
_MEM_MULT = {"": 1024**2, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4, "p": 1024**5}


def parse_memory(value: Optional[str]) -> Optional[float]:
    """Parse JVM-style sizes (``4g``, ``4096m``). Bare numbers are MiB like spark.executor.memory."""
    if not value:
        return None
    m = _MEM_RE.match(str(value))
    if not m:
        return None
    return float(m.group(1)) * _MEM_MULT[m.group(2).lower()]


class AnalysisContext:
    def __init__(self, snapshot: Snapshot, config: AnalysisConfig):
        self.snap = snapshot
        self.cfg = config
        self.t: Thresholds = config.thresholds

    # -------------------------------------------------------------- totals
    @cached_property
    def total_task_time_ms(self) -> float:
        return sum(s.executor_run_time_ms for s in self.snap.stages) or 0.0

    @cached_property
    def workers(self):
        non_driver = [e for e in self.snap.executors if not e.is_driver]
        return non_driver or [e for e in self.snap.executors if e.is_driver]

    @cached_property
    def is_local_mode(self) -> bool:
        return all(e.is_driver for e in self.snap.executors) or (self.snap.conf("spark.master", "") or "").startswith(
            "local"
        )

    @cached_property
    def total_cores(self) -> int:
        """Peak number of concurrently available executor cores."""
        execs = self.workers
        if not execs:
            return 0
        events = []
        for e in execs:
            if e.add_time_ms is None:
                continue
            events.append((e.add_time_ms, e.total_cores))
            if e.remove_time_ms:
                events.append((e.remove_time_ms, -e.total_cores))
        if not events:
            return sum(e.total_cores for e in execs if e.is_active) or sum(e.total_cores for e in execs)
        cur = peak = 0
        for _, delta in sorted(events, key=lambda x: (x[0], x[1])):
            cur += delta
            peak = max(peak, cur)
        return peak or sum(e.total_cores for e in execs)

    @cached_property
    def num_workers(self) -> int:
        return len({e.host for e in self.workers if e.host}) or len(self.workers)

    @cached_property
    def executor_memory_bytes(self) -> Optional[float]:
        return parse_memory(self.snap.conf("spark.executor.memory"))

    @cached_property
    def cores_per_executor(self) -> Optional[float]:
        cores = [e.total_cores for e in self.workers if e.total_cores]
        return max(cores) if cores else None

    @cached_property
    def photon_nodes(self) -> int:
        return sum(1 for e in self.snap.sql for n in e.nodes if n.name.startswith("Photon"))

    @cached_property
    def photon_enabled(self) -> bool:
        engine = (self.snap.conf("sparkmedic.cluster.runtime_engine", "") or "").upper()
        version = self.snap.conf("spark.databricks.clusterUsageTags.sparkVersion", "") or ""
        return self.photon_nodes > 0 or engine == "PHOTON" or "photon" in version.lower()

    # -------------------------------------------------------------- stages
    def stage_share(self, st: Stage) -> float:
        return safe_div(st.executor_run_time_ms, self.total_task_time_ms) or 0.0

    def is_significant(self, st: Stage) -> bool:
        return (
            self.stage_share(st) >= self.t.min_stage_share
            and st.executor_run_time_ms >= self.t.min_stage_task_time_ms
        )

    @cached_property
    def significant_stages(self) -> list[Stage]:
        stages = [s for s in self.snap.stages if self.is_significant(s)]
        if self.cfg.focus_stage_ids:
            focus = [s for s in self.snap.stages if s.stage_id in self.cfg.focus_stage_ids]
            stages = list({s.key: s for s in stages + focus}.values())
        return sorted(stages, key=lambda s: s.executor_run_time_ms, reverse=True)

    def stage_wall_s(self, st: Stage) -> Optional[float]:
        w = st.wall_time_ms
        return w / 1000.0 if w else None

    def cpu_ratio(self, st: Stage) -> Optional[float]:
        return safe_div(st.executor_cpu_time_ns / 1e6, st.executor_run_time_ms)

    def gc_ratio(self, st: Stage) -> Optional[float]:
        return safe_div(st.jvm_gc_time_ms, st.executor_run_time_ms)

    def stage_input_bytes(self, st: Stage) -> float:
        """Input bytes; falls back to SQL scan metrics when the task metric is missing or tiny."""
        scan = sum(
            n.total("size of files read", "files size read", "size of files read (bytes)") or 0
            for n in self.stage_nodes(st)
            if "Scan" in n.name
        )
        return max(st.input_bytes, scan)

    def io_rate_mb_per_core_s(self, st: Stage) -> Optional[float]:
        """Databricks heuristic: largest I/O column / cores / stage duration."""
        wall = self.stage_wall_s(st)
        cores = min(self.total_cores or 0, st.num_tasks or 0) or self.total_cores
        if not wall or not cores:
            return None
        largest = max(self.stage_input_bytes(st), st.output_bytes, st.shuffle_read_bytes, st.shuffle_write_bytes)
        return largest / 1024**2 / cores / wall

    def rows_per_task_second(self, st: Stage) -> Optional[float]:
        rows = max(st.input_records, st.shuffle_read_records, st.output_records, st.shuffle_write_records)
        return safe_div(rows, st.executor_run_time_ms / 1000.0)

    # -------------------------------------------------------------- SQL
    @cached_property
    def sql_by_id(self) -> dict[int, SqlExecution]:
        return {e.execution_id: e for e in self.snap.sql}

    def sql_for_stage(self, st: Stage) -> Optional[SqlExecution]:
        if st.sql_execution_id is None:
            return None
        return self.sql_by_id.get(st.sql_execution_id)

    def stage_nodes(self, st: Stage) -> list[SqlNode]:
        """Plan nodes that ran in this stage (falls back to the whole plan when unknown)."""
        e = self.sql_for_stage(st)
        if not e:
            return []
        mapped = [n for n in e.nodes if st.stage_id in n.stage_ids]
        return mapped

    def stage_plan_text(self, st: Stage) -> tuple[str, bool]:
        """Text to scan for expensive expressions and whether it is stage-specific."""
        nodes = self.stage_nodes(st)
        if nodes:
            return "\n".join(f"{n.name}\n{n.description}" for n in nodes), True
        e = self.sql_for_stage(st)
        return (e.plan_description if e else ""), False

    def sql_share(self, e: SqlExecution) -> float:
        stage_ids = {sid for j in self.snap.jobs if j.job_id in e.job_ids for sid in j.stage_ids}
        t = sum(s.executor_run_time_ms for s in self.snap.stages if s.stage_id in stage_ids)
        return safe_div(t, self.total_task_time_ms) or 0.0

    def sql_stage_ids(self, e: SqlExecution) -> list[int]:
        ids = {sid for j in self.snap.jobs if j.job_id in e.job_ids for sid in j.stage_ids}
        present = {s.stage_id for s in self.snap.stages}
        return sorted(ids & present)

    def node_children(self, e: SqlExecution, node: SqlNode) -> list[SqlNode]:
        by_id = {n.node_id: n for n in e.nodes}
        return [by_id[c] for c in node.children if c in by_id]

    # -------------------------------------------------------------- overview
    def overview(self) -> dict:
        snap = self.snap
        dur = snap.duration_ms
        cores = self.total_cores
        util = safe_div(self.total_task_time_ms, (cores or 0) * (dur or 0)) if cores and dur else None
        top = sorted(snap.stages, key=lambda s: s.executor_run_time_ms, reverse=True)[:10]
        top_sql = sorted(snap.sql, key=lambda e: e.duration_ms or 0, reverse=True)[:5]
        return {
            "app_id": snap.app_id,
            "app_name": snap.app_name,
            "spark_version": snap.spark_version,
            "source": {"kind": snap.source.kind, "target": snap.source.target, "detail": snap.source.detail},
            "completed": snap.completed,
            "duration_ms": dur,
            "total_task_time_ms": self.total_task_time_ms,
            "executors": len(self.workers),
            "workers": self.num_workers,
            "total_cores": cores,
            "cluster_utilization": util,
            "is_databricks": snap.is_databricks,
            "photon": self.photon_enabled if snap.is_databricks else None,
            "local_mode": self.is_local_mode,
            "jobs": len(snap.jobs),
            "stages": len(snap.stages),
            "sql_executions": len(snap.sql),
            "failed_tasks": sum(s.num_failed_tasks for s in snap.stages),
            "cluster": {k.split("sparkmedic.cluster.", 1)[1]: v for k, v in snap.spark_conf.items() if k.startswith("sparkmedic.cluster.")},
            "top_stages": [
                {
                    "stage_id": s.stage_id,
                    "attempt_id": s.attempt_id,
                    "name": s.name,
                    "status": s.status,
                    "share": self.stage_share(s),
                    "task_time_ms": s.executor_run_time_ms,
                    "wall_time_ms": s.wall_time_ms,
                    "num_tasks": s.num_tasks,
                    "input_bytes": self.stage_input_bytes(s),
                    "shuffle_read_bytes": s.shuffle_read_bytes,
                    "shuffle_write_bytes": s.shuffle_write_bytes,
                    "spill_disk_bytes": s.disk_bytes_spilled,
                    "gc_ratio": self.gc_ratio(s),
                    "cpu_ratio": self.cpu_ratio(s),
                    "max_over_median": safe_div(s.q("duration", 1.0), s.q("duration", 0.5)),
                    "sql_execution_id": s.sql_execution_id,
                }
                for s in top
            ],
            "top_sql": [
                {
                    "execution_id": e.execution_id,
                    "description": e.description[:200],
                    "duration_ms": e.duration_ms,
                    "status": e.status,
                    "share": self.sql_share(e),
                }
                for e in top_sql
            ],
        }
