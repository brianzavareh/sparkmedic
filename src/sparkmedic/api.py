"""High-level Python API: collect -> analyze -> report."""

from __future__ import annotations

from typing import Callable, Optional, Sequence

from sparkmedic.analysis.engine import AnalysisConfig, Diagnosis, run_analysis
from sparkmedic.collectors.rest import CollectOptions
from sparkmedic.collectors.targets import ConnectionOptions, Target
from sparkmedic.model import Snapshot


def collect(
    target: str,
    app_id: Optional[str] = None,
    top_stages: int = 25,
    stage_ids: Optional[Sequence[int]] = None,
    connection: Optional[ConnectionOptions] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> Snapshot:
    """Collect a normalized snapshot of one Spark application from ``target``."""
    opts = CollectOptions(top_stages=top_stages, stage_ids=list(stage_ids) if stage_ids else None)
    return Target(target, connection, progress).collect(app_id, opts)


def analyze(
    snapshot: Snapshot,
    config: Optional[AnalysisConfig] = None,
) -> Diagnosis:
    """Run all diagnosis rules on a snapshot."""
    return run_analysis(snapshot, config or AnalysisConfig())


def diagnose(
    target: str = "notebook",
    app_id: Optional[str] = None,
    focus_stages: Optional[Sequence[int]] = None,
    focus_sql: Optional[Sequence[int]] = None,
    min_severity: str = "low",
    top_stages: int = 25,
    connection: Optional[ConnectionOptions] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> Diagnosis:
    """Collect and analyze in one call. Returns a :class:`Diagnosis`.

    ``diagnose("notebook").display()`` renders the report inside a Databricks notebook.
    """
    snap = collect(target, app_id, top_stages, focus_stages, connection, progress)
    cfg = AnalysisConfig(
        focus_stage_ids=list(focus_stages) if focus_stages else None,
        focus_sql_ids=list(focus_sql) if focus_sql else None,
        min_severity=min_severity,
    )
    return analyze(snap, cfg)
