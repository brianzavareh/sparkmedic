"""Rule engine: findings, configuration, registry and the Diagnosis result object."""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Optional

from sparkmedic.analysis.knowledge import DocRef
from sparkmedic.model import Snapshot

SEVERITIES = ["info", "low", "medium", "high", "critical"]
_SEV_WEIGHT = {"info": 0.1, "low": 0.4, "medium": 1.0, "high": 2.0, "critical": 3.0}


@dataclass
class Recommendation:
    kind: str  # code | config | cluster | table | practice
    title: str
    detail: str = ""
    snippet: str = ""
    language: str = ""  # python | sql | text


@dataclass
class Finding:
    rule_id: str
    title: str
    severity: str
    category: str
    summary: str
    evidence: list[str] = field(default_factory=list)
    root_cause: str = ""
    recommendations: list[Recommendation] = field(default_factory=list)
    references: list[DocRef] = field(default_factory=list)
    stage_ids: list[int] = field(default_factory=list)
    sql_ids: list[int] = field(default_factory=list)
    impact_share: Optional[float] = None  # share of total task time involved
    confidence: str = "medium"  # low | medium | high

    @property
    def score(self) -> float:
        share = self.impact_share if self.impact_share is not None else 0.2
        conf = {"low": 0.6, "medium": 0.85, "high": 1.0}.get(self.confidence, 0.85)
        return _SEV_WEIGHT.get(self.severity, 1.0) * (0.25 + share) * conf


@dataclass
class Thresholds:
    """All tunable heuristics in one place. Defaults follow Spark/Databricks guidance where it exists."""

    min_stage_share: float = 0.05  # stage must use >= 5% of total task time to be analyzed
    min_stage_task_time_ms: float = 10_000
    skew_max_over_p75: float = 1.5  # Databricks: max duration 50% above the 75th percentile
    skew_min_abs_gap_ms: float = 10_000
    spill_min_disk_bytes: float = 64 * 1024**2
    gc_warn_ratio: float = 0.10
    gc_high_ratio: float = 0.25
    io_bound_mb_per_core_s: float = 3.0  # Databricks guide: ~3 MB/s per core
    compute_cpu_ratio: float = 0.70
    fetch_wait_ratio: float = 0.20
    tiny_task_ms: float = 200  # Spark tuning guide: tasks >= 200 ms are efficient
    scheduler_overhead_ratio: float = 0.30
    large_partition_bytes: float = 1024**3
    small_file_bytes: float = 8 * 1024**2  # Databricks: files should be >= 8 MB
    small_file_min_count: int = 1_000
    broadcast_candidate_bytes: float = 200 * 1024**2
    explosion_ratio: float = 10.0
    explosion_min_rows: float = 10_000_000
    repeated_json_calls: int = 20
    repeated_scan_min: int = 3
    low_utilization: float = 0.30
    gap_min_ms: float = 60_000
    many_small_jobs: int = 200


@dataclass
class AnalysisConfig:
    thresholds: Thresholds = field(default_factory=Thresholds)
    focus_stage_ids: Optional[list[int]] = None
    focus_sql_ids: Optional[list[int]] = None
    min_severity: str = "low"
    disabled_rules: list[str] = field(default_factory=list)
    max_findings: int = 40


RuleFn = Callable[["AnalysisContext"], list[Finding]]  # noqa: F821


@dataclass
class RuleInfo:
    rule_id: str
    name: str
    category: str
    description: str
    fn: RuleFn


RULES: dict[str, RuleInfo] = {}


def rule(rule_id: str, name: str, category: str, description: str):
    def deco(fn: RuleFn) -> RuleFn:
        RULES[rule_id] = RuleInfo(rule_id, name, category, description, fn)
        return fn

    return deco


def severity_from_share(share: Optional[float], strong: bool = True) -> str:
    share = share or 0.0
    if share >= 0.5 and strong:
        return "critical"
    if share >= 0.25:
        return "high"
    if share >= 0.10:
        return "medium"
    return "low"


@dataclass
class Diagnosis:
    snapshot: Snapshot
    findings: list[Finding]
    overview: dict
    config: AnalysisConfig
    errors: list[str] = field(default_factory=list)
    generated_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        from sparkmedic.report.json_report import build_json

        return build_json(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_markdown(self) -> str:
        from sparkmedic.report.markdown import render_markdown

        return render_markdown(self)

    def save(self, out_dir: str, formats: tuple[str, ...] = ("md", "json"), snapshot: bool = True) -> dict[str, str]:
        os.makedirs(out_dir, exist_ok=True)
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in self.snapshot.app_id or "app")
        paths: dict[str, str] = {}
        if "md" in formats:
            paths["markdown"] = os.path.join(out_dir, f"sparkmedic-{safe}.md")
            with open(paths["markdown"], "w", encoding="utf-8") as fh:
                fh.write(self.to_markdown())
        if "json" in formats:
            paths["json"] = os.path.join(out_dir, f"sparkmedic-{safe}.json")
            with open(paths["json"], "w", encoding="utf-8") as fh:
                fh.write(self.to_json())
        if snapshot:
            paths["snapshot"] = os.path.join(out_dir, f"snapshot-{safe}.json")
            self.snapshot.save(paths["snapshot"])
        return paths

    def display(self) -> None:
        """Render in Jupyter / Databricks notebooks, print otherwise."""
        md = self.to_markdown()
        try:
            from IPython.display import Markdown, display  # type: ignore

            display(Markdown(md))
        except Exception:
            print(md)

    def summary_dict(self) -> list[dict]:
        return [
            {
                "rule_id": f.rule_id,
                "severity": f.severity,
                "title": f.title,
                "stage_ids": f.stage_ids,
                "sql_ids": f.sql_ids,
                "impact_share": f.impact_share,
            }
            for f in self.findings
        ]


def run_analysis(snapshot: Snapshot, config: AnalysisConfig) -> Diagnosis:
    from sparkmedic.analysis import rules  # noqa: F401  (registers rules)
    from sparkmedic.analysis.context import AnalysisContext

    ctx = AnalysisContext(snapshot, config)
    findings: list[Finding] = []
    errors: list[str] = []
    for rid, info in sorted(RULES.items()):
        if rid in config.disabled_rules:
            continue
        try:
            findings.extend(info.fn(ctx))
        except Exception as exc:  # a broken rule must never break the report
            errors.append(f"{rid} ({info.name}) failed: {type(exc).__name__}: {exc}")
    findings = _filter_focus(findings, config)
    min_idx = SEVERITIES.index(config.min_severity) if config.min_severity in SEVERITIES else 1
    findings = [f for f in findings if SEVERITIES.index(f.severity) >= min_idx]
    findings.sort(key=lambda f: f.score, reverse=True)
    findings = findings[: config.max_findings]
    return Diagnosis(snapshot=snapshot, findings=findings, overview=ctx.overview(), config=config, errors=errors)


def _filter_focus(findings: list[Finding], config: AnalysisConfig) -> list[Finding]:
    if not config.focus_stage_ids and not config.focus_sql_ids:
        return findings
    st = set(config.focus_stage_ids or [])
    sq = set(config.focus_sql_ids or [])
    out = []
    for f in findings:
        app_level = not f.stage_ids and not f.sql_ids
        if app_level or (st & set(f.stage_ids)) or (sq & set(f.sql_ids)):
            out.append(f)
    return out


def finding_to_dict(f: Finding) -> dict:
    d = asdict(f)
    d["score"] = round(f.score, 4)
    return d
