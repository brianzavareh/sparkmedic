"""Machine-readable JSON report (stable, versioned schema)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sparkmedic.analysis.engine import RULES, finding_to_dict
from sparkmedic.analysis.knowledge import DOCS_REVIEWED
from sparkmedic.version import __version__

if TYPE_CHECKING:  # pragma: no cover
    from sparkmedic.analysis.engine import Diagnosis

REPORT_SCHEMA_VERSION = "1.0"


def build_json(d: "Diagnosis") -> dict:
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "tool": {"name": "sparkmedic", "version": __version__, "docs_reviewed": DOCS_REVIEWED},
        "generated_at_ms": d.generated_at_ms,
        "application": {
            "app_id": d.snapshot.app_id,
            "name": d.snapshot.app_name,
            "spark_version": d.snapshot.spark_version,
            "completed": d.snapshot.completed,
            "start_time_ms": d.snapshot.start_time_ms,
            "end_time_ms": d.snapshot.end_time_ms,
        },
        "overview": d.overview,
        "analysis": {
            "focus_stage_ids": d.config.focus_stage_ids,
            "focus_sql_ids": d.config.focus_sql_ids,
            "min_severity": d.config.min_severity,
            "rules_run": sorted(r for r in RULES if r not in d.config.disabled_rules),
        },
        "findings": [finding_to_dict(f) for f in d.findings],
        "notes": list(d.snapshot.notes),
        "errors": list(d.errors),
    }
