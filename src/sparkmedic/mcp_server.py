"""MCP server exposing sparkmedic to AI assistants (Claude, Cursor, VS Code, ...).

Run with ``sparkmedic-mcp`` (stdio) or ``sparkmedic mcp``. Works with the official
MCP Python SDK 1.x (``FastMCP``) and 2.x (``MCPServer``).

All tools are read-only. Credentials come from the server's environment
(SPARKMEDIC_TOKEN, Databricks unified auth), never from tool arguments.
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
from typing import Any, Optional

from sparkmedic.analysis.engine import RULES, AnalysisConfig, run_analysis
from sparkmedic.collectors.rest import CollectOptions
from sparkmedic.collectors.targets import ConnectionOptions, Target
from sparkmedic.model import Snapshot
from sparkmedic.units import fmt_bytes, fmt_ms, fmt_pct
from sparkmedic.version import __version__

INSTRUCTIONS = """\
sparkmedic diagnoses slow Apache Spark / Databricks applications from the Spark UI REST API
or Spark event logs, and returns ranked root causes with concrete fixes.

Workflow:
1. Call `plan_diagnosis` with the user's target (Spark UI URL, History Server URL,
   databricks://<cluster-id>, databricks://run/<run-id>, or an event log path).
2. Show the user which application and scope will be analyzed and CONFIRM with them before continuing
   (ask which app if several are listed, and whether a specific stage or query is the concern).
3. Call `diagnose` with the confirmed app_id (and focus stage / SQL ids if given).
4. Use `get_stage_details` / `get_sql_plan` on the returned snapshot_id to dig into specific evidence.
5. When proposing code changes, keep the user's results identical and suggest verifying on a sample.
Never ask the user to paste tokens; credentials are configured in the server environment.
"""


def _load_server_class():
    try:  # MCP Python SDK 2.x
        from mcp.server.mcpserver import MCPServer

        return MCPServer
    except ImportError:
        pass
    try:  # MCP Python SDK 1.x
        from mcp.server.fastmcp import FastMCP

        return FastMCP
    except ImportError as exc:
        raise SystemExit("The MCP server needs the MCP SDK: pip install 'sparkmedic[mcp]'") from exc


class _Cache:
    """In-memory snapshot cache (also persisted to a private temp dir for reuse)."""

    def __init__(self) -> None:
        self.snaps: dict[str, Snapshot] = {}
        self.dir = os.environ.get("SPARKMEDIC_CACHE_DIR") or os.path.join(tempfile.gettempdir(), "sparkmedic-mcp")
        os.makedirs(self.dir, mode=0o700, exist_ok=True)

    def put(self, snap: Snapshot) -> str:
        sid = f"{snap.app_id}-{int(time.time())}"
        self.snaps[sid] = snap
        try:
            snap.save(os.path.join(self.dir, f"{sid}.json"))
        except OSError:
            pass
        return sid

    def get(self, sid: str) -> Snapshot:
        if sid in self.snaps:
            return self.snaps[sid]
        path = os.path.join(self.dir, f"{os.path.basename(sid)}.json")
        if os.path.exists(path):
            self.snaps[sid] = Snapshot.load(path)
            return self.snaps[sid]
        raise ValueError(f"Unknown snapshot_id {sid!r}. Run diagnose first.")


CACHE = _Cache()


def _conn() -> ConnectionOptions:
    return ConnectionOptions(
        timeout=float(os.environ.get("SPARKMEDIC_TIMEOUT", "60")),
        verify_tls=os.environ.get("SPARKMEDIC_INSECURE", "").lower() not in ("1", "true", "yes"),
        ca_bundle=os.environ.get("SPARKMEDIC_CA_BUNDLE"),
        databricks_profile=os.environ.get("DATABRICKS_CONFIG_PROFILE"),
        databricks_org_id=os.environ.get("DATABRICKS_ORG_ID"),
    )


# ------------------------------------------------------------------ tool implementations (sync)
def list_applications_impl(target: str, limit: int = 20) -> dict[str, Any]:
    return {"target": target, "applications": Target(target, _conn()).list_applications(limit)}


def plan_diagnosis_impl(target: str, app_id: Optional[str] = None) -> dict[str, Any]:
    t = Target(target, _conn())
    apps = t.list_applications(limit=20)
    chosen = None
    if app_id:
        chosen = next((a for a in apps if a["app_id"] == app_id), None)
    if chosen is None and apps:
        running = [a for a in apps if not a.get("completed", True)]
        chosen = running[0] if running else apps[0]
    return {
        "target": target,
        "target_kind": t.kind,
        "will_analyze": chosen,
        "other_applications": [a for a in apps if a is not chosen][:10],
        "what_will_be_read": [
            "application environment (Spark conf, secrets redacted)",
            "executors, jobs, stages and task-metric distributions for the heaviest stages",
            "SQL executions with physical plans and operator metrics",
        ],
        "access": "read-only",
        "next_step": "Confirm the application and any focus (stage/query ids) with the user, then call diagnose.",
    }


def diagnose_impl(
    target: str,
    app_id: Optional[str] = None,
    focus_stage_ids: Optional[list[int]] = None,
    focus_sql_ids: Optional[list[int]] = None,
    min_severity: str = "low",
    top_stages: int = 25,
    include_markdown: bool = True,
) -> dict[str, Any]:
    t = Target(target, _conn())
    snap = t.collect(app_id, CollectOptions(top_stages=top_stages, stage_ids=focus_stage_ids))
    sid = CACHE.put(snap)
    diag = run_analysis(
        snap, AnalysisConfig(focus_stage_ids=focus_stage_ids, focus_sql_ids=focus_sql_ids, min_severity=min_severity)
    )
    out: dict[str, Any] = {"snapshot_id": sid, "report": diag.to_dict()}
    if include_markdown:
        out["markdown"] = diag.to_markdown()
    return out


def get_stage_details_impl(snapshot_id: str, stage_id: int, attempt_id: Optional[int] = None) -> dict[str, Any]:
    snap = CACHE.get(snapshot_id)
    st = snap.stage(stage_id, attempt_id)
    if st is None:
        raise ValueError(f"Stage {stage_id} not in snapshot")
    from dataclasses import asdict

    d = asdict(st)
    d["readable"] = {
        "task_time": fmt_ms(st.executor_run_time_ms),
        "wall_time": fmt_ms(st.wall_time_ms),
        "input": fmt_bytes(st.input_bytes),
        "shuffle_read": fmt_bytes(st.shuffle_read_bytes),
        "shuffle_write": fmt_bytes(st.shuffle_write_bytes),
        "disk_spill": fmt_bytes(st.disk_bytes_spilled),
        "gc_share": fmt_pct(st.jvm_gc_time_ms / st.executor_run_time_ms) if st.executor_run_time_ms else "n/a",
    }
    d["quantile_levels"] = [0.0, 0.25, 0.5, 0.75, 1.0]
    return d


def get_sql_plan_impl(snapshot_id: str, execution_id: int, max_chars: int = 30_000) -> dict[str, Any]:
    snap = CACHE.get(snapshot_id)
    e = snap.sql_execution(execution_id)
    if e is None:
        raise ValueError(f"SQL execution {execution_id} not in snapshot")
    nodes = [
        {
            "node_id": n.node_id,
            "name": n.name,
            "stage_ids": n.stage_ids,
            "metrics": {k: v.text or v.total for k, v in n.metrics.items()},
        }
        for n in e.nodes
    ]
    return {
        "execution_id": e.execution_id,
        "description": e.description,
        "status": e.status,
        "duration": fmt_ms(e.duration_ms),
        "job_ids": e.job_ids,
        "nodes": nodes,
        "plan_description": e.plan_description[:max_chars],
        "truncated": len(e.plan_description) > max_chars,
    }


def list_rules_impl() -> list[dict[str, str]]:
    from sparkmedic.analysis import rules  # noqa: F401

    return [{"rule_id": r.rule_id, "name": r.name, "category": r.category, "description": r.description} for r in RULES.values()]


# ------------------------------------------------------------------ server
def build_server():
    Server = _load_server_class()
    server = Server("sparkmedic", instructions=INSTRUCTIONS)

    try:
        import anyio

        async def run_sync(fn, *a, **kw):
            return await anyio.to_thread.run_sync(lambda: fn(*a, **kw))
    except ImportError:  # pragma: no cover
        async def run_sync(fn, *a, **kw):
            return fn(*a, **kw)

    @server.tool()
    async def list_applications(target: str, limit: int = 20) -> dict:
        """List Spark applications at a target (Spark UI/History Server URL, databricks://<cluster-id>,
        databricks://run/<run-id>, 'notebook', or an event log path)."""
        return await run_sync(list_applications_impl, target, limit)

    @server.tool()
    async def plan_diagnosis(target: str, app_id: Optional[str] = None) -> dict:
        """Resolve which application will be analyzed and what will be read. Call this first and confirm
        the application and scope with the user before calling diagnose."""
        return await run_sync(plan_diagnosis_impl, target, app_id)

    @server.tool()
    async def diagnose(
        target: str,
        app_id: Optional[str] = None,
        focus_stage_ids: Optional[list[int]] = None,
        focus_sql_ids: Optional[list[int]] = None,
        min_severity: str = "low",
        top_stages: int = 25,
        include_markdown: bool = True,
    ) -> dict:
        """Collect Spark UI data for one application and return ranked performance findings with evidence,
        root causes, and code/config/cluster fixes. Returns a snapshot_id for follow-up tools."""
        return await run_sync(
            diagnose_impl, target, app_id, focus_stage_ids, focus_sql_ids, min_severity, top_stages, include_markdown
        )

    @server.tool()
    async def get_stage_details(snapshot_id: str, stage_id: int, attempt_id: Optional[int] = None) -> dict:
        """Full metrics for one stage: totals, task-metric quantiles (min/25/50/75/max), slowest tasks, failures."""
        return await run_sync(get_stage_details_impl, snapshot_id, stage_id, attempt_id)

    @server.tool()
    async def get_sql_plan(snapshot_id: str, execution_id: int, max_chars: int = 30000) -> dict:
        """Physical plan and per-operator metrics for one SQL execution (the SQL / DataFrame tab)."""
        return await run_sync(get_sql_plan_impl, snapshot_id, execution_id, max_chars)

    @server.tool()
    async def list_rules() -> list:
        """List the diagnosis rules this server applies."""
        return list_rules_impl()

    @server.prompt()
    def diagnose_slow_spark_job(target: str) -> str:
        """Guided diagnosis of a slow Spark job."""
        return (
            f"Diagnose why the Spark application at {target} is slow. First call plan_diagnosis and confirm the "
            "application and scope with me. Then call diagnose, explain the top root causes in plain language, "
            "and propose fixes that keep results identical."
        )

    return server


def main(argv: Optional[list[str]] = None) -> None:
    p = argparse.ArgumentParser(prog="sparkmedic-mcp", description=f"sparkmedic MCP server {__version__}")
    p.add_argument("--transport", choices=["stdio", "streamable-http"], default="stdio")
    args = p.parse_args(argv)
    build_server().run(transport=args.transport)


if __name__ == "__main__":
    main()
