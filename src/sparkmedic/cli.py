"""Command-line interface: ``sparkmedic``."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from sparkmedic.analysis.engine import RULES, SEVERITIES, AnalysisConfig, Thresholds, run_analysis
from sparkmedic.collectors.rest import CollectOptions
from sparkmedic.collectors.targets import ConnectionOptions, Target
from sparkmedic.version import __version__

EPILOG = """\
TARGET examples:
  http://localhost:4040                 live Spark UI (driver)
  http://history-server:18080           Spark History Server
  databricks://0923-123456-abcd123      remote Databricks cluster (Databricks SDK auth)
  databricks://run/123456789            cluster used by a Databricks job run
  ./spark-events  or  app-logs.zip      offline event logs
  /Volumes/cat/sch/vol/cluster-logs/<cluster-id>/eventlog   delivered Databricks event logs
  snapshot.json                         a snapshot saved with 'sparkmedic collect'

Authentication (never pass secrets as arguments):
  Spark REST:  SPARKMEDIC_TOKEN (bearer) or SPARKMEDIC_USERNAME / SPARKMEDIC_PASSWORD
  Databricks:  standard Databricks unified auth (DATABRICKS_HOST + OAuth/PAT/Azure CLI, or --profile)
"""


def _conn(args) -> ConnectionOptions:
    return ConnectionOptions(
        timeout=args.timeout,
        verify_tls=not args.insecure,
        ca_bundle=args.ca_bundle,
        token_env=args.token_env,
        databricks_profile=args.profile,
        databricks_host=args.host,
        databricks_org_id=args.org_id,
        databricks_ui_port=args.ui_port,
        prefer_eventlogs=getattr(args, "prefer_eventlogs", False),
    )


def _progress(quiet: bool):
    if quiet:
        return lambda _m: None
    return lambda m: print(f"[sparkmedic] {m}", file=sys.stderr, flush=True)


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("target", help="Spark UI / History Server URL, databricks://..., notebook, event log path, or snapshot.json")
    p.add_argument("--app-id", help="application id (default: the running app, else the most recent)")
    g = p.add_argument_group("connection")
    g.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds (default 60)")
    g.add_argument("--insecure", action="store_true", help="disable TLS certificate verification (not recommended)")
    g.add_argument("--ca-bundle", help="path to a custom CA bundle for TLS")
    g.add_argument("--token-env", default="SPARKMEDIC_TOKEN", help="env var holding a bearer token (default SPARKMEDIC_TOKEN)")
    g.add_argument("--profile", help="Databricks config profile (~/.databrickscfg)")
    g.add_argument("--host", help="Databricks workspace URL (else from env/profile)")
    g.add_argument("--org-id", help="Databricks org/workspace id for the driver proxy (auto-detected)")
    g.add_argument("--ui-port", type=int, default=40001, help="Spark UI port on Databricks drivers (default 40001)")
    g.add_argument("--prefer-eventlogs", action="store_true", help="Databricks: read delivered event logs even if the cluster runs")
    p.add_argument("--top-stages", type=int, default=25, help="stages to fetch task distributions for (default 25)")
    p.add_argument("-q", "--quiet", action="store_true", help="no progress messages")


def _confirm(target: Target, app_id: Optional[str], args) -> Optional[str]:
    """Explicit confirmation step: show what will be analyzed and ask before collecting."""
    try:
        apps = target.list_applications(limit=20)
    except Exception as exc:  # noqa: BLE001
        print(f"Could not list applications: {exc}", file=sys.stderr)
        return app_id
    if not apps:
        return app_id
    chosen = next((a for a in apps if a["app_id"] == app_id), None) if app_id else None
    if chosen is None:
        running = [a for a in apps if not a.get("completed", True)]
        chosen = running[0] if running else apps[0]
    if args.yes and getattr(args, "quiet", False):
        return chosen["app_id"]
    print("\nsparkmedic will analyze:", file=sys.stderr)
    print(f"  target      : {target.raw}", file=sys.stderr)
    print(f"  application : {chosen['app_id']}  {chosen.get('name') or ''}", file=sys.stderr)
    if len(apps) > 1:
        print(f"  (other apps : {', '.join(a['app_id'] for a in apps[:6] if a is not chosen)})", file=sys.stderr)
    focus = []
    if args.focus_stage:
        focus.append(f"stages {args.focus_stage}")
    if args.focus_sql:
        focus.append(f"SQL queries {args.focus_sql}")
    print(f"  focus       : {', '.join(focus) if focus else 'whole application'}", file=sys.stderr)
    print("  access      : read-only GET requests / local file reads\n", file=sys.stderr)
    if args.yes or not sys.stdin.isatty():
        return chosen["app_id"]
    answer = input("Proceed? [Y/n/<other app id>] ").strip()
    if answer.lower() in ("n", "no"):
        print("Cancelled.", file=sys.stderr)
        sys.exit(1)
    if answer and answer.lower() not in ("y", "yes"):
        return answer
    return chosen["app_id"]


def cmd_apps(args) -> int:
    t = Target(args.target, _conn(args), _progress(args.quiet))
    apps = t.list_applications(limit=args.limit)
    if args.json:
        print(json.dumps(apps, indent=2, default=str))
        return 0
    if not apps:
        print("No applications found.")
    for a in apps:
        state = "completed" if a.get("completed") else "running"
        if a.get("kind"):
            state = a["kind"]
        print(f"{a['app_id']:<40} {state:<12} {a.get('name') or ''}")
    return 0


def _snapshot(args):
    t = Target(args.target, _conn(args), _progress(args.quiet))
    app_id = args.app_id
    if getattr(args, "confirm", True) and hasattr(args, "yes"):
        app_id = _confirm(t, app_id, args)
    opts = CollectOptions(top_stages=args.top_stages, stage_ids=args.focus_stage or None)
    return t.collect(app_id, opts)


def cmd_collect(args) -> int:
    snap = _snapshot(args)
    snap.save(args.output)
    print(f"Snapshot saved to {args.output} (secrets in Spark conf are redacted).")
    return 0


def cmd_analyze(args) -> int:
    snap = _snapshot(args)
    cfg = AnalysisConfig(
        thresholds=Thresholds(min_stage_share=args.min_stage_share),
        focus_stage_ids=args.focus_stage or None,
        focus_sql_ids=args.focus_sql or None,
        min_severity=args.min_severity,
        disabled_rules=args.disable or [],
    )
    diag = run_analysis(snap, cfg)
    formats = ("md", "json") if args.format == "both" else (args.format,)
    if args.out:
        paths = diag.save(args.out, formats=formats, snapshot=not args.no_snapshot)
        for kind, p in paths.items():
            print(f"{kind:>9}: {os.path.abspath(p)}", file=sys.stderr)
    if not args.out or args.print:
        print(diag.to_json() if args.format == "json" else diag.to_markdown())
    worst = max((SEVERITIES.index(f.severity) for f in diag.findings), default=-1)
    if args.fail_on and worst >= SEVERITIES.index(args.fail_on):
        return 2
    return 0


def cmd_rules(args) -> int:
    from sparkmedic.analysis import rules  # noqa: F401

    for rid, info in sorted(RULES.items()):
        print(f"{rid}  {info.name:<45} [{info.category}] {info.description}")
    return 0


def cmd_mcp(args) -> int:
    from sparkmedic.mcp_server import main as mcp_main

    mcp_main(["--transport", args.transport])
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sparkmedic",
        description="Find the root causes of slow Spark / Databricks jobs from the Spark UI REST API or event logs.",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    a = sub.add_parser("analyze", help="collect data and produce a diagnosis report", epilog=EPILOG,
                       formatter_class=argparse.RawDescriptionHelpFormatter)
    _add_common(a)
    a.add_argument("--focus-stage", type=int, action="append", help="only report on this stage id (repeatable)")
    a.add_argument("--focus-sql", type=int, action="append", help="only report on this SQL execution id (repeatable)")
    a.add_argument("--min-severity", choices=SEVERITIES, default="low")
    a.add_argument("--min-stage-share", type=float, default=0.05,
                   help="only analyze stages using at least this share of total task time (default 0.05)")
    a.add_argument("--disable", action="append", help="disable a rule id, e.g. SD403 (repeatable)")
    a.add_argument("--format", choices=["md", "json", "both"], default="md")
    a.add_argument("-o", "--out", help="directory to write the report(s) and snapshot to")
    a.add_argument("--print", action="store_true", help="also print the report when --out is used")
    a.add_argument("--no-snapshot", action="store_true", help="with --out, do not save the raw snapshot")
    a.add_argument("-y", "--yes", action="store_true", help="skip the confirmation prompt")
    a.add_argument("--fail-on", choices=SEVERITIES, help="exit with code 2 if a finding has this severity or higher (CI)")
    a.set_defaults(func=cmd_analyze)

    c = sub.add_parser("collect", help="save a redacted snapshot JSON for later/offline analysis")
    _add_common(c)
    c.add_argument("-o", "--output", default="spark-snapshot.json")
    c.add_argument("--focus-stage", type=int, action="append", help=argparse.SUPPRESS)
    c.add_argument("--focus-sql", type=int, action="append", help=argparse.SUPPRESS)
    c.add_argument("-y", "--yes", action="store_true", help="skip the confirmation prompt")
    c.set_defaults(func=cmd_collect)

    l_ = sub.add_parser("apps", help="list applications at a target")
    _add_common(l_)
    l_.add_argument("--limit", type=int, default=50)
    l_.add_argument("--json", action="store_true")
    l_.set_defaults(func=cmd_apps)

    r = sub.add_parser("rules", help="list all diagnosis rules")
    r.set_defaults(func=cmd_rules)

    m = sub.add_parser("mcp", help="run the MCP server (for Claude, Cursor and other MCP clients)")
    m.add_argument("--transport", choices=["stdio", "streamable-http"], default="stdio")
    m.set_defaults(func=cmd_mcp)
    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001
        if os.environ.get("SPARKMEDIC_DEBUG"):
            raise
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
