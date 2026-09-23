"""Shared fixtures: a fake Spark REST server backed by recorded responses, and snapshot builders."""

from __future__ import annotations

import json
import os
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sparkmedic.model import Executor, Job, MetricValue, Snapshot, SourceInfo, SqlExecution, SqlNode, Stage

HERE = os.path.dirname(__file__)
REST_DIR = os.path.join(HERE, "fixtures", "rest")
EVENTLOG_DIR = os.path.join(HERE, "fixtures", "eventlog")
DEMO_APP = "local-1790140145275"


def _fixture_name(path_and_query: str) -> str:
    return path_and_query.replace("?", "__").replace("&", "_").replace("=", "-").replace(",", "_") + ".json"


class _Handler(BaseHTTPRequestHandler):
    seen_headers: list = []
    required_token: str | None = None

    def log_message(self, *args):  # silence
        pass

    def do_GET(self):  # noqa: N802
        type(self).seen_headers.append(dict(self.headers))
        if self.required_token and self.headers.get("Authorization") != f"Bearer {self.required_token}":
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"unauthorized")
            return
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body>Login</body></html>")
            return
        rel = parsed.path.split("/api/v1/", 1)[-1]
        query = urllib.parse.unquote(parsed.query)
        name = _fixture_name(rel + ("?" + query if query else ""))
        fp = os.path.join(REST_DIR, name)
        if not os.path.exists(fp):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"message": "not found"}')
            return
        with open(fp, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture()
def fake_spark_ui():
    _Handler.seen_headers = []
    _Handler.required_token = None
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}", _Handler
    server.shutdown()


# --------------------------------------------------------------------------- snapshot builders
def make_stage(stage_id: int, run_ms: float, **kw) -> Stage:
    st = Stage(stage_id=stage_id, name=kw.pop("name", f"stage {stage_id} at x.py:1"), status=kw.pop("status", "COMPLETE"))
    st.executor_run_time_ms = run_ms
    st.executor_cpu_time_ns = kw.pop("cpu_ratio", 0.9) * run_ms * 1e6
    st.submission_time_ms = 1_000
    st.first_task_launched_ms = 1_000
    st.completion_time_ms = 1_000 + kw.pop("wall_ms", run_ms / 8)
    for k, v in kw.items():
        setattr(st, k, v)
    return st


def quant(p0, p25, p50, p75, p100):
    return [float(p0), float(p25), float(p50), float(p75), float(p100)]


def make_snapshot(stages, sql=None, conf=None, executors=None, jobs=None, duration_ms=3_600_000) -> Snapshot:
    execs = executors or [
        Executor(executor_id="driver", total_cores=8, add_time_ms=0),
        Executor(executor_id="0", host="10.0.0.1", total_cores=32, add_time_ms=0),
        Executor(executor_id="1", host="10.0.0.2", total_cores=32, add_time_ms=0),
    ]
    if jobs is None:
        jobs = []
        for i, st in enumerate(stages):
            jobs.append(Job(job_id=i, stage_ids=[st.stage_id], submission_time_ms=0, completion_time_ms=duration_ms,
                            sql_execution_id=st.sql_execution_id))
            st.job_ids = [i]
    for e in sql or []:
        e.job_ids = [j.job_id for j in jobs if j.sql_execution_id == e.execution_id]
    return Snapshot(
        app_id="app-test", app_name="test", spark_version="4.0.0", source=SourceInfo("snapshot", "test"),
        start_time_ms=0, end_time_ms=duration_ms, completed=True, collected_at_ms=duration_ms,
        spark_conf=conf or {"spark.master": "spark://x:7077"}, executors=execs, jobs=jobs, stages=stages, sql=sql or [],
    )


def node(node_id, name, description="", stage_ids=(), children=(), **metrics) -> SqlNode:
    ms = {}
    for k, v in metrics.items():
        unit = "bytes" if "size" in k or "bytes" in k else "ms" if "time" in k else "count"
        ms[k.replace("_", " ")] = MetricValue(unit=unit, total=float(v), text=str(v))
    return SqlNode(node_id=node_id, name=name, description=description, stage_ids=list(stage_ids),
                   children=list(children), metrics=ms)


def sql_exec(eid, nodes, plan="", duration_ms=600_000, description="q") -> SqlExecution:
    return SqlExecution(execution_id=eid, description=description, status="COMPLETED", duration_ms=duration_ms,
                        plan_description=plan, nodes=nodes)


def load_json(name):
    with open(os.path.join(REST_DIR, name)) as fh:
        return json.load(fh)
