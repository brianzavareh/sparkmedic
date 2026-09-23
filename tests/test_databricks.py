"""End-to-end tests of the Databricks paths with the real Databricks SDK against a fake workspace."""

from __future__ import annotations

import json
import os
import shutil
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("databricks.sdk")

from sparkmedic.collectors.targets import ConnectionOptions, Target, parse_databricks_target  # noqa: E402

from .conftest import DEMO_APP, EVENTLOG_DIR, REST_DIR, _fixture_name  # noqa: E402

TOKEN = "dapi-test-token"
CLUSTER = "0923-111111-abcdefgh"


class FakeWorkspace(BaseHTTPRequestHandler):
    cluster_state = "RUNNING"
    volume_root = ""  # local dir mirroring /Volumes/main/ops/logs
    proxy_orgs_seen: list = []

    def log_message(self, *a):
        pass

    def _json(self, obj, status=200, headers=None):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _local(self, vol_path: str) -> str:
        rel = vol_path[len("/Volumes/main/ops/logs") :].lstrip("/")
        return os.path.join(self.volume_root, rel)

    def do_HEAD(self):  # noqa: N802
        self.do_GET()

    def do_GET(self):  # noqa: N802
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._json({"error_code": "UNAUTHENTICATED"}, 401)
        u = urllib.parse.urlsplit(self.path)
        q = dict(urllib.parse.parse_qsl(u.query))
        path = urllib.parse.unquote(u.path)
        if path == "/api/2.1/clusters/get":
            conf = {"cluster_id": q["cluster_id"], "cluster_name": "etl", "state": self.cluster_state,
                    "node_type_id": "Standard_D32ds_v5", "driver_node_type_id": "Standard_D32ds_v5",
                    "num_workers": 2, "spark_version": "17.3.x-scala2.13", "runtime_engine": "STANDARD",
                    "cluster_log_conf": {"volumes": {"destination": "/Volumes/main/ops/logs"}}}
            return self._json(conf)
        if path == "/api/2.2/jobs/runs/get":
            return self._json({"run_id": int(q["run_id"]), "tasks": [
                {"task_key": "merge", "cluster_instance": {"cluster_id": CLUSTER, "spark_context_id": "555"}}]})
        if path == "/api/2.0/preview/scim/v2/Me":
            return self._json({"userName": "me"}, headers={"X-Databricks-Org-Id": "1234"})
        if path.startswith("/driver-proxy-api/o/"):
            parts = path.split("/")
            org, cluster, port = parts[3], parts[4], parts[5]
            type(self).proxy_orgs_seen.append(org)
            if org != "1234" or cluster != CLUSTER or port != "40001":
                return self._json({"error": "bad proxy path"}, 404)
            rel = path.split("/api/v1/", 1)[1]
            fp = os.path.join(REST_DIR, _fixture_name(rel + ("?" + urllib.parse.unquote(u.query) if u.query else "")))
            if not os.path.exists(fp):
                return self._json({"message": "nf"}, 404)
            with open(fp) as fh:
                return self._json(json.load(fh))
        if path.startswith("/api/2.0/fs/directories/Volumes/"):
            local = self._local(path[len("/api/2.0/fs/directories") :])
            if not os.path.isdir(local):
                return self._json({"error_code": "NOT_FOUND"}, 404)
            vol = path[len("/api/2.0/fs/directories") :].rstrip("/")
            contents = [{"path": f"{vol}/{n}", "name": n, "is_directory": os.path.isdir(os.path.join(local, n)),
                         "file_size": os.path.getsize(os.path.join(local, n))} for n in sorted(os.listdir(local))]
            return self._json({"contents": contents})
        if path.startswith("/api/2.0/fs/files/Volumes/"):
            local = self._local(path[len("/api/2.0/fs/files") :])
            if not os.path.isfile(local):
                return self._json({"error_code": "NOT_FOUND"}, 404)
            with open(local, "rb") as fh:
                data = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)
            return None
        return self._json({"error_code": "ENDPOINT_NOT_FOUND"}, 404)


@pytest.fixture()
def workspace(monkeypatch, tmp_path):
    FakeWorkspace.cluster_state = "RUNNING"
    FakeWorkspace.proxy_orgs_seen = []
    FakeWorkspace.volume_root = str(tmp_path / "vol")
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeWorkspace)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host = f"http://127.0.0.1:{server.server_address[1]}"
    for k in list(os.environ):
        if k.startswith("DATABRICKS_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("DATABRICKS_HOST", host)
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    monkeypatch.setenv("HOME", str(tmp_path))  # no ~/.databrickscfg
    yield host
    server.shutdown()


def test_parse_databricks_target():
    assert parse_databricks_target("databricks://0923-abc") == {"cluster_id": "0923-abc"}
    assert parse_databricks_target("databricks://cluster/0923-abc") == {"cluster_id": "0923-abc"}
    assert parse_databricks_target("databricks://run/42?task=merge") == {"run_id": 42, "task_key": "merge"}


def test_running_cluster_via_driver_proxy(workspace):
    snap = Target(f"databricks://{CLUSTER}", ConnectionOptions(timeout=10)).collect()
    assert snap.app_id == DEMO_APP
    assert snap.source.kind == "databricks-remote"
    assert "1234" in FakeWorkspace.proxy_orgs_seen  # workspace id detected from the SCIM response header
    assert snap.spark_conf["sparkmedic.cluster.node_type"] == "Standard_D32ds_v5"
    assert TOKEN not in snap.to_json()


def test_terminated_job_cluster_falls_back_to_log_delivery(workspace):
    pytest.importorskip("zstandard")
    FakeWorkspace.cluster_state = "TERMINATED"
    dest = os.path.join(FakeWorkspace.volume_root, CLUSTER, "eventlog", f"{CLUSTER}_10_0_0_1", "555")
    shutil.copytree(EVENTLOG_DIR, dest)
    snap = Target("databricks://run/99", ConnectionOptions(timeout=10)).collect()
    assert snap.app_id == DEMO_APP and snap.source.kind == "eventlog"
    assert snap.stage(12).executor_run_time_ms > 0
    assert snap.spark_conf["sparkmedic.cluster.name"] == "etl"


def test_terminated_without_logs_explains(workspace):
    FakeWorkspace.cluster_state = "TERMINATED"
    os.makedirs(FakeWorkspace.volume_root, exist_ok=True)
    with pytest.raises(Exception) as exc:
        Target(f"databricks://{CLUSTER}", ConnectionOptions(timeout=10)).collect()
    assert "No files found" in str(exc.value) or "not found" in str(exc.value).lower()
