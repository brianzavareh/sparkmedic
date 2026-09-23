"""Turn a target string into a collector.

Target forms
------------
``http(s)://host:port[/...]``          live Spark UI or History Server (``/api/v1`` is added if missing)
``notebook``                           inside a Databricks notebook (driver-local API)
``databricks://<cluster-id>``          remote Databricks cluster (Databricks SDK auth)
``databricks://run/<run-id>[?task=k]`` the cluster used by a Databricks job run
``/path/to/eventlogs`` or ``.zip``     offline Spark / Databricks event logs
``dbfs:/...`` or ``/Volumes/...``      delivered event logs in Databricks storage (downloaded via SDK
                                       when not mounted locally)
``snapshot.json``                      a snapshot saved earlier with ``sparkmedic collect``
"""

from __future__ import annotations

import os
import urllib.parse
from dataclasses import dataclass, field
from typing import Callable, Optional

from sparkmedic.auth import auth_from_env
from sparkmedic.collectors.eventlog import EventLogCollector
from sparkmedic.collectors.rest import CollectOptions, RestCollector
from sparkmedic.http import HttpClient
from sparkmedic.model import Snapshot, SourceInfo


@dataclass
class ConnectionOptions:
    timeout: float = 60.0
    verify_tls: bool = True
    ca_bundle: Optional[str] = None
    token_env: str = "SPARKMEDIC_TOKEN"
    databricks_profile: Optional[str] = None
    databricks_host: Optional[str] = None
    databricks_org_id: Optional[str] = None
    databricks_ui_port: int = 40001
    prefer_eventlogs: bool = False
    extra: dict = field(default_factory=dict)


def target_kind(target: str) -> str:
    t = target.strip()
    low = t.lower()
    if low in ("notebook", "databricks-notebook", "local-notebook"):
        return "notebook"
    if low.startswith("databricks://"):
        return "databricks"
    if low.startswith(("http://", "https://")):
        return "rest"
    if low.endswith(".json") and os.path.isfile(os.path.expanduser(t)):
        return "snapshot"
    if low.startswith("dbfs:") or (t.startswith("/Volumes/") and not os.path.exists(t)):
        return "databricks-path"
    return "eventlog"


def rest_base_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    path = parts.path.rstrip("/")
    if "/api/v1" in path:
        path = path[: path.index("/api/v1") + len("/api/v1")]
    else:
        path = path + "/api/v1"
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path + "/", "", ""))


def parse_databricks_target(target: str) -> dict:
    parts = urllib.parse.urlsplit(target)
    host_part = parts.netloc
    segs = [s for s in parts.path.split("/") if s]
    query = dict(urllib.parse.parse_qsl(parts.query))
    if host_part == "run" and segs:
        return {"run_id": int(segs[0]), "task_key": query.get("task")}
    if host_part == "cluster" and segs:
        return {"cluster_id": segs[0]}
    if host_part:
        return {"cluster_id": host_part}
    raise ValueError(f"Invalid Databricks target {target!r}; use databricks://<cluster-id> or databricks://run/<run-id>")


class Target:
    """Resolved target that can list applications and collect snapshots."""

    def __init__(
        self,
        target: str,
        conn: Optional[ConnectionOptions] = None,
        progress: Optional[Callable[[str], None]] = None,
    ):
        self.raw = target
        self.conn = conn or ConnectionOptions()
        self.kind = target_kind(target)
        self.progress = progress or (lambda _m: None)

    # ------------------------------------------------------------------
    def _rest(self) -> RestCollector:
        if self.kind == "rest":
            client = HttpClient(
                rest_base_url(self.raw),
                auth_from_env(self.conn.token_env),
                timeout=self.conn.timeout,
                verify_tls=self.conn.verify_tls,
                ca_bundle=self.conn.ca_bundle,
            )
            return RestCollector(client, SourceInfo("rest", _safe_target(self.raw)), self.progress)
        if self.kind == "notebook":
            from sparkmedic.collectors.databricks import notebook_rest_collector

            return notebook_rest_collector(self.conn.timeout, self.progress)
        if self.kind == "databricks":
            return self._databricks().rest_collector()
        raise ValueError(f"Target {self.raw!r} is not a REST target")

    def _databricks(self):
        from sparkmedic.collectors.databricks import DatabricksCollector

        return DatabricksCollector(
            profile=self.conn.databricks_profile,
            host=self.conn.databricks_host,
            org_id=self.conn.databricks_org_id,
            ui_port=self.conn.databricks_ui_port,
            timeout=self.conn.timeout,
            prefer_eventlogs=self.conn.prefer_eventlogs,
            progress=self.progress,
            **parse_databricks_target(self.raw),
        )

    def _eventlog_path(self) -> str:
        if self.kind == "databricks-path":
            from databricks.sdk import WorkspaceClient

            from sparkmedic.auth import DatabricksSdkAuth
            from sparkmedic.collectors.databricks import download_databricks_path

            auth = DatabricksSdkAuth(self.conn.databricks_profile, self.conn.databricks_host)
            return download_databricks_path(WorkspaceClient(config=auth.config), self.raw, progress=self.progress)
        return self.raw

    # ------------------------------------------------------------------
    def list_applications(self, limit: int = 50) -> list[dict]:
        if self.kind == "snapshot":
            s = Snapshot.load(self.raw)
            return [{"app_id": s.app_id, "name": s.app_name, "completed": s.completed, "start_time_ms": s.start_time_ms}]
        if self.kind in ("eventlog", "databricks-path"):
            return [r.as_dict() for r in EventLogCollector(self._eventlog_path()).list_applications()[:limit]]
        return [a.as_dict() for a in self._rest().list_applications(limit)]

    def collect(self, app_id: Optional[str] = None, options: Optional[CollectOptions] = None) -> Snapshot:
        opts = options or CollectOptions()
        if self.kind == "snapshot":
            return Snapshot.load(self.raw)
        if self.kind in ("eventlog", "databricks-path"):
            snap = EventLogCollector(self._eventlog_path(), self.progress, top_tasks=opts.task_samples).collect(app_id)
            snap.source = SourceInfo("eventlog", _safe_target(self.raw), snap.source.detail)
            return snap
        if self.kind == "databricks":
            return self._databricks().collect(app_id, opts)
        return self._rest().collect(app_id, opts)


def _safe_target(t: str) -> str:
    parts = urllib.parse.urlsplit(t)
    if parts.scheme in ("http", "https"):
        netloc = parts.hostname or ""
        if parts.port:
            netloc += f":{parts.port}"
        return urllib.parse.urlunsplit((parts.scheme, netloc, parts.path, "", ""))
    return t
