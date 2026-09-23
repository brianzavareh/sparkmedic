"""Databricks-specific access paths.

1. **Inside a notebook** (``target="notebook"``): reads the driver's own Spark REST API
   (``sc.uiWebUrl``). No credentials are needed. When the SparkContext is not reachable
   (for example standard/shared access mode), it falls back to the workspace driver proxy
   with the notebook's short-lived token, which is kept in memory only.
2. **Remote cluster** (``databricks://<cluster-id>`` or ``databricks://run/<run-id>``):
   uses the Databricks SDK unified authentication and the workspace driver proxy
   ``https://<workspace>/driver-proxy-api/o/<org-id>/<cluster-id>/<port>/api/v1``.
   This proxy path is widely used but only community-documented; if it fails, or the
   cluster is terminated, the collector falls back to the event logs written by
   compute log delivery (Volumes or DBFS), which is the officially documented path.

Serverless compute does not expose the Spark UI REST API; use the query profile or
``system.query.history`` for those workloads.
"""

from __future__ import annotations

import os
import tempfile
from typing import Callable, Optional

from sparkmedic.auth import BearerTokenAuth, DatabricksSdkAuth, NoAuth
from sparkmedic.collectors.eventlog import EventLogCollector
from sparkmedic.collectors.rest import CollectOptions, RestCollector
from sparkmedic.http import HttpClient, SparkApiError
from sparkmedic.model import Snapshot, SourceInfo

DEFAULT_UI_PORT = 40001
MAX_DOWNLOAD_BYTES = 4 * 1024**3


class DatabricksError(RuntimeError):
    pass


def running_in_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# --------------------------------------------------------------------------- notebook
def _notebook_context() -> dict:
    """Host, cluster id, workspace id and API token of the current notebook (in memory only)."""
    try:
        from dbruntime.databricks_repl_context import get_context  # type: ignore

        c = get_context()
        return {
            "host": c.browserHostName,
            "cluster_id": c.clusterId,
            "workspace_id": getattr(c, "workspaceId", None),
            "token": c.apiToken,
        }
    except Exception:
        pass
    try:  # older runtimes: use dbutils from the IPython namespace
        import IPython  # type: ignore

        dbutils = IPython.get_ipython().user_ns["dbutils"]
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        host = ctx.browserHostName().get() if ctx.browserHostName().isDefined() else ctx.apiUrl().get()
        return {
            "host": host.replace("https://", "").rstrip("/"),
            "cluster_id": ctx.tags().apply("clusterId"),
            "workspace_id": ctx.tags().apply("orgId") if ctx.tags().contains("orgId") else None,
            "token": ctx.apiToken().get(),
        }
    except Exception as exc:
        raise DatabricksError(
            "Could not read the Databricks notebook context. Run this inside a Databricks notebook, "
            "or use a databricks://<cluster-id> target from your machine."
        ) from exc


def notebook_rest_collector(timeout: float = 60.0, progress: Optional[Callable[[str], None]] = None) -> RestCollector:
    """Best available REST collector from inside a Databricks notebook."""
    sc = None
    try:
        from pyspark import SparkContext

        sc = SparkContext._active_spark_context  # noqa: SLF001 - documented way to reach the running context
    except Exception:
        sc = None
    if sc is not None and getattr(sc, "uiWebUrl", None):
        url = sc.uiWebUrl.rstrip("/") + "/api/v1/"
        client = HttpClient(url, NoAuth(), timeout=timeout, retries=1)
        try:
            client.get_json("version")
            client.retries = 3
            return RestCollector(client, SourceInfo("notebook", "notebook", "driver-local Spark UI"), progress)
        except SparkApiError:
            pass  # fall through to the driver proxy
    ctx = _notebook_context()
    if not ctx.get("cluster_id"):
        raise DatabricksError("No cluster id in the notebook context (serverless compute has no Spark UI API).")
    token = ctx["token"]
    auth = BearerTokenAuth(lambda: token)
    last: Optional[Exception] = None
    for org in _org_candidates(None, ctx.get("workspace_id")):
        url = f"https://{ctx['host']}/driver-proxy-api/o/{org}/{ctx['cluster_id']}/{DEFAULT_UI_PORT}/api/v1/"
        client = HttpClient(url, auth, timeout=timeout)
        try:
            client.get_json("version")
            return RestCollector(
                client, SourceInfo("notebook", "notebook", f"Databricks driver proxy, cluster {ctx['cluster_id']}"), progress
            )
        except SparkApiError as exc:
            last = exc
    raise DatabricksError(f"Could not reach the Spark UI API from this notebook: {last}")


def _org_candidates(org_id: Optional[str], workspace_id: Optional[object]) -> list[str]:
    out = []
    for v in (org_id, workspace_id, "0"):
        if v not in (None, "") and str(v) not in out:
            out.append(str(v))
    return out


# --------------------------------------------------------------------------- remote
class DatabricksCollector:
    """Remote collector for a Databricks cluster or a job run."""

    def __init__(
        self,
        cluster_id: Optional[str] = None,
        run_id: Optional[int] = None,
        task_key: Optional[str] = None,
        profile: Optional[str] = None,
        host: Optional[str] = None,
        org_id: Optional[str] = None,
        ui_port: int = DEFAULT_UI_PORT,
        timeout: float = 60.0,
        prefer_eventlogs: bool = False,
        progress: Optional[Callable[[str], None]] = None,
    ):
        self.auth = DatabricksSdkAuth(profile=profile, host=host)
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient(config=self.auth.config)
        self.cluster_id = cluster_id
        self.run_id = run_id
        self.task_key = task_key
        self.org_id = org_id or os.environ.get("DATABRICKS_ORG_ID")
        self.ui_port = ui_port
        self.timeout = timeout
        self.prefer_eventlogs = prefer_eventlogs
        self.progress = progress or (lambda _m: None)
        self.spark_context_id: Optional[int] = None
        self.notes: list[str] = []

    # resolution ---------------------------------------------------------
    def resolve_cluster(self) -> str:
        if self.cluster_id:
            return self.cluster_id
        if self.run_id is None:
            raise DatabricksError("Provide a cluster id or a job run id.")
        run = self.w.jobs.get_run(self.run_id)
        candidates = []
        for t in run.tasks or []:
            if t.cluster_instance and t.cluster_instance.cluster_id:
                candidates.append((t.task_key, t.cluster_instance))
        if run.cluster_instance and run.cluster_instance.cluster_id:
            candidates.append(("<run>", run.cluster_instance))
        if not candidates:
            raise DatabricksError(
                f"Run {self.run_id} has no classic cluster (serverless tasks do not expose the Spark UI API)."
            )
        chosen = candidates[0]
        if self.task_key:
            match = [c for c in candidates if c[0] == self.task_key]
            if not match:
                raise DatabricksError(f"Task {self.task_key!r} not found; tasks: {[c[0] for c in candidates]}")
            chosen = match[0]
        elif len({c[1].cluster_id for c in candidates}) > 1:
            self.notes.append(
                "This run used several clusters; analyzed task "
                f"{chosen[0]!r}. Others: {[c[0] for c in candidates[1:]]}. Use ?task=<task_key> to choose."
            )
        self.cluster_id = chosen[1].cluster_id
        if chosen[1].spark_context_id:
            self.spark_context_id = int(chosen[1].spark_context_id)
        return self.cluster_id

    def rest_collector(self) -> RestCollector:
        cluster = self.resolve_cluster()
        ws_id = None
        try:
            ws_id = self.w.get_workspace_id()
        except Exception:
            pass
        last: Optional[Exception] = None
        for org in _org_candidates(self.org_id, ws_id):
            url = f"{self.auth.host}/driver-proxy-api/o/{org}/{cluster}/{self.ui_port}/api/v1/"
            client = HttpClient(url, self.auth, timeout=self.timeout)
            try:
                client.get_json("version")
                return RestCollector(
                    client, SourceInfo("databricks-remote", f"databricks://{cluster}", f"driver proxy (org {org})"), self.progress
                )
            except SparkApiError as exc:
                last = exc
        raise DatabricksError(f"Driver proxy not reachable for cluster {cluster}: {last}")

    # collection ---------------------------------------------------------
    def collect(self, app_id: Optional[str] = None, options: Optional[CollectOptions] = None) -> Snapshot:
        cluster = self.resolve_cluster()
        details = self.w.clusters.get(cluster)
        state = str(getattr(details.state, "value", details.state))
        self.progress(f"Cluster {cluster} ({details.cluster_name}) is {state}")
        snap: Optional[Snapshot] = None
        if state == "RUNNING" and not self.prefer_eventlogs:
            try:
                snap = self.rest_collector().collect(app_id, options)
            except (DatabricksError, SparkApiError) as exc:
                self.notes.append(f"Live Spark UI API failed ({exc}); trying delivered event logs.")
        if snap is None:
            snap = self._collect_from_log_delivery(details, app_id)
        snap.notes.extend(self.notes)
        _add_cluster_info(snap, details)
        return snap

    def _collect_from_log_delivery(self, details, app_id: Optional[str]) -> Snapshot:
        conf = details.cluster_log_conf
        if conf is None or not (conf.volumes or conf.dbfs or conf.s3):
            raise DatabricksError(
                f"Cluster {details.cluster_id} is not running and compute log delivery is not configured, "
                "so no Spark metrics are available. Enable log delivery (Advanced options > Logging) to keep "
                "event logs after the cluster terminates."
            )
        if conf.s3 and not (conf.volumes or conf.dbfs):
            raise DatabricksError(
                f"Event logs are delivered to S3 ({conf.s3.destination}/{details.cluster_id}/eventlog/). "
                "Copy them locally and run: sparkmedic analyze <local-folder>"
            )
        dest = (conf.volumes.destination if conf.volumes else conf.dbfs.destination).rstrip("/")
        root = f"{dest}/{details.cluster_id}/eventlog"
        local = download_databricks_path(self.w, root, prefer_leaf=str(self.spark_context_id or ""), progress=self.progress)
        return EventLogCollector(local, progress=self.progress).collect(app_id)


def _add_cluster_info(snap: Snapshot, details) -> None:
    """Record cluster shape in the (redacted) conf so rules can reason about it."""
    info = {
        "sparkmedic.cluster.id": details.cluster_id,
        "sparkmedic.cluster.name": details.cluster_name or "",
        "sparkmedic.cluster.node_type": details.node_type_id or "",
        "sparkmedic.cluster.driver_node_type": details.driver_node_type_id or "",
        "sparkmedic.cluster.num_workers": str(details.num_workers or 0),
        "sparkmedic.cluster.spark_version": details.spark_version or "",
        "sparkmedic.cluster.runtime_engine": str(getattr(details.runtime_engine, "value", details.runtime_engine) or ""),
    }
    if details.autoscale:
        info["sparkmedic.cluster.autoscale"] = f"{details.autoscale.min_workers}-{details.autoscale.max_workers}"
    snap.spark_conf.update({k: v for k, v in info.items() if v not in (None, "None")})


def _list_remote_files(w, root: str, is_volume: bool) -> list[tuple[str, int]]:
    files: list[tuple[str, int]] = []
    if is_volume:
        try:
            w.files.get_metadata(root)  # the path is a single file
            return [(root, 0)]
        except Exception:
            pass
        stack = [root]
        while stack:
            d = stack.pop()
            for e in w.files.list_directory_contents(d):
                if e.is_directory:
                    stack.append(e.path)
                else:
                    files.append((e.path, int(e.file_size or 0)))
    else:
        path = root if root.startswith("dbfs:") else f"dbfs:{root}"
        for info in w.dbfs.list(path, recursive=True):
            if not info.is_dir:
                files.append((info.path, int(info.file_size or 0)))
    return files


def download_databricks_path(
    w, root: str, prefer_leaf: str = "", progress: Optional[Callable[[str], None]] = None
) -> str:
    """Download a Volumes (``/Volumes/...``) or DBFS (``dbfs:/...``) folder or file to a temp dir."""
    progress = progress or (lambda _m: None)
    tmp = tempfile.mkdtemp(prefix="sparkmedic-dbx-")
    files: list[tuple[str, int]] = []
    is_volume = root.startswith("/Volumes/")
    try:
        files = _list_remote_files(w, root, is_volume)
    except Exception as exc:  # SDK raises NotFound/PermissionDenied/...
        raise DatabricksError(
            f"No files found under {root} ({type(exc).__name__}: {exc}). Check that compute log delivery writes to "
            "this location, that the cluster ran long enough to deliver logs (every ~5 minutes), and that you can read it."
        ) from exc
    if prefer_leaf:
        leaf = [f for f in files if f"/{prefer_leaf}/" in f[0]]
        files = leaf or files
    total = sum(s for _, s in files)
    if total > MAX_DOWNLOAD_BYTES:
        raise DatabricksError(
            f"Event logs under {root} are {total / 1024**3:.1f} GiB; download a single application folder "
            "and pass it as a local path instead."
        )
    if not files:
        raise DatabricksError(f"No files found under {root}")
    progress(f"Downloading {len(files)} event log file(s) ({total / 1024**2:.0f} MiB) from {root}")
    prefix = root.rstrip("/")
    for remote, _ in files:
        rel = remote.replace("dbfs:", "")[len(prefix.replace("dbfs:", "")) :].lstrip("/") or os.path.basename(remote)
        dest = os.path.realpath(os.path.join(tmp, rel))
        if not dest.startswith(os.path.realpath(tmp) + os.sep):
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as out:
            if is_volume:
                stream = w.files.download(remote).contents
            else:
                stream = w.dbfs.download(remote)
            try:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            finally:
                close = getattr(stream, "close", None)
                if close:
                    close()
    return tmp
