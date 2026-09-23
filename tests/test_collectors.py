import gzip
import json
import os
import shutil
import zipfile

import pytest

from sparkmedic.auth import BearerTokenAuth
from sparkmedic.collectors.eventlog import EventLogCollector, EventLogError, discover
from sparkmedic.collectors.rest import CollectOptions, RestCollector
from sparkmedic.collectors.targets import Target, rest_base_url, target_kind
from sparkmedic.http import HttpClient, SparkApiError
from sparkmedic.model import REDACTED, Snapshot, SourceInfo, redact_conf

from .conftest import DEMO_APP, EVENTLOG_DIR


# ---------------------------------------------------------------- REST
def test_rest_collect(fake_spark_ui):
    url, _ = fake_spark_ui
    snap = Target(url).collect()
    assert snap.app_id == DEMO_APP
    assert snap.spark_version == "4.2.0"
    assert len(snap.stages) == 19 and len(snap.sql) == 7
    st = snap.stage(12)
    assert st.task_quantiles["duration"][-1] > 90_000
    assert st.top_tasks and st.top_tasks[0].duration_ms >= st.top_tasks[-1].duration_ms
    assert st.sql_execution_id == 4
    q4 = snap.sql_execution(4)
    scan = next(n for n in q4.nodes if n.name.startswith("Scan"))
    assert scan.metric("size of files read").total == pytest.approx(254 * 1024**2)
    assert 12 in scan.stage_ids
    project = next(n for n in q4.nodes if n.name == "Project")
    assert "get_json_object" in project.description and 12 in project.stage_ids


def test_rest_snapshot_roundtrip(fake_spark_ui, tmp_path):
    url, _ = fake_spark_ui
    snap = Target(url).collect()
    p = tmp_path / "s.json"
    snap.save(str(p))
    again = Snapshot.load(str(p))
    assert again.stage(12).task_quantiles == snap.stage(12).task_quantiles
    assert again.sql_execution(4).nodes[0].metrics.keys() == snap.sql_execution(4).nodes[0].metrics.keys()


def test_bearer_auth_and_401(fake_spark_ui, monkeypatch):
    url, handler = fake_spark_ui
    handler.required_token = "s3cret"
    with pytest.raises(SparkApiError) as exc:
        Target(url).collect()
    assert exc.value.status == 401 and "s3cret" not in str(exc.value)
    monkeypatch.setenv("SPARKMEDIC_TOKEN", "s3cret")
    snap = Target(url).collect()
    assert snap.app_id == DEMO_APP
    assert handler.seen_headers[-1]["Authorization"] == "Bearer s3cret"


def test_html_response_gives_helpful_error(fake_spark_ui):
    url, _ = fake_spark_ui
    client = HttpClient(url + "/", BearerTokenAuth(lambda: "x"), retries=0)
    with pytest.raises(SparkApiError) as exc:
        client.get_json("html")
    assert "HTML page" in str(exc.value)


def test_missing_app(fake_spark_ui):
    url, _ = fake_spark_ui
    rc = RestCollector(HttpClient(rest_base_url(url), retries=0), SourceInfo("rest", url))
    with pytest.raises(SparkApiError):
        rc.collect("nope", CollectOptions())


def test_target_kinds(tmp_path):
    assert target_kind("http://h:4040") == "rest"
    assert target_kind("notebook") == "notebook"
    assert target_kind("databricks://0923-abc") == "databricks"
    assert target_kind("dbfs:/cluster-logs/x") == "databricks-path"
    assert target_kind(str(tmp_path)) == "eventlog"
    assert rest_base_url("http://h:18080/history/app/jobs/") == "http://h:18080/history/app/jobs/api/v1/"
    assert rest_base_url("http://h:4040/api/v1/applications") == "http://h:4040/api/v1/"


def test_redaction():
    conf = redact_conf({"spark.hadoop.fs.azure.account.key.x": "abc", "spark.app.name": "x",
                        "spark.databricks.token": "t", "a.b": "{{secrets/scope/key}}"})
    assert conf["spark.app.name"] == "x"
    assert all(conf[k] == REDACTED for k in conf if k != "spark.app.name")


# ---------------------------------------------------------------- event logs
def test_eventlog_rolling_zstd_matches_rest(fake_spark_ui):
    pytest.importorskip("zstandard")
    snap = EventLogCollector(EVENTLOG_DIR).collect()
    assert snap.app_id == DEMO_APP
    st = snap.stage(12)
    assert st.num_complete_tasks == 2
    assert st.executor_run_time_ms == pytest.approx(193_955)
    url, _ = fake_spark_ui
    rest = Target(url).collect()
    assert st.task_quantiles["executorRunTime"] == rest.stage(12).task_quantiles["executorRunTime"]
    q4 = snap.sql_execution(4)
    scan = next(n for n in q4.nodes if n.name.startswith("Scan"))
    assert scan.metric("size of files read").total == pytest.approx(254 * 1024**2, rel=0.01)
    assert 12 in scan.stage_ids


def _write_single_log(path, events, compress=False):
    data = "\n".join(json.dumps(e) for e in events) + "\n"
    if compress:
        with gzip.open(path, "wt") as fh:
            fh.write(data)
    else:
        with open(path, "w") as fh:
            fh.write(data)


def _tiny_events(app_id="app-1"):
    return [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.1"},
        {"Event": "SparkListenerApplicationStart", "App Name": "tiny", "App ID": app_id, "Timestamp": 1000, "User": "u"},
        {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": [["spark.master", "yarn"], ["spark.x.password", "p"]]},
        {"Event": "SparkListenerExecutorAdded", "Timestamp": 1000, "Executor ID": "1", "Executor Info": {"Host": "h1", "Total Cores": 4}},
        {"Event": "SparkListenerJobStart", "Job ID": 0, "Submission Time": 1100, "Stage Infos": [{"Stage ID": 0, "Stage Attempt ID": 0, "Stage Name": "count at a.py:1", "Number of Tasks": 2}], "Stage IDs": [0], "Properties": {}},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 0, "Stage Attempt ID": 0, "Stage Name": "count at a.py:1", "Number of Tasks": 2, "Submission Time": 1100}},
        *[
            {"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Stage Attempt ID": 0, "Task End Reason": {"Reason": "Success"},
             "Task Info": {"Task ID": i, "Index": i, "Attempt": 0, "Launch Time": 1100, "Finish Time": 1100 + d, "Executor ID": "1", "Host": "h1", "Getting Result Time": 0},
             "Task Metrics": {"Executor Run Time": d - 10, "Executor CPU Time": (d - 10) * 1_000_000, "JVM GC Time": 1, "Executor Deserialize Time": 5,
                              "Result Serialization Time": 0, "Memory Bytes Spilled": 0, "Disk Bytes Spilled": 0,
                              "Input Metrics": {"Bytes Read": 100, "Records Read": 10}}}
            for i, d in enumerate([100, 900])
        ],
        {"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Stage Attempt ID": 0,
         "Task End Reason": {"Reason": "ExceptionFailure", "Class Name": "java.lang.OutOfMemoryError", "Description": "Java heap space"},
         "Task Info": {"Task ID": 9, "Index": 1, "Attempt": 0, "Launch Time": 1100, "Finish Time": 1200, "Executor ID": "1", "Host": "h1"},
         "Task Metrics": {"Executor Run Time": 90}},
        {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": 0, "Stage Attempt ID": 0, "Stage Name": "count at a.py:1", "Number of Tasks": 2, "Submission Time": 1100, "Completion Time": 2100}},
        {"Event": "SparkListenerJobEnd", "Job ID": 0, "Completion Time": 2100, "Job Result": {"Result": "JobSucceeded"}},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 3000},
    ]


def test_single_gz_log_and_failures(tmp_path):
    p = tmp_path / "app-1.gz"
    _write_single_log(p, _tiny_events(), compress=True)
    snap = EventLogCollector(str(p)).collect()
    st = snap.stage(0)
    assert snap.completed and snap.spark_version == "3.5.1"
    assert st.num_complete_tasks == 2 and st.num_failed_tasks == 1
    assert st.task_quantiles["duration"] == [100.0, 100.0, 900.0, 900.0, 900.0]
    assert "OutOfMemoryError" in st.failed_task_samples[0].error
    assert snap.spark_conf["spark.x.password"] == REDACTED


def test_databricks_layout_order(tmp_path):
    d = tmp_path / "0923-abc" / "eventlog" / "0923-abc_10_0_0_1" / "7771234"
    d.mkdir(parents=True)
    ev = _tiny_events("app-dbx")
    _write_single_log(d / "eventlog-2026-09-23--01-00.gz", ev[:5], compress=True)
    _write_single_log(d / "eventlog", ev[5:])
    refs = discover(str(tmp_path))
    assert len(refs) == 1 and refs[0].kind == "databricks"
    assert refs[0].files[-1].endswith("eventlog")
    snap = EventLogCollector(str(tmp_path)).collect()
    assert snap.app_id == "app-dbx" and snap.stage(0).num_complete_tasks == 2


def test_zip_and_multiple_apps(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    _write_single_log(src / "application_1_0001", _tiny_events("application_1_0001"))
    ev2 = _tiny_events("application_1_0002")
    ev2[1]["Timestamp"] = 5000
    _write_single_log(src / "application_1_0002", ev2)
    z = tmp_path / "logs.zip"
    with zipfile.ZipFile(z, "w") as zf:
        for f in os.listdir(src):
            zf.write(src / f, f)
    snap = EventLogCollector(str(z)).collect()
    assert snap.app_id == "application_1_0002"
    assert any("Found 2 applications" in n for n in snap.notes)
    snap1 = EventLogCollector(str(z)).collect("application_1_0001")
    assert snap1.app_id == "application_1_0001"


def test_unsupported_codec(tmp_path):
    p = tmp_path / "app-1.lz4"
    p.write_bytes(b"xx")
    with pytest.raises(EventLogError):
        EventLogCollector(str(p)).collect()


def test_truncated_inprogress_log(tmp_path):
    p = tmp_path / "app-2.inprogress"
    ev = _tiny_events("app-2")[:-3]
    data = "\n".join(json.dumps(e) for e in ev) + '\n{"Event": "SparkListenerTaskEnd", "Stage'
    p.write_text(data)
    snap = EventLogCollector(str(p)).collect()
    assert not snap.completed and snap.stage(0).status == "ACTIVE"


def test_copy_of_fixture_is_discoverable(tmp_path):
    dst = tmp_path / "nested" / "logs"
    shutil.copytree(EVENTLOG_DIR, dst)
    refs = discover(str(tmp_path))
    assert [r.app_id for r in refs] == [DEMO_APP]
