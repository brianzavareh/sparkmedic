import json

import pytest

from sparkmedic.cli import main

from .conftest import DEMO_APP, EVENTLOG_DIR


def test_cli_rules(capsys):
    assert main(["rules"]) == 0
    assert "SD104" in capsys.readouterr().out


def test_cli_apps_json(fake_spark_ui, capsys):
    url, _ = fake_spark_ui
    assert main(["apps", url, "--json", "-q"]) == 0
    apps = json.loads(capsys.readouterr().out)
    assert apps[0]["app_id"] == DEMO_APP


def test_cli_analyze_to_dir_and_fail_on(fake_spark_ui, tmp_path, capsys):
    url, _ = fake_spark_ui
    out = tmp_path / "out"
    code = main(["analyze", url, "-y", "-q", "-o", str(out), "--format", "both", "--fail-on", "high"])
    assert code == 2  # the demo app has a critical compute-bound stage
    files = sorted(p.name for p in out.iterdir())
    assert files == [f"snapshot-{DEMO_APP}.json", f"sparkmedic-{DEMO_APP}.json", f"sparkmedic-{DEMO_APP}.md"]
    report = json.loads((out / f"sparkmedic-{DEMO_APP}.json").read_text())
    assert report["findings"][0]["rule_id"] == "SD104"


def test_cli_collect_then_analyze_snapshot(fake_spark_ui, tmp_path, capsys):
    url, _ = fake_spark_ui
    snap = tmp_path / "snap.json"
    assert main(["collect", url, "-y", "-q", "-o", str(snap)]) == 0
    capsys.readouterr()
    assert main(["analyze", str(snap), "-y", "-q", "--format", "json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["application"]["app_id"] == DEMO_APP


def test_cli_eventlog_markdown(capsys):
    pytest.importorskip("zstandard")
    assert main(["analyze", EVENTLOG_DIR, "-y", "-q"]) == 0
    out = capsys.readouterr().out
    assert "# Spark performance diagnosis: sparkmedic-demo" in out
    assert "get_json_object() 40 times" in out


def test_cli_error_exit_code(capsys):
    assert main(["analyze", "/does/not/exist", "-y", "-q"]) == 1
    assert "error:" in capsys.readouterr().err


def test_mcp_tools(fake_spark_ui):
    pytest.importorskip("mcp")
    from sparkmedic import mcp_server

    url, _ = fake_spark_ui
    plan = mcp_server.plan_diagnosis_impl(url)
    assert plan["will_analyze"]["app_id"] == DEMO_APP and plan["access"] == "read-only"
    res = mcp_server.diagnose_impl(url, include_markdown=False)
    sid = res["snapshot_id"]
    assert res["report"]["findings"][0]["rule_id"] == "SD104"
    st = mcp_server.get_stage_details_impl(sid, 12)
    assert st["stage_id"] == 12 and st["readable"]["task_time"].endswith("min")
    plan = mcp_server.get_sql_plan_impl(sid, 4, max_chars=100)
    assert plan["truncated"] and plan["nodes"]
    server = mcp_server.build_server()
    assert server is not None
