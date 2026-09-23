"""Rule tests on synthetic snapshots, one scenario per root cause."""

from sparkmedic.analysis.engine import AnalysisConfig, run_analysis
from sparkmedic.model import Executor, TaskSample

from .conftest import make_snapshot, make_stage, node, quant, sql_exec

GB = 1024**3
MB = 1024**2


def ids(diag):
    return [f.rule_id for f in diag.findings]


def by_rule(diag, rid):
    return [f for f in diag.findings if f.rule_id == rid]


def test_databricks_variant_foreachbatch_case():
    """Reproduces a real case: foreachBatch MERGE where 776 variant_get calls per row dominate a stage,
    and the micro-batch (Scan ExistingRDD) is recomputed for a collect_set and again for the MERGE."""
    project_desc = "(3) Project [codegen id : 1]\nOutput [790]: [" + ", ".join(
        f"variant_get(periodic_json#1423, $.field{i}, StringType, true, Some(UTC)) AS field{i}#{2000 + i}" for i in range(776)
    ) + "]\nInput [14]: [...]"
    merge_nodes = [
        node(0, "Exchange", stage_ids=[26], children=[1], data_size=15.1 * GB),
        node(1, "WindowGroupLimit", stage_ids=[26], children=[2]),
        node(2, "Sort", stage_ids=[26], children=[3]),
        node(3, "Project", description=project_desc, stage_ids=[26], children=[4]),
        node(4, "Scan ExistingRDD", stage_ids=[26], number_of_output_rows=45_000_000),
    ]
    count_nodes = [
        node(0, "ObjectHashAggregate", stage_ids=[23], children=[1]),
        node(1, "Project", description="(3) Project\nOutput [1]: [dlog_date#1426]", stage_ids=[23], children=[2]),
        node(2, "Scan ExistingRDD", stage_ids=[23], number_of_output_rows=45_000_000),
    ]
    stage26 = make_stage(
        26, run_ms=9.3 * 3_600_000, wall_ms=11 * 60_000, num_tasks=126, cpu_ratio=0.97,
        jvm_gc_time_ms=0.01 * 9.3 * 3_600_000, shuffle_write_bytes=15.1 * GB, shuffle_write_records=18_218_912,
        input_records=23_338_912, memory_bytes_spilled=4.4 * GB, disk_bytes_spilled=451 * MB, sql_execution_id=16,
        task_quantiles={"duration": quant(432_000, 552_000, 558_000, 558_000, 564_000),
                        "shuffleWriteBytes": quant(217 * MB, 245 * MB, 246 * MB, 246 * MB, 297 * MB)},
    )
    stage23 = make_stage(23, run_ms=27.7 * 60_000, wall_ms=30_000, num_tasks=157, sql_execution_id=15)
    snap = make_snapshot(
        [stage26, stage23],
        sql=[sql_exec(15, count_nodes, duration_ms=33_000), sql_exec(16, merge_nodes, duration_ms=20 * 60_000)],
        conf={"spark.master": "spark://x", "spark.databricks.clusterUsageTags.sparkVersion": "17.3.x-scala2.13",
              "spark.sql.shuffle.partitions": "auto"},
    )
    diag = run_analysis(snap, AnalysisConfig())
    top = diag.findings[0]
    assert top.rule_id == "SD104" and top.stage_ids == [26]
    assert top.severity == "critical" and top.confidence == "high"
    assert any("776 times" in e and "periodic_json" in e for e in top.evidence)
    assert any("cast the VARIANT to a STRUCT" in r.title for r in top.recommendations)
    assert any(r.title.startswith("Try Photon") for r in top.recommendations)
    rep = by_rule(diag, "SD207")
    assert rep and rep[0].title.startswith("The same input is read 2 times (ExistingRDD")
    spill = by_rule(diag, "SD102")
    assert spill and spill[0].severity == "low"
    assert "SD101" not in ids(diag)  # no skew: tasks are even
    md = diag.to_markdown()
    assert "Compute-bound Stage 26" in md and "Confirm before acting" in md


def test_data_skew():
    st = make_stage(
        3, run_ms=2 * 3_600_000, wall_ms=40 * 60_000, num_tasks=200, sql_execution_id=None,
        shuffle_read_bytes=40 * GB,
        task_quantiles={
            "duration": quant(20_000, 30_000, 35_000, 40_000, 35 * 60_000),
            "shuffleReadBytes": quant(50 * MB, 150 * MB, 180 * MB, 200 * MB, 9 * GB),
        },
        top_tasks=[TaskSample(task_id=7, index=7, executor_id="1", host="10.0.0.2", duration_ms=35 * 60_000)],
    )
    diag = run_analysis(make_snapshot([st]), AnalysisConfig())
    f = by_rule(diag, "SD101")[0]
    assert f.title.startswith("Data skew") and f.confidence == "high"
    assert any("skewJoin" in r.snippet for r in f.recommendations)


def test_straggler_without_data_skew_suggests_speculation():
    st = make_stage(
        4, run_ms=3_600_000, wall_ms=20 * 60_000, num_tasks=64,
        task_quantiles={"duration": quant(50_000, 55_000, 56_000, 57_000, 600_000),
                        "inputBytes": quant(120 * MB, 125 * MB, 126 * MB, 127 * MB, 128 * MB)},
        top_tasks=[TaskSample(task_id=i, host="10.0.0.9", duration_ms=600_000 - i) for i in range(3)],
    )
    diag = run_analysis(make_snapshot([st]), AnalysisConfig())
    f = by_rule(diag, "SD101")[0]
    assert f.title.startswith("Straggler")
    assert any("speculation" in r.snippet for r in f.recommendations)
    assert any("10.0.0.9" in e for e in f.evidence)


def test_big_spill_and_partition_sizing():
    st = make_stage(
        5, run_ms=3 * 3_600_000, wall_ms=30 * 60_000, num_tasks=200, shuffle_read_bytes=400 * GB,
        memory_bytes_spilled=300 * GB, disk_bytes_spilled=120 * GB,
        task_quantiles={"diskBytesSpilled": quant(0.4 * GB, 0.5 * GB, 0.6 * GB, 0.6 * GB, 0.7 * GB)},
    )
    diag = run_analysis(make_snapshot([st], conf={"spark.master": "yarn", "spark.sql.shuffle.partitions": "200"}), AnalysisConfig())
    f = by_rule(diag, "SD102")[0]
    assert f.severity in ("high", "critical")
    assert any("3,200 partitions" in r.detail for r in f.recommendations)
    assert "SD402" in ids(diag) and "SD106" in ids(diag)


def test_gc_pressure():
    st = make_stage(6, run_ms=3_600_000, jvm_gc_time_ms=0.3 * 3_600_000, num_tasks=128)
    diag = run_analysis(make_snapshot([st]), AnalysisConfig())
    assert by_rule(diag, "SD103")[0].severity == "high"


def test_io_bound_input():
    st = make_stage(7, run_ms=3_600_000, wall_ms=60_000, num_tasks=512, input_bytes=2000 * GB)
    diag = run_analysis(make_snapshot([st]), AnalysisConfig())
    f = by_rule(diag, "SD105")[0]
    assert "(input)" in f.title
    assert "SD104" not in ids(diag)


def test_python_udf_and_small_files_and_nested_loop():
    nodes = [
        node(0, "BroadcastNestedLoopJoin", stage_ids=[8], children=[1, 3], number_of_output_rows=5e9),
        node(1, "BatchEvalPython", stage_ids=[8], children=[2]),
        node(2, "Scan parquet main.sales.events", stage_ids=[8], number_of_files_read=50_000, size_of_files_read=50_000 * 2 * MB),
        node(3, "Scan parquet main.sales.dim", stage_ids=[8], number_of_files_read=3, size_of_files_read=30 * MB),
    ]
    st = make_stage(8, run_ms=2 * 3_600_000, wall_ms=30 * 60_000, num_tasks=512, input_bytes=100 * GB, sql_execution_id=1)
    diag = run_analysis(make_snapshot([st], sql=[sql_exec(1, nodes)]), AnalysisConfig())
    got = ids(diag)
    assert "SD201" in got and "SD203" in got and "SD206" in got
    sf = by_rule(diag, "SD206")[0]
    assert "main.sales.events" in sf.title and sf.severity == "high"


def test_broadcast_candidate():
    nodes = [
        node(0, "SortMergeJoin", stage_ids=[9], children=[1, 3]),
        node(1, "Sort", stage_ids=[9], children=[2]),
        node(2, "Exchange", stage_ids=[9], children=[], data_size=50 * GB),
        node(3, "Sort", stage_ids=[9], children=[4]),
        node(4, "Exchange", stage_ids=[9], children=[], data_size=20 * MB),
    ]
    st = make_stage(9, run_ms=3_600_000, num_tasks=400, sql_execution_id=2, shuffle_read_bytes=50 * GB)
    snap = make_snapshot([st], sql=[sql_exec(2, nodes)], conf={"spark.master": "yarn", "spark.sql.autoBroadcastJoinThreshold": "-1",
                                                           "spark.sql.shuffle.partitions": "auto"})
    diag = run_analysis(snap, AnalysisConfig())
    f = by_rule(diag, "SD204")[0]
    assert "20.0 MiB" in f.summary
    assert any("autoBroadcastJoinThreshold is -1" in e for e in f.evidence)


def test_failures_and_executor_loss():
    st = make_stage(10, run_ms=3_600_000, num_tasks=100, num_failed_tasks=12, status="COMPLETE",
                    failed_task_samples=[TaskSample(task_id=1, error="ExecutorLostFailure: Container killed by YARN for exceeding memory limits")])
    execs = [
        Executor(executor_id="driver", total_cores=4, add_time_ms=0),
        Executor(executor_id="1", host="h1", total_cores=16, add_time_ms=0, remove_time_ms=100, remove_reason="Container killed: exit code 137"),
        Executor(executor_id="2", host="h2", total_cores=16, add_time_ms=0),
    ]
    diag = run_analysis(make_snapshot([st], executors=execs), AnalysisConfig())
    f = by_rule(diag, "SD302")[0]
    assert any("out of memory" in e for e in f.evidence)
    assert by_rule(diag, "SD303")[0].severity == "high"


def test_aqe_disabled_and_focus_filter():
    a = make_stage(11, run_ms=3_600_000, jvm_gc_time_ms=0.3 * 3_600_000, num_tasks=64, shuffle_read_bytes=10 * GB)
    b = make_stage(12, run_ms=3_600_000, jvm_gc_time_ms=0.3 * 3_600_000, num_tasks=64)
    snap = make_snapshot([a, b], conf={"spark.master": "yarn", "spark.sql.adaptive.enabled": "false"})
    diag = run_analysis(snap, AnalysisConfig(focus_stage_ids=[12]))
    got = ids(diag)
    assert "SD401" in got  # app-level findings stay
    assert all(12 in f.stage_ids for f in diag.findings if f.stage_ids)


def test_driver_gaps():
    st = make_stage(13, run_ms=600_000, num_tasks=64)
    snap = make_snapshot([st], duration_ms=3_600_000)
    snap.jobs[0].submission_time_ms = 0
    snap.jobs[0].completion_time_ms = 600_000
    from sparkmedic.model import Job

    snap.jobs.append(Job(job_id=99, stage_ids=[], submission_time_ms=3_000_000, completion_time_ms=3_500_000))
    diag = run_analysis(snap, AnalysisConfig())
    f = by_rule(diag, "SD301")[0]
    assert "gaps" in f.title.lower()


def test_broken_rule_does_not_break_report(monkeypatch):
    from sparkmedic.analysis import engine

    def boom(ctx):
        raise RuntimeError("bad")

    monkeypatch.setitem(engine.RULES, "SD999", engine.RuleInfo("SD999", "boom", "x", "x", boom))
    diag = run_analysis(make_snapshot([make_stage(1, run_ms=1000)]), AnalysisConfig())
    assert any("SD999" in e for e in diag.errors)
    assert "Rule error" in diag.to_markdown()


def test_json_report_schema():
    diag = run_analysis(make_snapshot([make_stage(6, run_ms=3_600_000, jvm_gc_time_ms=0.3 * 3_600_000, num_tasks=128)]), AnalysisConfig())
    d = diag.to_dict()
    assert d["schema_version"] == "1.0" and d["findings"][0]["rule_id"] == "SD103"
    assert {"evidence", "recommendations", "references", "score"} <= set(d["findings"][0])
