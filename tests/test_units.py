from sparkmedic.units import fmt_bytes, fmt_ms, parse_metric_text, parse_spark_time


def test_plain_count():
    m = parse_metric_text("44,997,492")
    assert m.unit == "count" and m.total == 44_997_492


def test_size():
    m = parse_metric_text("254.0 MiB")
    assert m.unit == "bytes" and m.total == 254 * 1024**2


def test_timing_with_distribution_and_stage_hint():
    text = "total (min, med, max (stageId: taskId))\n27.7 m (50 ms, 10.2 s, 18.9 s (stage 23.0: task 679))"
    m = parse_metric_text(text)
    assert m.unit == "ms"
    assert round(m.total) == round(27.7 * 60_000)
    assert m.min == 50 and m.med == 10_200 and m.max == 18_900


def test_databricks_size_distribution():
    m = parse_metric_text("15.0 KiB (72.0 B, 72.0 B, 816.0 B)")
    assert m.total == 15 * 1024 and m.min == 72 and m.max == 816


def test_non_metric_text():
    m = parse_metric_text("estimated rows output 1 (1X)")
    assert m.total == 1


def test_time_parse_and_format():
    assert parse_spark_time("2026-09-23T05:09:28.599GMT") == 1790140168599
    assert fmt_bytes(1536) == "1.5 KiB"
    assert fmt_ms(90_000) == "1.5 min"
