"""Parsing and formatting helpers for sizes, durations and Spark UI metric strings."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

_SIZE_UNITS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
    "PiB": 1024**5,
    # Some UIs print decimal-looking units; treat them as binary like Spark does.
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}
_TIME_UNITS_MS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1000.0, "m": 60_000.0, "min": 60_000.0, "h": 3_600_000.0}

_VALUE_RE = re.compile(
    r"(-?\d[\d,]*(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*(PiB|TiB|GiB|MiB|KiB|TB|GB|MB|KB|B|ns|us|ms|min|s|m|h)?(?![A-Za-z])"
)
_STAGE_TASK_RE = re.compile(r"\(\s*stage[^)]*\)")


@dataclass
class ParsedMetric:
    """A Spark SQL metric value parsed from its UI string."""

    text: str
    unit: str  # "bytes" | "ms" | "count"
    total: Optional[float] = None
    min: Optional[float] = None
    med: Optional[float] = None
    max: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "text": self.text,
            "unit": self.unit,
            "total": self.total,
            "min": self.min,
            "med": self.med,
            "max": self.max,
        }


def _to_number(num: str, unit: Optional[str]) -> tuple[float, str]:
    value = float(num.replace(",", ""))
    if not unit:
        return value, "count"
    if unit in _SIZE_UNITS:
        return value * _SIZE_UNITS[unit], "bytes"
    return value * _TIME_UNITS_MS[unit], "ms"


def parse_metric_text(text: str) -> ParsedMetric:
    """Parse strings such as ``"1,000,000"``, ``"254.0 MiB"`` or
    ``"total (min, med, max (stageId: taskId))\\n27.7 m (50 ms, 10.2 s, 18.9 s (stage 23.0: task 679))"``.
    """
    raw = text or ""
    value_line = raw.strip().splitlines()[-1] if raw.strip() else ""
    header = raw.strip().splitlines()[0] if "\n" in raw.strip() else ""
    cleaned = _STAGE_TASK_RE.sub("", value_line)
    tokens = _VALUE_RE.findall(cleaned)
    if not tokens:
        return ParsedMetric(text=raw, unit="count")
    parsed = [_to_number(n, u) for n, u in tokens]
    total, unit = parsed[0]
    result = ParsedMetric(text=raw, unit=unit, total=total)
    has_distribution = "min, med, max" in header or (
        "(" in cleaned and len(parsed) >= 4 and all(p[1] == unit for p in parsed[:4])
    )
    if has_distribution and len(parsed) >= 4:
        result.min, result.med, result.max = parsed[1][0], parsed[2][0], parsed[3][0]
    return result


def parse_spark_time(value: Optional[str]) -> Optional[int]:
    """Parse Spark REST timestamps like ``2026-09-23T05:09:28.599GMT`` to epoch ms."""
    if not value:
        return None
    v = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fGMT", "%Y-%m-%dT%H:%M:%SGMT", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            dt = datetime.strptime(v, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def fmt_bytes(n: Optional[float]) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or unit == "PiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def fmt_ms(ms: Optional[float]) -> str:
    if ms is None:
        return "n/a"
    ms = float(ms)
    if ms < 1000:
        return f"{ms:.0f} ms"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f} s"
    m = s / 60
    if m < 60:
        return f"{m:.1f} min"
    return f"{m / 60:.1f} h"


def fmt_count(n: Optional[float]) -> str:
    if n is None:
        return "n/a"
    return f"{int(round(n)):,}"


def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:.0f}%" if x >= 0.01 or x == 0 else f"{x * 100:.1f}%"


def safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b
