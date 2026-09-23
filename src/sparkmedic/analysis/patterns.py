"""Recognizers for expensive operators and expressions in physical plans."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

PYTHON_UDF_NODES = (
    "BatchEvalPython",
    "ArrowEvalPython",
    "MapInPandas",
    "MapInArrow",
    "FlatMapGroupsInPandas",
    "FlatMapCoGroupsInPandas",
    "FlatMapGroupsInArrow",
    "FlatMapCoGroupsInArrow",
    "AggregateInPandas",
    "WindowInPandas",
    "BatchEvalPythonUDTF",
    "ArrowEvalPythonUDTF",
    "PythonMapInArrow",
)
ROW_AT_A_TIME_PY = ("BatchEvalPython", "BatchEvalPythonUDTF")

_JSON_CALL = re.compile(
    r"\b(get_json_object|json_tuple|from_json|variant_get|try_variant_get|schema_of_json)\s*\(\s*([A-Za-z_][\w.]*?)(?:#\d+L?)?\s*[,)]",
    re.I,
)
_PARSE_JSON = re.compile(r"\b(try_parse_json|parse_json)\s*\(", re.I)
_REGEX = re.compile(r"\b(rlike|regexp_extract_all|regexp_extract|regexp_replace|regexp_like|regexp_count|regexp_substr|regexp_instr)\s*\(|\sRLIKE\s", re.I)
_OUTPUT_WIDTH = re.compile(r"Output \[(\d+)\]")
_SCALA_UDF = re.compile(r"\bUDF(?:\([^)]*\))?\(|\budf\(", re.I)
_COLLECT_AGG = re.compile(r"\b(collect_list|collect_set|array_agg)\s*\(", re.I)


@dataclass
class PlanHits:
    json_calls: Counter = field(default_factory=Counter)  # (function, column) -> count
    parse_json_calls: int = 0
    regex_calls: int = 0
    python_udf_nodes: Counter = field(default_factory=Counter)
    scala_udf_calls: int = 0
    generate_nodes: int = 0
    window_nodes: int = 0
    sort_nodes: int = 0
    collect_aggs: int = 0
    max_output_width: int = 0

    @property
    def json_total(self) -> int:
        return sum(self.json_calls.values())

    def top_json_columns(self, n: int = 3) -> list[tuple[str, str, int]]:
        return [(fn, col, c) for (fn, col), c in self.json_calls.most_common(n)]

    def is_empty(self) -> bool:
        return not (
            self.json_calls
            or self.parse_json_calls
            or self.regex_calls
            or self.python_udf_nodes
            or self.scala_udf_calls
            or self.generate_nodes
            or self.collect_aggs
        )


def scan_plan_text(text: str, node_names: list[str] | None = None) -> PlanHits:
    hits = PlanHits()
    if not text:
        return hits
    seen_blocks = set()
    for m in _JSON_CALL.finditer(text):
        # Count each distinct expression once, even if the plan prints it twice.
        span_text = text[m.start() : text.find(")", m.end()) + 1]
        key = (m.group(1).lower(), m.group(2), span_text)
        if key in seen_blocks:
            continue
        seen_blocks.add(key)
        hits.json_calls[(m.group(1).lower(), m.group(2))] += 1
    hits.parse_json_calls = len(_PARSE_JSON.findall(text))
    hits.regex_calls = len(_REGEX.findall(text))
    hits.scala_udf_calls = len(_SCALA_UDF.findall(text))
    hits.collect_aggs = len(_COLLECT_AGG.findall(text))
    widths = [int(w) for w in _OUTPUT_WIDTH.findall(text)]
    hits.max_output_width = max(widths) if widths else 0
    for name in node_names or []:
        base = name.split(" ")[0]
        base = base[len("Photon") :] if base.startswith("Photon") else base
        if base in PYTHON_UDF_NODES:
            hits.python_udf_nodes[base] += 1
        elif base == "Generate":
            hits.generate_nodes += 1
        elif base in ("Window", "WindowGroupLimit", "RunningWindowFunction"):
            hits.window_nodes += 1
        elif base == "Sort":
            hits.sort_nodes += 1
    if not node_names:
        for p in PYTHON_UDF_NODES:
            c = len(re.findall(rf"\b{p}\b", text))
            if c:
                hits.python_udf_nodes[p] += c
        hits.generate_nodes = len(re.findall(r"\bGenerate\b", text))
    return hits


def scan_relation_name(node_name: str, description: str) -> str:
    """Best-effort identity of the data source read by a scan node."""
    name = node_name.strip()
    m = re.match(r"^(?:Photon)?(?:File)?Scan\s+(\w+)\s+(\S+)", name)
    if m:  # Databricks: "Scan parquet catalog.schema.table"
        return m.group(2)
    if "ExistingRDD" in name or "ExistingRDD" in description:
        return "ExistingRDD (pre-computed DataFrame, e.g. a foreachBatch micro-batch)"
    loc = re.search(r"Location: \w+ \[([^\]]+)\]", description)
    if loc:
        return loc.group(1).split(",")[0].strip()
    tbl = re.search(r"(?:Relation|Table)\s*[:=]?\s*\[?([\w`.]+\.[\w`.]+)", description)
    if tbl:
        return tbl.group(1)
    return name
