# sparkmedic

**Your Spark job is sick. sparkmedic tells you why, and how to fix it.**

`sparkmedic` reads the same data you see in the Spark UI (stages, task metrics, SQL plans, executors)
through the documented [Spark monitoring REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
or from Spark event logs. It then runs a set of diagnosis rules and gives you a ranked report:

- **what** is slow (stage, query, share of total task time),
- **why** (the evidence from the metrics and the physical plan),
- **what to do** (code changes, Spark configs, cluster/table changes), each linked to the official
  Apache Spark or Databricks documentation.

It works on open-source Spark, Spark History Server and Databricks, and ships as a **CLI**, a **Python API**
(usable inside notebooks) and an **MCP server** so AI assistants such as Claude can run the diagnosis for you.

```text
$ sparkmedic analyze databricks://0923-123456-abcdefgh

1. [CRITICAL] Compute-bound Stage 26 (involves 95% of total task time)
   - I/O rate 0.37 MB/s per core: not I/O bound. CPU 97% of task time, GC 1%.
   - The plan for this stage calls variant_get() 776 times on column 'periodic_json'.
   Fix: cast the VARIANT to a STRUCT once, then select the fields. Then try Photon.
2. [LOW] Spill to disk in Stage 26: only 3% of the data moved, a minor cost here.
...
5. [LOW] The same input is read 2 times (ExistingRDD, e.g. a foreachBatch micro-batch)
   Fix: persist the micro-batch once, run every action on it, then unpersist.
```

---

## Install

```bash
pip install sparkmedic                 # core: zero dependencies (Python 3.10+)
pip install "sparkmedic[databricks]"   # + remote Databricks clusters (Databricks SDK)
pip install "sparkmedic[mcp]"          # + MCP server
pip install "sparkmedic[all]"          # everything, incl. zstd event logs
```

From source:

```bash
git clone https://github.com/brianzavareh/sparkmedic.git
cd sparkmedic
pip install -e ".[all,dev]"
pytest
```

## Quick start

`sparkmedic analyze <TARGET>` works the same for every data source:

| Where your app is | TARGET |
|---|---|
| Running app, driver UI | `http://driver-host:4040` |
| Spark History Server | `http://history-host:18080` |
| Databricks cluster (from your laptop) | `databricks://<cluster-id>` |
| Databricks job run (any task cluster) | `databricks://run/<run-id>` (add `?task=<task_key>` to pick a task) |
| Inside a Databricks notebook | `notebook` (Python API) |
| Event log files / folder / zip | `./spark-events`, `app-123.zip` |
| Databricks delivered logs | `/Volumes/<cat>/<schema>/<vol>/<cluster-id>/eventlog` or `dbfs:/cluster-logs/<cluster-id>/eventlog` |
| Saved snapshot | `snapshot.json` |

```bash
sparkmedic apps http://localhost:18080                   # list applications
sparkmedic analyze http://localhost:18080 --app-id app-2026...  # analyze one
sparkmedic analyze ./spark-events -o reports/ --format both      # write Markdown + JSON + snapshot
sparkmedic analyze databricks://0923-123456-abcdefgh --focus-stage 26
sparkmedic collect http://localhost:4040 -o snap.json            # save now, analyze later / elsewhere
sparkmedic rules                                                  # list all rules
```

Before collecting, `analyze` shows **which application and scope** it will analyze and asks you to confirm
(`-y` skips the prompt; non-interactive runs proceed automatically). Useful flags:

- `--focus-stage ID` / `--focus-sql ID` (repeatable): report only on the stage/query you care about
- `--min-severity medium`: hide low-severity findings
- `--min-stage-share 0.02`: also analyze smaller stages (default: stages using at least 5% of task time)
- `--disable SD403`: turn a rule off
- `--fail-on high`: exit with code 2 when a finding of that severity exists (CI / scheduled checks)

### Inside a Databricks notebook

```python
%pip install sparkmedic
from sparkmedic import diagnose

result = diagnose("notebook")          # current Spark app on this cluster, no token needed
result.display()                       # renders the report
result.save("/Volumes/main/default/perf_reports/")
```

See [`examples/databricks_notebook.py`](examples/databricks_notebook.py).

### Python API

```python
from sparkmedic import collect, analyze, diagnose
from sparkmedic.analysis.engine import AnalysisConfig, Thresholds

snap = collect("http://localhost:18080", app_id="app-20260922-0001")
diag = analyze(snap, AnalysisConfig(thresholds=Thresholds(min_stage_share=0.02)))
print(diag.to_markdown())
data = diag.to_dict()          # stable JSON schema (schema_version 1.0)
```

## Authentication (no hard-coded credentials)

Secrets are **never** accepted as command-line arguments or tool parameters, never logged, and never written
to reports or snapshots (Spark conf values that look like secrets are redacted).

| Target | How credentials are provided |
|---|---|
| Spark UI / History Server | none, or `SPARKMEDIC_TOKEN` (bearer), or `SPARKMEDIC_USERNAME` + `SPARKMEDIC_PASSWORD` (basic). Use `--token-env NAME` to read a different variable. |
| Databricks remote | [Databricks unified authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth) through the official SDK: OAuth (U2M `databricks auth login`, M2M service principal), Azure CLI / managed identity, or a config profile (`--profile`). |
| Databricks notebook | the notebook's own short-lived context token, in memory only (only when the driver-local API is not reachable). |

TLS certificates are verified by default (`--ca-bundle` for private CAs). Credentials are dropped if a
server redirects to a different host.

## How Databricks access works

1. **`notebook`**: reads the driver's local Spark UI API (`sc.uiWebUrl`). If the SparkContext is not
   available (standard/shared access mode), it falls back to the workspace driver proxy.
2. **`databricks://<cluster-id>`** on a running cluster: calls the Spark REST API through the workspace
   driver proxy `https://<workspace>/driver-proxy-api/o/<org-id>/<cluster-id>/40001/api/v1/`, authenticated
   with the Databricks SDK. The org id is detected automatically. Note: this proxy route is widely used but only
   community-documented.
3. **Terminated clusters / job clusters**: falls back to the Spark event logs written by
   [compute log delivery](https://docs.databricks.com/aws/en/compute/configure#compute-log-delivery)
   (Volumes or DBFS), downloads them with the SDK and replays them locally. Turn on log delivery for job
   clusters you want to diagnose after they finish. S3 destinations: copy the folder locally and pass the path.

Serverless compute does not expose the Spark UI API; use the query profile or `system.query.history` there.

## Using it from Claude and other AI assistants (MCP)

`sparkmedic-mcp` is a stdio [Model Context Protocol](https://modelcontextprotocol.io) server with read-only tools:

| Tool | What it does |
|---|---|
| `list_applications` | apps at a target |
| `plan_diagnosis` | resolves the app and scope; the assistant confirms this with you before continuing |
| `diagnose` | collects + analyzes; returns the JSON report, Markdown and a `snapshot_id` |
| `get_stage_details` | full stage metrics, quantiles and slowest tasks |
| `get_sql_plan` | physical plan and per-operator metrics |
| `list_rules` | rule catalog |

Claude Code:

```bash
claude mcp add sparkmedic -- sparkmedic-mcp
```

Claude Desktop / Cursor / other clients (`mcpServers` config):

```json
{
  "mcpServers": {
    "sparkmedic": {
      "command": "sparkmedic-mcp",
      "env": { "DATABRICKS_CONFIG_PROFILE": "DEFAULT" }
    }
  }
}
```

Then ask: *"Why is the job on databricks://0923-123456-abcdefgh slow?"*. The server works with MCP Python SDK 1.x and 2.x.

## What it checks

The stage rules follow the Databricks
[Spark UI diagnosis guide](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/): find the stages that
use the most task time, check them for skew and spill, decide whether they are I/O bound, and otherwise look
for the compute cost in the physical plan.

| Rule | Detects | Main evidence |
|---|---|---|
| SD101 | Data skew / straggler tasks | max task duration > 1.5x the 75th percentile; per-task bytes/records |
| SD102 | Spill to disk | memory/disk spill, per-task spill, bytes per partition |
| SD103 | GC pressure | JVM GC time / task time |
| SD104 | Compute-bound stage (little I/O) | I/O MB/s per core, CPU share, rows/s; plan scan for repeated JSON/VARIANT extraction, Python UDFs, regex, very wide rows |
| SD105 | I/O-bound stage | largest of input/output/shuffle per core-second |
| SD106 | Low parallelism / huge partitions | tasks vs cores, bytes per task |
| SD107 | Tiny tasks / scheduling overhead | median task time, scheduler delay |
| SD108 | Shuffle fetch wait | fetch wait / task time |
| SD201 | Python UDFs | BatchEvalPython / ArrowEvalPython / pandas operators |
| SD202 | Repeated JSON/VARIANT parsing | many `get_json_object` / `variant_get` calls on one column |
| SD203 | Nested-loop / cartesian joins | BroadcastNestedLoopJoin, CartesianProduct |
| SD204 | Shuffle join that could broadcast | sort-merge join input sizes |
| SD205 | Row explosion | output rows >> input rows (explode, many-to-many joins) |
| SD206 | Small files (read and write) | files read/written, average size < 8 MB |
| SD207 | Same data read repeatedly | same table / `Scan ExistingRDD` in several queries (foreachBatch pattern) |
| SD301 | Driver-side gaps / idle cluster | time with no running jobs, core utilization |
| SD302 | Task/stage failures | failure reasons grouped (OOM, fetch failed, lost nodes, exceptions) |
| SD303 | Executors lost | remove reasons (OOM, spot, excluding autoscaling) |
| SD304 | Many small jobs | job count and median duration |
| SD305 | Large results to driver | result size per stage |
| SD306 | Heap almost full | peak JVM heap vs `spark.executor.memory` |
| SD401-405 | Configuration | AQE disabled, fixed shuffle partitions vs data size, Photon off (Databricks), broadcast disabled, low CPU efficiency |

All thresholds live in `sparkmedic.analysis.engine.Thresholds` and can be tuned.

## Report format

See a full example: [`examples/sample_report.md`](examples/sample_report.md) (and the matching
[`sample_report.json`](examples/sample_report.json)), built from a real-world pattern: a Databricks
`foreachBatch` MERGE where 776 `variant_get` calls per row dominated the runtime.

- **Markdown**: summary, ranked findings (evidence, why it is slow, what to do with code/config snippets,
  references), longest stages and queries, data collection notes, and a **"Confirm before acting"** checklist.
- **JSON** (`schema_version: "1.0"`): the same content for automation, plus a `score` per finding.
- **Snapshot**: the normalized, redacted data behind the report. Share it or re-analyze it later
  (`sparkmedic analyze snapshot.json`).

## Try it locally

```bash
pip install pyspark "sparkmedic[zstd]"
python examples/demo_slow_job.py --keep-alive 600     # a job with deliberate problems, UI on :4040
sparkmedic analyze http://localhost:4040
sparkmedic analyze ./spark-events                    # same result from the event log
```

## Project layout

```text
src/sparkmedic/
  collectors/   rest.py (Spark REST API), eventlog.py (event log replay), databricks.py, targets.py
  analysis/     engine.py (rules, findings, thresholds), context.py (derived metrics),
                patterns.py (plan scanning), knowledge.py (doc links), rules/*.py
  report/       markdown.py, json_report.py
  cli.py, mcp_server.py, api.py, model.py (normalized snapshot), http.py, auth.py
tests/          unit + integration tests (fake Spark UI, fake Databricks workspace, recorded fixtures)
examples/       demo_slow_job.py, databricks_notebook.py, sample_report.md/json
```

## Limitations

- The live UI only keeps the most recent stages/tasks (`spark.ui.retained*`); use event logs or the History
  Server for very long applications.
- Event logs compressed with Spark's `lz4`/`snappy`/`lzf` framing are not readable here; use
  `spark.eventLog.compression.codec=zstd` (the Spark 4 default) or analyze through a History Server.
- Plan-to-stage mapping uses the stage ids recorded in SQL metrics; when unavailable, the whole query plan
  is scanned instead (reported as such).
- Recommendations are heuristics. Validate changes on a sample and compare outputs before rolling them out.

## Security

Read-only GET requests only; see [SECURITY.md](SECURITY.md) for reporting vulnerabilities.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE).
