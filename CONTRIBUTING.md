# Contributing

Thanks for helping make Spark jobs faster.

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[all,dev]"
pytest
ruff check src tests examples
```

The core package has no runtime dependencies on purpose, so it installs on any Databricks cluster.
Optional features (Databricks SDK, MCP, zstd) are imported lazily.

## Adding a rule

1. Add a function decorated with `@rule("SDxxx", name, category, description)` in the matching module under
   `src/sparkmedic/analysis/rules/` (stage, plan, app or config).
2. Use `AnalysisContext` helpers instead of re-deriving metrics, and put every threshold in `Thresholds`.
3. Every finding needs: plain-language summary, concrete evidence (numbers from the run), a root cause,
   at least one recommendation, and references from `analysis/knowledge.py` (official Spark or Databricks docs).
4. Add a test in `tests/test_rules.py` with a synthetic snapshot that triggers the rule and one that must not.

## Test fixtures

`tests/fixtures/rest` holds recorded REST responses and `tests/fixtures/eventlog` the matching event log,
both produced by `examples/demo_slow_job.py`. Never commit data from real workloads unless it is anonymized.

## Pull requests

Keep PRs focused, add tests, and update the rule table in `README.md` when you add or change a rule.
