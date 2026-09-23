# Security policy

## Design

- Read-only: the tool only sends HTTP GET/HEAD requests and reads local files.
- No credentials in arguments: tokens come from environment variables or the Databricks SDK's unified
  authentication. MCP tools never accept secrets as parameters.
- Secrets are not persisted: Spark configuration values whose keys look like secrets (password, token, key,
  credential, SAS, `{{secrets/...}}` references, ...) are redacted before anything is saved or reported.
- TLS verification is on by default. Authorization headers are dropped on cross-host redirects.
- Zip archives are checked for path traversal before extraction.

Reports contain query plans, table names and file paths from your workload. Review them before sharing publicly.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting ("Security" tab ->
"Report a vulnerability") on this repository. We aim to respond within 5 working days.
