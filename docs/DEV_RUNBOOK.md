## ASM MVP Dev Runbook

- **Scope files**:
  - Runtime scope files used by the scanner are generated under `/app/scope`.
  - They are created by the API when calling `GET /api/<target>/launch_scan`, using `targets.domains` from MongoDB.
  - In `docker-compose`, the `api` and `worker` services share a volume mounted at `/app/scope`, so the worker sees the same generated scope files.
  - Do not try to edit files in `/app/scope` by hand; change the `targets` documents instead.

