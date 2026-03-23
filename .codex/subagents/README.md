# Bolt Subagents

This directory defines repo-local Codex subagent configurations for working on Bolt.

The current set is workflow-oriented rather than directory-oriented.

The Bolt build workflow now lives in the repo-local skill at [.codex/skills/build/SKILL.md](/Users/bytedance/projects/bolt-github/.codex/skills/build/SKILL.md).

The remaining subagents are:

- `validate-debug`: testing, benchmarking, debugging, and trace-based investigation.
- `api-integration`: external-facing APIs, connectors, storage adapters, and integration contracts.
- `perf-vectorization`: hot-path optimization, vectorization, and runtime efficiency.
- `semantics-correctness`: wrong-result bugs, SQL semantics, type behavior, and parity-sensitive logic.

The canonical manifest is [manifest.yaml](/Users/bytedance/projects/bolt-github/.codex/subagents/manifest.yaml).
