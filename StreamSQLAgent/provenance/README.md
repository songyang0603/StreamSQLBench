# Run provenance

The paper reports one run of StreamSQLAgent 3.0.0 on all 402 tasks (GLM-5.1, thinking disabled): 320/402 EX and 304/402 under the strict row-wise check. Every task, including the failures, is in the denominator; no task was rerun.

- `code-listing.json`: the unchanged entries (path and SHA-1) of the run's file listing for the 86 files in this release; the complete listing has 196 files.
- `agent-config.json`: the unchanged run configuration (per-task budgets; campaign cap 402 x 120,000 tokens).
- `PROVENANCE.json`: run identity, the SHA-256 of the complete listing and of the archived run code.

Check that the code in this directory is the code of the run:

```bash
python StreamSQLAgent/provenance/verify.py
```

All code, prompt and configuration files must be identical. Documentation that was updated after the run to report its result is listed separately. The 110 files of the listing that this release does not include are the scripts that launched and monitored the run on the experiment server and internal development scripts; none of them is imported by the agent.
