# Released implementation notes

Version 3.0.0 replaces the earlier two-role verification loop with the six-role
generate–execute–verify loop described in `METHOD.md`:

| Component | Module |
|---|---|
| Coordinator (loop, budgets, candidates, selection, resumable journal) | `streamsqlagent/agent.py` |
| Roles and their information boundaries | `streamsqlagent/roles.py`, `streamsqlagent/prompts/` |
| Data Explorer mirror and profile | `streamsqlagent/data_explorer.py` |
| Registered schema and time attributes | `streamsqlagent/engine_profile.py` |
| Engine-limitation rules and SQL lints | `streamsqlagent/engine_knowledge.py` |
| Feedback Observer (result summaries, located diagnoses) | `streamsqlagent/observer.py` |
| Execution Worker tools (EXPLAIN, complete-input runs) | `streamsqlagent/plan_tool.py`, `streamsqlagent/runtime_bridge.py` |
| Campaign runner, formal runtime and evaluation (unchanged) | `streamsqlagent/campaign.py`, `runtime_support/`, `scripts/` |

Every recommended form in `engine_knowledge.py` was planned successfully by the
Flink 1.20.5 SQL Gateway of the benchmark runtime, and every quoted failure
message was reproduced there. The protected-information guard is applied to
inputs; model outputs cannot contain evaluator material because no role
receives it.
