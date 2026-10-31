from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


CAMPAIGN_CONTRACT = "streamsqlbench_rq2_official_baselines_campaign_v1"
METHOD_RESULT_CONTRACT = "streamsqlbench_rq2_method_result_v1"
BACKEND_RESULT_CONTRACT = "streamsqlbench_rq2_docker_backend_result_v1"
METHOD_FEEDBACK_RESULT_CONTRACT = "streamsqlbench_rq2_method_feedback_result_v1"
EVALUATION_RESULT_CONTRACT = "streamsqlbench_rq2_evaluation_result_v1"
LEDGER_EVENT_CONTRACT = "streamsqlbench_rq2_ledger_event_v1"


class FailureClass(str, Enum):
    INFRA = "infra"
    MODEL = "model"
    METHOD = "method"
    RUNTIME = "runtime"
    EVALUATOR = "evaluator"


class StageStatus(str, Enum):
    STARTED = "started"
    PASSED = "passed"
    FAILED = "failed"
    PAUSED = "paused"


class ContractError(ValueError):
    pass


OFFICIAL_STAGE_PROFILES: dict[str, tuple[str, ...]] = {
    "din_sql": (
        "schema_linking",
        "difficulty_classification",
        "class_specific_generation",
        "self_correction",
    ),
    "dail_sql": (
        "official_data_preprocessing",
        "masked_question_preliminary_selection_9shot",
        "five_preliminary_candidates_execution_sc",
        "preliminary_sql_skeleton",
        "skeleton_conditioned_selection_9shot",
        "five_final_candidates",
        "execution_based_self_consistency",
    ),
    "spider_agent": (
        "prompt_agent_thought_action_loop",
        "official_local_action_space",
        "isolated_task_workspace",
        "docker_flink_local_db_sql_feedback",
        "official_stop_condition",
        "candidate_sql_capture_from_final_successful_sql_action",
    ),
    "mac_sql": (
        "selector_schema_value_inspection",
        "decomposer_subquestion_generation",
        "refiner_docker_flink_execution_feedback",
        "chat_manager_routing_max_three_rounds",
    ),
}

REPAIRED_STAGE_PROFILES = dict(OFFICIAL_STAGE_PROFILES)
REPAIRED_STAGE_PROFILES["spider_agent"] = (
    *OFFICIAL_STAGE_PROFILES["spider_agent"][:-1],
    "candidate_sql_capture_from_terminated_final_sql_file",
)


@dataclass(frozen=True)
class Artifact:
    path: Path


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    public_input: Artifact
    task_package: Artifact
    gold_relation: Artifact
    evaluation_contract: Artifact
    artifact_sha256: Mapping[str, str] | None = None


@dataclass(frozen=True)
class CommandSpec:
    command: tuple[str, ...]
    timeout_seconds: int


@dataclass(frozen=True)
class MethodSpec:
    method_id: str
    repository: Path
    repository_url: str
    commit: str
    mode: str
    required_stages: tuple[str, ...]
    invocation: CommandSpec
    environment_allowlist: tuple[str, ...]


@dataclass(frozen=True)
class CampaignSpec:
    campaign_id: str
    manifest_path: Path
    tasks: tuple[TaskSpec, ...]
    methods: tuple[MethodSpec, ...]
    method_feedback_backend: CommandSpec
    docker_backend: CommandSpec
    evaluator: CommandSpec


FORBIDDEN_PUBLIC_KEYS = {
    "gold",
    "gold_sql",
    "gold_csv",
    "gold_relation",
    "oracle",
    "expected",
    "expected_rows",
    "evaluation_contract",
    "verdict",
    "hidden_scenarios",
}


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read JSON {path}: {exc}") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve(base: Path, raw: Any, field: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise ContractError(f"{field} must be a non-empty path")
    path = Path(raw)
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def _artifact(base: Path, row: Mapping[str, Any], field: str) -> Artifact:
    path = _resolve(base, row.get(field), field)
    if not path.is_file():
        raise ContractError(f"{field} does not exist: {path}")
    return Artifact(path)


def _command(row: Mapping[str, Any], field: str) -> CommandSpec:
    command = row.get("command")
    timeout = row.get("timeout_seconds")
    if not isinstance(command, list) or not command or not all(
        isinstance(part, str) and part for part in command
    ):
        raise ContractError(f"{field}.command must be a non-empty string array")
    if not isinstance(timeout, int) or timeout <= 0:
        raise ContractError(f"{field}.timeout_seconds must be a positive integer")
    return CommandSpec(tuple(command), timeout)


def _walk_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            keys.add(str(key).casefold())
            keys.update(_walk_keys(child))
    elif isinstance(value, list):
        for child in value:
            keys.update(_walk_keys(child))
    return keys


def validate_public_input(path: Path) -> None:
    value = _read_json(path)
    if not isinstance(value, Mapping):
        raise ContractError(f"public input must be a JSON object: {path}")
    leaked = sorted(FORBIDDEN_PUBLIC_KEYS & _walk_keys(value))
    if leaked:
        raise ContractError(f"public input contains evaluator-only keys {leaked}: {path}")


def _validate_no_hidden_placeholders(command: Sequence[str], field: str) -> None:
    joined = " ".join(command).casefold()
    forbidden = ("{gold", "{oracle", "{expected", "{evaluation_contract")
    leaked = [token for token in forbidden if token in joined]
    if leaked:
        raise ContractError(f"{field} exposes evaluator-only placeholders: {leaked}")


def load_campaign(path: Path) -> CampaignSpec:
    path = path.resolve()
    raw = _read_json(path)
    if not isinstance(raw, Mapping) or raw.get("contract") != CAMPAIGN_CONTRACT:
        raise ContractError(f"unsupported campaign contract: {path}")
    base = path.parent
    campaign_id = raw.get("campaign_id")
    if not isinstance(campaign_id, str) or not campaign_id.strip():
        raise ContractError("campaign_id must be a non-empty string")

    raw_tasks = raw.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ContractError("Agent campaign must contain at least one task")
    tasks: list[TaskSpec] = []
    seen_tasks: set[str] = set()
    for index, row in enumerate(raw_tasks):
        if not isinstance(row, Mapping):
            raise ContractError(f"tasks[{index}] must be an object")
        task_id = row.get("task_id")
        if not isinstance(task_id, str) or not task_id.strip() or task_id in seen_tasks:
            raise ContractError(f"tasks[{index}].task_id is empty or duplicated")
        seen_tasks.add(task_id)
        task = TaskSpec(
            task_id=task_id,
            public_input=_artifact(base, row, "public_input"),
            task_package=_artifact(base, row, "task_package"),
            gold_relation=_artifact(base, row, "gold_relation"),
            evaluation_contract=_artifact(base, row, "evaluation_contract"),
            artifact_sha256=row.get("artifact_sha256")
            if isinstance(row.get("artifact_sha256"), Mapping) else None,
        )
        expected_digests = task.artifact_sha256
        required_artifacts = {
            "public_input": task.public_input.path,
            "task_package": task.task_package.path,
            "gold_relation": task.gold_relation.path,
            "evaluation_contract": task.evaluation_contract.path,
        }
        if not isinstance(expected_digests, Mapping) or set(expected_digests) != set(required_artifacts):
            raise ContractError(f"{task_id}: artifact_sha256 must bind all four frozen artifacts")
        for name, artifact_path in required_artifacts.items():
            expected = expected_digests.get(name)
            if not isinstance(expected, str) or len(expected) != 64:
                raise ContractError(f"{task_id}: invalid {name} digest")
            if _sha256_file(artifact_path) != expected:
                raise ContractError(f"{task_id}: {name} digest mismatch")
        validate_public_input(task.public_input.path)
        if task.public_input.path in {
            task.gold_relation.path,
            task.evaluation_contract.path,
        }:
            raise ContractError(f"{task_id}: public input aliases hidden evaluation evidence")
        tasks.append(task)

    raw_methods = raw.get("methods")
    if not isinstance(raw_methods, list):
        raise ContractError("methods must be an array")
    methods: list[MethodSpec] = []
    seen_methods: set[str] = set()
    for index, row in enumerate(raw_methods):
        if not isinstance(row, Mapping):
            raise ContractError(f"methods[{index}] must be an object")
        method_id = row.get("method_id")
        if method_id not in OFFICIAL_STAGE_PROFILES or method_id in seen_methods:
            raise ContractError(f"unknown or duplicated official method: {method_id!r}")
        seen_methods.add(method_id)
        mode = row.get("mode")
        if mode != "official_unablated":
            raise ContractError(f"{method_id}: mode must be official_unablated")
        stages = tuple(row.get("required_stages") or ())
        profiles = REPAIRED_STAGE_PROFILES if raw.get("adapter_revision") == "target-adaptation-v3" else OFFICIAL_STAGE_PROFILES
        if stages != profiles[method_id]:
            raise ContractError(
                f"{method_id}: required_stages must exactly match the official profile"
            )
        invocation = _command(row.get("invocation") or {}, f"methods[{index}].invocation")
        _validate_no_hidden_placeholders(invocation.command, f"methods[{index}].invocation")
        if method_id in {"dail_sql", "spider_agent", "mac_sql"} and not any(
            "{runtime_tool}" in part for part in invocation.command
        ):
            raise ContractError(
                f"{method_id}: official control flow requires the Gold-free Docker runtime tool"
            )
        env_allowlist = tuple(row.get("environment_allowlist") or ())
        if not all(isinstance(name, str) and name for name in env_allowlist):
            raise ContractError(f"{method_id}: invalid environment_allowlist")
        methods.append(
            MethodSpec(
                method_id=method_id,
                repository=_resolve(base, row.get("repository"), f"{method_id}.repository"),
                repository_url=str(row.get("repository_url") or ""),
                commit=str(row.get("commit") or ""),
                mode=mode,
                required_stages=stages,
                invocation=invocation,
                environment_allowlist=env_allowlist,
            )
        )
    if seen_methods and seen_methods != set(OFFICIAL_STAGE_PROFILES):
        raise ContractError(
            f"campaign must contain all four official methods; found={sorted(seen_methods)}"
        )

    method_feedback_backend = _command(
        raw.get("method_feedback_backend") or {}, "method_feedback_backend"
    )
    _validate_no_hidden_placeholders(
        method_feedback_backend.command, "method_feedback_backend"
    )
    backend = _command(raw.get("docker_backend") or {}, "docker_backend")
    _validate_no_hidden_placeholders(backend.command, "docker_backend")
    evaluator = _command(raw.get("evaluator") or {}, "evaluator")
    return CampaignSpec(
        campaign_id=campaign_id,
        manifest_path=path,
        tasks=tuple(tasks),
        methods=tuple(methods),
        method_feedback_backend=method_feedback_backend,
        docker_backend=backend,
        evaluator=evaluator,
    )
