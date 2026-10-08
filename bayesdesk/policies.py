"""Deterministic task admission and acceptance policies; no NLP interpreter.

V0.2 intentionally supports a narrow, fail-closed schema. The research owner
chooses these predicates. The Leader and Broker only enforce them.
"""
from __future__ import annotations

from .contracts import exact_keys, resource_profile, validate_id, payload_fingerprint
from .errors import require

KINDS = frozenset({"test_report", "code_review", "diagnostic", "hypothesis_test", "experiment_note", "verification"})


def validate_input_constraints(rule: dict, inputs: dict) -> None:
    require(isinstance(rule, dict) and isinstance(inputs, dict) and set(inputs) == set(rule),
            "INPUT_NOT_APPROVED", "input names must match the task's approved schema")
    for name, spec in rule.items():
        validate_id(name, "input name")
        require(isinstance(spec, dict) and "type" in spec, "INVALID_POLICY", "invalid input constraint")
        value = inputs[name]
        if spec["type"] == "integer":
            exact_keys(spec, {"type", "min", "max"})
            require(type(value) is int and type(spec["min"]) is int and type(spec["max"]) is int
                    and spec["min"] <= value <= spec["max"], "INPUT_NOT_APPROVED", name)
        elif spec["type"] == "enum":
            exact_keys(spec, {"type", "choices"})
            require(isinstance(spec["choices"], list) and spec["choices"]
                    and isinstance(value, str) and value in spec["choices"], "INPUT_NOT_APPROVED", name)
        elif spec["type"] == "exact":
            exact_keys(spec, {"type", "value"})
            require(type(value) is type(spec["value"]) and value == spec["value"], "INPUT_NOT_APPROVED", name)
        else:
            require(False, "INVALID_POLICY", "unsupported input constraint")


def validate_execution_policy(policy: dict) -> dict:
    exact_keys(policy, {"recipes"})
    require(isinstance(policy["recipes"], dict), "INVALID_POLICY", "recipes must be an object")
    for recipe_id, recipe in policy["recipes"].items():
        validate_id(recipe_id, "recipe_id")
        exact_keys(recipe, {"profiles", "inputs", "max_timeout_seconds", "max_attempts"}, {"source_commits"})
        require(isinstance(recipe["profiles"], dict) and recipe["profiles"], "INVALID_POLICY", "profiles required")
        for name, profile in recipe["profiles"].items():
            validate_id(name, "profile_name")
            resource_profile(profile)
        require(isinstance(recipe["inputs"], dict), "INVALID_POLICY", "inputs must be an object")
        # Validate the constraint schema itself (not only at the first handoff).
        for name, spec in recipe["inputs"].items():
            validate_id(name, "input name")
            require(isinstance(spec, dict) and spec.get("type") in ("integer", "enum", "exact"),
                    "INVALID_POLICY", "unsupported input constraint")
            if spec["type"] == "integer":
                exact_keys(spec, {"type", "min", "max"})
                require(type(spec["min"]) is int and type(spec["max"]) is int and spec["min"] <= spec["max"],
                        "INVALID_POLICY", "bad integer input range")
            elif spec["type"] == "enum":
                exact_keys(spec, {"type", "choices"})
                require(isinstance(spec["choices"], list) and len(spec["choices"]) > 0
                        and all(isinstance(v, str) for v in spec["choices"]), "INVALID_POLICY", "bad choices")
            else:
                exact_keys(spec, {"type", "value"})
        require(type(recipe["max_timeout_seconds"]) is int and 1 <= recipe["max_timeout_seconds"] <= 604800,
                "INVALID_POLICY", "invalid timeout")
        require(type(recipe["max_attempts"]) is int and 1 <= recipe["max_attempts"] <= 1000,
                "INVALID_POLICY", "invalid attempt budget")
        if "source_commits" in recipe:
            from .contracts import HEX40
            require(isinstance(recipe["source_commits"], list) and all(
                isinstance(c, str) and HEX40.fullmatch(c) for c in recipe["source_commits"]),
                "INVALID_POLICY", "source commit allowlist must contain full SHAs")
    return policy


def authorize_handoff(policy: dict, recipe_id: str, source_commit: str, profiles: dict,
                      inputs: dict, timeout: int, prior_attempts: int) -> None:
    validate_execution_policy(policy)
    require(recipe_id in policy["recipes"], "RECIPE_NOT_APPROVED", "recipe is not task-approved")
    approved = policy["recipes"][recipe_id]
    require(profiles and all(name in approved["profiles"] and approved["profiles"][name] == p
                             for name, p in profiles.items()), "PROFILE_NOT_APPROVED", "profiles outside task policy")
    require(1 <= timeout <= approved["max_timeout_seconds"], "TIMEOUT_NOT_APPROVED", "timeout exceeds task policy")
    require(prior_attempts < approved["max_attempts"], "ATTEMPT_BUDGET_EXHAUSTED", "too many attempts")
    if approved.get("source_commits"):
        require(source_commit in approved["source_commits"], "SOURCE_NOT_APPROVED", "SHA not in task allowlist")
    validate_input_constraints(approved["inputs"], inputs)


def validate_acceptance(acceptance: dict) -> dict:
    require(isinstance(acceptance, dict) and acceptance.get("type") in ("job_exit", "evidence_only"),
            "INVALID_ACCEPTANCE", "expected job_exit or evidence_only")
    if acceptance["type"] == "job_exit":
        exact_keys(acceptance, {"type", "exit_code"}, {"required_evidence_kinds"})
        require(type(acceptance["exit_code"]) is int, "INVALID_ACCEPTANCE", "exit_code must be integer")
    else:
        exact_keys(acceptance, {"type", "required_evidence_kinds"})
    kinds = acceptance.get("required_evidence_kinds", [])
    require(isinstance(kinds, list) and all(k in KINDS for k in kinds),
            "INVALID_ACCEPTANCE", "unknown evidence kind")
    return acceptance
