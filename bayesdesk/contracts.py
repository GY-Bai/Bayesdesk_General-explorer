"""Versioned machine contracts. Broker never interprets natural language."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from .storage import dumps
from .errors import ContractError, require

CONTRACT_VERSION = 1
HEX40 = re.compile(r"^[0-9a-fA-F]{40}$")
HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")


def validate_id(value: str, label: str) -> None:
    require(isinstance(value, str) and ID.fullmatch(value) is not None,
            "INVALID_ID", f"{label} must be an ASCII identifier (1-100 chars)")


def exact_keys(obj: dict, required: set, optional: set = frozenset()):
    require(isinstance(obj, dict), "INVALID_SCHEMA", "expected JSON object")
    missing, extra = required - obj.keys(), obj.keys() - required - optional
    require(not missing and not extra, "INVALID_SCHEMA", f"missing={sorted(missing)}, unexpected={sorted(extra)}")


def resource_profile(value: dict) -> dict:
    exact_keys(value, {"cpu_units", "memory_mib", "gpu_count"})
    for k in ("cpu_units", "memory_mib", "gpu_count"):
        require(type(value[k]) is int, "INVALID_RESOURCE", f"{k} must be integer")
    require(value["cpu_units"] >= 1 and value["memory_mib"] >= 64,
            "INVALID_RESOURCE", "CPU >= 1 and memory >= 64 MiB required")
    require(value["gpu_count"] in (0, 1), "INVALID_RESOURCE", "V1 supports at most one exclusive GPU")
    return value


def validate_job(job: dict) -> dict:
    exact_keys(job, {"schema_version", "handoff_id", "task_id", "attempt_id", "decision_id",
                     "generation", "source_commit", "recipe_id", "inputs", "profile_name",
                     "profile", "timeout_seconds", "permit"})
    require(job["schema_version"] == 1, "UNSUPPORTED_SCHEMA", "expected schema_version=1")
    for k in ("handoff_id", "task_id", "attempt_id", "decision_id", "recipe_id", "profile_name"):
        validate_id(job[k], k)
    require(type(job["generation"]) is int and job["generation"] >= 1, "INVALID_SCHEMA", "generation must be positive integer")
    require(isinstance(job["source_commit"], str) and HEX40.fullmatch(job["source_commit"]),
            "INVALID_SCHEMA", "source_commit must be a full Git SHA-1 hex string")
    require(type(job["timeout_seconds"]) is int and 1 <= job["timeout_seconds"] <= 7 * 86400,
            "INVALID_SCHEMA", "timeout_seconds must be between 1 and 604800")
    require(isinstance(job["inputs"], dict), "INVALID_SCHEMA", "inputs must be a JSON object")
    resource_profile(job["profile"])
    return job


def sign_permit(secret: str, payload: dict) -> dict:
    frozen = json.loads(dumps(payload))
    serialized = dumps(frozen)
    signature = hmac.new(secret.encode(), serialized.encode(), hashlib.sha256).hexdigest()
    return {"claims": frozen, "signature": signature}


def verify_permit(secret: str, job: dict) -> None:
    permit = job["permit"]
    exact_keys(permit, {"claims", "signature"})
    claims = permit["claims"]
    exact_keys(claims, {"handoff_id", "task_id", "attempt_id", "decision_id", "generation",
                        "recipe_id", "source_commit", "allowed_profiles", "max_timeout_seconds", "expires_at"})
    expected = sign_permit(secret, claims)["signature"]
    require(isinstance(permit["signature"], str) and hmac.compare_digest(permit["signature"], expected),
            "INVALID_PERMIT", "permit signature mismatch")
    require(type(claims["expires_at"]) is int and time.time() < claims["expires_at"],
            "EXPIRED_PERMIT", "worker handoff permit expired")
    for key in ("handoff_id", "task_id", "attempt_id", "decision_id", "generation", "recipe_id", "source_commit"):
        require(job[key] == claims[key], "INVALID_PERMIT", f"permit does not authorize {key}")
    profiles = claims["allowed_profiles"]
    require(isinstance(profiles, dict) and job["profile_name"] in profiles,
            "PROFILE_NOT_APPROVED", "resource profile not approved")
    require(job["profile"] == resource_profile(profiles[job["profile_name"]]),
            "PROFILE_NOT_APPROVED", "profile resources changed after approval")
    require(type(claims["max_timeout_seconds"]) is int and job["timeout_seconds"] <= claims["max_timeout_seconds"],
            "TIMEOUT_NOT_APPROVED", "job exceeds approved walltime")


def payload_fingerprint(value: dict) -> str:
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def sign_event(secret: str, event: dict) -> dict:
    frozen = json.loads(dumps(event))
    frozen.pop("signature", None)
    signature = hmac.new(secret.encode(), dumps(frozen).encode(), hashlib.sha256).hexdigest()
    return {**frozen, "signature": signature}


def verify_event(secret: str, event: dict) -> None:
    require(isinstance(event, dict) and isinstance(event.get("signature"), str),
            "INVALID_EVENT_SIGNATURE", "missing event signature")
    expected = sign_event(secret, event)["signature"]
    require(hmac.compare_digest(event["signature"], expected),
            "INVALID_EVENT_SIGNATURE", "event signature mismatch")
