"""Stable error codes for machine clients. Never parse exception prose to make decisions."""
from __future__ import annotations


class ContractError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable

    def as_dict(self) -> dict:
        return {"ok": False, "error": {"code": self.code, "message": str(self), "retryable": self.retryable}}


def require(condition: bool, code: str, message: str) -> None:
    if not condition:
        raise ContractError(code, message)
