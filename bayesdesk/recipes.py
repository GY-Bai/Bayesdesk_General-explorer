"""Operator-registered, non-shell command recipes and pinned source worktrees."""
from __future__ import annotations
from pathlib import Path
import re
import subprocess
from .errors import ContractError, require
from .contracts import exact_keys


def render_recipe(recipe: dict, inputs: dict) -> tuple[list[str], str]:
    exact_keys(recipe, {"argv", "cwd", "parameters"}, {"source"})
    require(isinstance(recipe["argv"], list) and bool(recipe["argv"]) and
            all(isinstance(s, str) and s for s in recipe["argv"]),
            "INVALID_RECIPE", "recipe argv must be a nonempty list of strings")
    cwd = Path(recipe["cwd"]).resolve(strict=True)
    require(cwd.is_dir(), "INVALID_RECIPE", "recipe cwd must be a directory")
    require(isinstance(recipe["parameters"], dict) and isinstance(inputs, dict), "INVALID_INPUT", "parameters/inputs must be objects")
    require(set(inputs) == set(recipe["parameters"]), "INVALID_INPUT", "inputs must exactly match recipe parameter names")
    argv = list(recipe["argv"])
    for name, definition in recipe["parameters"].items():
        require(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", name) is not None,
                "INVALID_RECIPE", "invalid parameter name")
        exact_keys(definition, {"type", "flag"}, {"min", "max", "root", "choices"})
        value = inputs[name]
        typ = definition["type"]
        if typ == "integer":
            require(type(value) is int, "INVALID_INPUT", f"{name} requires integer")
            require(definition.get("min", value) <= value <= definition.get("max", value), "INVALID_INPUT", f"{name} out of range")
            text = str(value)
        elif typ == "enum":
            require(isinstance(value, str) and value in definition.get("choices", []), "INVALID_INPUT", f"{name} not allowed")
            text = value
        elif typ == "path":
            require(isinstance(value, str) and not Path(value).is_absolute(), "INVALID_INPUT", f"{name} must be relative")
            root = Path(definition["root"]).resolve(strict=True)
            resolved = (root / value).resolve(strict=True)
            require(resolved.is_file() and resolved.is_relative_to(root), "INVALID_INPUT", f"{name} outside allowed directory")
            text = str(resolved)
        else:
            raise ContractError("INVALID_RECIPE", f"unknown input type: {typ}")
        flag = definition["flag"]
        require(isinstance(flag, str) and flag.startswith("--") and re.fullmatch(r"--[a-zA-Z0-9-]+", flag),
                "INVALID_RECIPE", "parameter flag must be an allowlisted long option")
        argv.extend([flag, text])
    return argv, str(cwd)


def materialize_source(recipe: dict, commit: str, job_dir: Path, default_cwd: str,
                       require_pinned: bool = True) -> str:
    """Launch against an exact Git commit, not a mutable shared source checkout.

    source: {repo_root: operator-controlled path, workdir: relative path}; no worker
    supplied shell or repo path. Git worktrees live under per-job directories.
    """
    source = recipe.get("source")
    if source is None:
        require(not require_pinned, "SOURCE_PIN_REQUIRED", "production executor requires a pinned Git source")
        return default_cwd
    exact_keys(source, {"repo_root", "workdir"})
    repo_root = Path(source["repo_root"]).resolve(strict=True)
    require(repo_root.is_dir(), "INVALID_RECIPE", "source repo must be directory")
    subdir = Path(source["workdir"])
    require(not subdir.is_absolute() and ".." not in subdir.parts,
            "INVALID_RECIPE", "workdir may not escape pinned checkout")
    command = ["git", "-C", str(repo_root), "cat-file", "-t", commit]
    p = subprocess.run(command, capture_output=True, text=True, timeout=15, check=False)
    require(p.returncode == 0 and p.stdout.strip() == "commit", "SOURCE_NOT_FOUND", "source_commit is not a known Git commit")
    dest = job_dir / "source"
    require(not dest.exists(), "SOURCE_STATE_UNCERTAIN", "source already materialized; inspect unknown job")
    p = subprocess.run(["git", "-C", str(repo_root), "worktree", "add", "--detach", str(dest), commit],
                       capture_output=True, text=True, timeout=60, check=False)
    require(p.returncode == 0, "SOURCE_MATERIALIZE_FAILED", p.stderr[:400])
    cwd = (dest / subdir).resolve(strict=True)
    require(cwd.is_dir() and cwd.is_relative_to(dest.resolve()), "INVALID_RECIPE", "invalid source workdir")
    return str(cwd)
