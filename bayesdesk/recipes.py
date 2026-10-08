"""Trusted executable recipes. Job inputs are typed parameters, not shell scripts."""
from __future__ import annotations
from pathlib import Path
import re
from .errors import ContractError, require
from .contracts import exact_keys


def render_recipe(recipe: dict, inputs: dict) -> tuple[list[str], str]:
    exact_keys(recipe, {"argv", "cwd", "parameters"})
    require(isinstance(recipe["argv"], list) and bool(recipe["argv"]) and
            all(isinstance(s, str) and s for s in recipe["argv"]),
            "INVALID_RECIPE", "recipe argv must be a nonempty list of strings")
    cwd = Path(recipe["cwd"]).resolve(strict=True)
    require(cwd.is_dir(), "INVALID_RECIPE", "recipe cwd must be a directory")
    require(isinstance(recipe["parameters"], dict) and isinstance(inputs, dict), "INVALID_INPUT", "parameters/inputs must be objects")
    require(set(inputs) == set(recipe["parameters"]), "INVALID_INPUT",
            "inputs must exactly match recipe parameter names")
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
