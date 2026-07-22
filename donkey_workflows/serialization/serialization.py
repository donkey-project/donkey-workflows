import ast
import inspect
import sys
from importlib.metadata import PackageNotFoundError, packages_distributions
from importlib.metadata import version as pkg_version
from typing import Any

from donkey_workflows.serialization.schemas import DependencyPackageSpec


def _used_names(source: str) -> set[str]:
    """Names referenced anywhere in a source snippet (best-effort, no scope resolution)."""
    return {
        node.id for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Name)
    }


def extract_dependencies(obj: Any) -> list[str]:
    """
    Extracts import statements from the object's source. Statements are stored verbatim
    so they can be replayed as-is (e.g. ``import numpy as np``) during export reconstruction.
    """
    try:
        obj_source = inspect.getsource(obj)
        module = inspect.getmodule(obj)
        module_source = inspect.getsource(module) if module else None
    except (OSError, TypeError):
        return []

    if not module_source:
        return []

    needed = _used_names(obj_source)
    dependencies: list[str] = []
    for node in ast.parse(module_source).body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            bound_names = {
                (alias.asname or alias.name).split(".")[0] for alias in node.names
            }
            if bound_names & needed:
                dependencies.append(ast.unparse(node))

    return dependencies


def resolve_dependency_packages(dependencies: list[str]) -> list[DependencyPackageSpec]:
    """
    Returns the installed version of each external (pip) package in ``dependencies``.
    Stdlib and local modules are skipped.
    """
    packages: list[DependencyPackageSpec] = []
    seen: set[str] = set()
    dist_map = packages_distributions()

    for stmt in dependencies:
        try:
            node = ast.parse(stmt).body[0]
        except (SyntaxError, IndexError):
            continue

        if isinstance(node, ast.ImportFrom):
            if node.level > 0 or node.module is None:
                continue  # relative import, no resolvable top-level module
            module_name = node.module.split(".")[0]
        elif isinstance(node, ast.Import):
            module_name = node.names[0].name.split(".")[0]
        else:
            continue

        if module_name in sys.stdlib_module_names or module_name in seen:
            continue

        dist_names = dist_map.get(module_name)
        if not dist_names:
            continue  # not a pip-installed package (internal/local module)

        try:
            packages.append(
                DependencyPackageSpec(
                    name=module_name, version=pkg_version(dist_names[0])
                )
            )
            seen.add(module_name)
        except PackageNotFoundError:
            continue

    return packages
