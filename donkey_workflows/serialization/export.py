import ast
import inspect
import sys
from importlib.metadata import PackageNotFoundError, packages_distributions
from importlib.metadata import version as pkg_version
from typing import Any

from pydantic import BaseModel, Field


def _used_names(source: str) -> set[str]:
    """Names referenced anywhere in a source snippet (no scope resolution -- best effort)."""
    return {node.id for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Name)}


def extract_dependencies(obj: Any) -> list[str]:
    """
    Literal top-level import statements from `obj`'s defining module that
    `obj`'s own source actually references.

    These are replayed verbatim (via exec) when reconstructing exported code
    in an environment where the original module isn't importable, so aliases
    like `import numpy as np` or `from pydantic import Field as F` just work --
    we rerun the same statement instead of trying to reverse-engineer it.
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
            bound_names = {(alias.asname or alias.name).split(".")[0] for alias in node.names}
            if bound_names & needed:
                dependencies.append(ast.unparse(node))

    return dependencies


class DependencyPackageExport(BaseModel):
    name: str
    version: str


def resolve_dependency_packages(dependencies: list[str]) -> list[DependencyPackageExport]:
    """
    Best-effort installed version, at export time, for each external (pip)
    package referenced in `dependencies`.

    Stdlib and internal/local modules are skipped -- they have no meaningful
    package version. Purely informational: useful for diagnosing drift
    between the export-time and load-time environments; never enforced.
    """
    packages: list[DependencyPackageExport] = []
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
                DependencyPackageExport(name=module_name, version=pkg_version(dist_names[0]))
            )
            seen.add(module_name)
        except PackageNotFoundError:
            continue

    return packages


class DependenciesExport(BaseModel):
    imports: list[str] = Field(default_factory=list)
    packages: list[DependencyPackageExport] = Field(default_factory=list)


class EventFieldExport(BaseModel):
    type: str
    required: bool


class EventExport(BaseModel):
    code: str | None = None
    fields: dict[str, EventFieldExport] = Field(default_factory=dict)


class StepExport(BaseModel):
    name: str
    triggers: list[str]
    produces: list[str]
    is_join: bool
    timeout: float | None = None
    max_retries: int = 0
    retry_delay: float = 1.0
    code: str | None = None


class WorkflowExport(BaseModel):
    version: str = "1.0.0"
    kind: str = "Workflow"
    name: str
    module: str | None = None
    description: str = ""
    state_type: str
    state_code: str | None = None
    code: str | None = None
    steps: list[StepExport] = Field(default_factory=list)
    events: dict[str, EventExport] = Field(default_factory=dict)
    dependencies: DependenciesExport = Field(default_factory=DependenciesExport)
