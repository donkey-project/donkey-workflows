import ast
import hashlib
import inspect
import json
import sys
from importlib.metadata import PackageNotFoundError, packages_distributions
from importlib.metadata import version as pkg_version
from typing import Any, Type

from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.serialization.schemas import (
    DependencyPackageSpec,
    EventFieldSpec,
    EventSpec,
    StepSpec,
)
from donkey_workflows.step_metadata import (
    get_step_description,
    get_step_event_types,
    get_step_max_retries,
    get_step_name,
    get_step_produced_events,
    get_step_retry_delay,
    get_step_timeout,
    is_join_step,
    is_step_method,
)


def _used_names(source: str) -> set[str]:
    """Names referenced anywhere in a source snippet (best-effort, no scope resolution)."""
    return {
        node.id for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Name)
    }


def extract_imports(obj: Any) -> list[str]:
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


def resolve_packages(imports: list[str]) -> list[DependencyPackageSpec]:
    """
    Returns the installed version of each external (pip) package referenced in ``imports``.
    Stdlib and local modules are skipped.
    """
    packages: list[DependencyPackageSpec] = []
    seen: set[str] = set()
    dist_map = packages_distributions()

    for stmt in imports:
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


def collect_steps(cls: Any) -> tuple[list[StepSpec], set[Type[Event]]]:
    """
    Collects all @step methods from a workflow class and builds StepSpec objects.

    Returns the list of StepSpec and the set of all event types referenced
    (triggers + produces), which is needed for event schema collection.
    """
    step_methods = [
        (name, method)
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction)
        if is_step_method(method)
    ]

    steps: list[StepSpec] = []
    event_types: set[Type[Event]] = set()

    for name, method in step_methods:
        triggers = get_step_event_types(method) or []
        produces = get_step_produced_events(method)

        event_types.update(triggers)
        event_types.update(produces)

        try:
            code = inspect.getsource(method)
        except OSError:
            code = None

        steps.append(
            StepSpec(
                name=get_step_name(method) or name,
                description=get_step_description(method),
                inputs=[e.__name__ for e in triggers],
                outputs=sorted(e.__name__ for e in produces),
                is_join_step=is_join_step(method),
                timeout=get_step_timeout(method),
                max_retries=get_step_max_retries(method),
                retry_delay=get_step_retry_delay(method),
                code=code,
            )
        )

    return steps, event_types


def collect_events(
    event_types: set[Type[Event]],
) -> tuple[list[EventSpec], list[str]]:
    """
    Builds EventSpec objects for all event types, including ancestor classes
    needed to reconstruct inheritance chains during load.

    Returns the list of EventSpec and the collected import dependencies.
    """
    _builtin_events: frozenset[type] = frozenset({Event, StartEvent, StopEvent})

    # Expand to include ancestor event classes from the MRO that aren't direct
    # triggers/outputs but are needed by load_from_json to reconstruct subclasses.
    ancestors: set[Type[Event]] = set()
    for evt_cls in event_types:
        for base in evt_cls.__mro__:
            if (
                base not in _builtin_events
                and base is not object
                and isinstance(base, type)
                and issubclass(base, Event)
                and base not in event_types
            ):
                ancestors.add(base)
    _event_types = event_types | ancestors

    dependencies: list[str] = []
    events: list[EventSpec] = []

    for evt_cls in sorted(_event_types, key=lambda e: e.__name__):
        try:
            evt_code = inspect.getsource(evt_cls)
        except OSError:
            evt_code = None

        dependencies.extend(extract_imports(evt_cls))

        fields: dict[str, EventFieldSpec] = {}
        for field_name, field_info in evt_cls.model_fields.items():
            annotation = field_info.annotation
            type_str = (
                annotation.__name__
                if hasattr(annotation, "__name__")
                else str(annotation)
            )
            fields[field_name] = EventFieldSpec(
                type=type_str,
                required=field_info.is_required(),
            )

        events.append(
            EventSpec(
                name=evt_cls.__name__,
                code=evt_code,
                fields=fields,
            )
        )

    return events, dependencies


def compute_checksum(raw: dict) -> str:
    """
    Computes a SHA-256 checksum over the manifest dict (checksum field excluded).

    Uses sort_keys for deterministic serialization regardless of dict insertion order.
    Returns a string in the format ``sha256:<hex>``, following the OCI/Docker convention.
    """
    digest = hashlib.sha256(
        json.dumps(raw, sort_keys=True, default=str).encode()
    ).hexdigest()
    return f"sha256:{digest}"
