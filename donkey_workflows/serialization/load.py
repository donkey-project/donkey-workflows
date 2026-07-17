import importlib
import json
from typing import Any, Type

from pydantic import BaseModel

from donkey_workflows.context import Context
from donkey_workflows.context.state_store import DictState
from donkey_workflows.decorators import step
from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.exceptions import WorkflowValidationError
from donkey_workflows.serialization.schemas import WorkflowSpec
from donkey_workflows.workflow import Workflow


def _resolve_dependencies(manifest: WorkflowSpec, namespace: dict[str, Any]) -> None:
    """
    Executes each import statement from ``dependencies`` into ``namespace``.
    Missing packages raise immediately with a clear error instead of a later ``NameError``.
    """
    missing: list[str] = []
    for stmt in manifest.dependencies.imports:
        try:
            exec(stmt, namespace)  # noqa: S102 — trusted export, module unavailable
        except ImportError:
            missing.append(stmt)

    if missing:
        raise WorkflowValidationError(
            f"Cannot load workflow '{manifest.name}': missing dependencies not installed "
            f"in this environment:\n  " + "\n  ".join(missing)
        )


def _build_exec_namespace(manifest: WorkflowSpec) -> dict[str, Any]:
    """Builds the exec namespace used to reconstruct a workflow from exported source."""
    namespace: dict[str, Any] = {
        "Workflow": Workflow,
        "step": step,
        "Context": Context,
        "Event": Event,
        "StartEvent": StartEvent,
        "StopEvent": StopEvent,
        "DictState": DictState,
        "BaseModel": BaseModel,
    }

    _resolve_dependencies(manifest, namespace)

    # Reconstruct event classes. Try original module first, then exec source.
    # Export order isn't guaranteed to respect dependencies between custom events
    # (inheritance, or one event referencing another as a field type), so retry
    # in passes: each pass execs whatever now succeeds, until nothing is left or
    # a full pass makes no progress (a real unresolved dependency).
    pending = {
        evt_name: evt_info.code
        for evt_name, evt_info in manifest.events.items()
        if evt_name not in namespace and evt_info.code
    }

    while pending:
        progressed = False
        for evt_name, code in list(pending.items()):
            try:
                exec(code, namespace)  # noqa: S102 — trusted export, module unavailable
            except NameError:
                continue
            del pending[evt_name]
            progressed = True

        if not progressed:
            raise WorkflowValidationError(
                f"Cannot load workflow '{manifest.name}': unresolved dependency between "
                f"exported event classes: {', '.join(sorted(pending))}"
            )

    # Exec the exported state class source if it's not already in the namespace
    if manifest.state_type not in namespace and manifest.state_code:
        exec(manifest.state_code, namespace)  # noqa: S102 — trusted export, module unavailable

    return namespace


def load_from_json(workflow: str | dict) -> Type[Workflow]:
    """
    Loads a Workflow from a JSON export (file path or dict).

    Tries to import from the original module first; Fallback to reconstructing
    via ``exec`` when the module is unavailable.

    Args:
        workflow: File path to a JSON export or a dict from ``Workflow.export()``.

    Returns:
        The Workflow subclass, ready to instantiate and run.

    Raises:
        WorkflowValidationError: If the workflow cannot be loaded.
    """
    if isinstance(workflow, str):
        with open(workflow, encoding="utf-8") as f:
            raw = json.load(f)
    else:
        raw = dict(workflow)

    manifest = WorkflowSpec.model_validate(raw)

    # Try importing from the original module first (no exec needed)
    if manifest.module:
        try:
            mod = importlib.import_module(manifest.module)
            workflow_cls = getattr(mod, manifest.name, None)
            if isinstance(workflow_cls, type) and issubclass(workflow_cls, Workflow):
                return workflow_cls
        except ImportError:
            pass

    # Fallback: reconstruct from exported code
    if not manifest.code:
        raise WorkflowValidationError(
            f"Cannot load workflow '{manifest.name}': module '{manifest.module}' is not available "
            "and no code was found in the export."
        )

    namespace = _build_exec_namespace(manifest)
    exec(manifest.code, namespace)  # noqa: S102 — trusted export, module unavailable

    workflow_cls = namespace.get(manifest.name)
    if not (isinstance(workflow_cls, type) and issubclass(workflow_cls, Workflow)):
        raise WorkflowValidationError(
            f"Failed to reconstruct workflow '{manifest.name}' from exported source."
        )

    return workflow_cls
