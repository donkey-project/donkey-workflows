import importlib
import json
from typing import Any, Type

from pydantic import BaseModel

from donkey_workflows.context import Context
from donkey_workflows.context.state_store import DictState
from donkey_workflows.decorators import step
from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.exceptions import WorkflowValidationError
from donkey_workflows.serialization.export import WorkflowExport
from donkey_workflows.workflow import Workflow


def _resolve_dependencies(manifest: WorkflowExport, namespace: dict[str, Any]) -> None:
    """
    Exec the dependency import statements captured at export time into `namespace`.

    Each statement is the literal `import ...` / `from ... import ...` line
    from the original module, so aliases (`import numpy as np`) resolve
    exactly as they did originally. If a dependency isn't installed in this
    environment, that's a real, expected failure -- surfaced as a single
    clear error instead of a confusing NameError once exec of the class
    itself gets underway.
    """
    missing: list[str] = []
    for stmt in manifest.dependencies.imports:
        try:
            exec(stmt, namespace)  # noqa: S102 — trusted export, replaying its own imports
        except ImportError:
            missing.append(stmt)

    if missing:
        raise WorkflowValidationError(
            f"Cannot load workflow '{manifest.name}': missing dependencies not installed "
            f"in this environment:\n  " + "\n  ".join(missing)
        )


def _build_exec_namespace(manifest: WorkflowExport) -> dict[str, Any]:
    """Build the exec namespace for reconstructing a workflow from exported source."""
    namespace: dict[str, Any] = {
        "Workflow": Workflow,
        "step": step,
        "Context": Context,
        "Event": Event,
        "StartEvent": StartEvent,
        "StopEvent": StopEvent,
        "DictState": DictState,
        "BaseModel": BaseModel,
        "Any": Any,
    }

    _resolve_dependencies(manifest, namespace)

    # Reconstruct event classes — try original module first, then exec source
    for evt_name, evt_info in manifest.events.items():
        if evt_name in namespace:
            continue

        if evt_info.code:
            exec(evt_info.code, namespace)  # noqa: S102 — trusted export, module unavailable

    # Reconstruct state class — exec state code if not in namespace
    if manifest.state_type not in namespace and manifest.state_code:
        exec(manifest.state_code, namespace)  # noqa: S102 — trusted export, module unavailable

    return namespace


def load_from_json(source: str | dict) -> Type[Workflow]:
    """
    Load a Workflow subclass from a previously exported JSON manifest.

    Tries to import from the original module first (safe, no exec). Falls back
    to reconstructing the workflow by executing the exported source code when
    the original module is unavailable.

    Args:
        source: File path to a JSON export or a dict from Workflow.export().

    Returns:
        The Workflow subclass, ready to instantiate and run.

    Raises:
        WorkflowValidationError: If the workflow cannot be loaded.
    """
    if isinstance(source, str):
        with open(source, encoding="utf-8") as f:
            raw = json.load(f)
    else:
        raw = dict(source)

    manifest = WorkflowExport.model_validate(raw)

    # Primary path: import from original module — no exec required
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
