import os
import tempfile
from datetime import datetime
from typing import Optional

import pytest
from pydantic import BaseModel

from donkey_workflows import (
    Context,
    Event,
    StartEvent,
    StopEvent,
    Workflow,
    load_from_json,
    step,
)
from donkey_workflows.exceptions import WorkflowValidationError


class OrderEvent(Event):
    order_id: str
    amount: float


class ConfirmEvent(Event):
    confirmed: bool


class OrderState(BaseModel):
    total: float = 0.0


class OrderWorkflow(Workflow):
    """Processes an order through validation and confirmation."""

    @step(when=StartEvent)
    async def validate(self, ctx: Context[OrderState], ev: StartEvent) -> OrderEvent:
        return OrderEvent(order_id="123", amount=99.9)

    @step(when=OrderEvent)
    async def confirm(self, ctx: Context[OrderState], ev: OrderEvent) -> ConfirmEvent:
        return ConfirmEvent(confirmed=True)

    @step(when=ConfirmEvent)
    async def finish(self, ctx: Context[OrderState], ev: ConfirmEvent) -> StopEvent:
        return StopEvent(result="done")


def test_load_from_dict_primary_path():
    """Primary path: module is importable, returns the original class directly."""
    data = OrderWorkflow.export()
    cls = load_from_json(data)

    assert cls is OrderWorkflow


def test_load_from_file_primary_path():
    """Load from a JSON file using the primary module import path."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = f.name

    try:
        OrderWorkflow.export(path=tmp_path)
        cls = load_from_json(tmp_path)
        assert cls is OrderWorkflow
    finally:
        os.unlink(tmp_path)


def test_load_fallback_exec_returns_workflow_subclass():
    """Fallback path: reconstructs from source when module is unavailable."""
    data = OrderWorkflow.export()
    data["module"] = "non.existent.module"

    cls = load_from_json(data)

    assert cls is not OrderWorkflow
    assert issubclass(cls, Workflow)
    assert cls.__name__ == "OrderWorkflow"


@pytest.mark.asyncio
async def test_load_fallback_exec_is_runnable():
    """Workflow reconstructed via exec executes and returns the correct result."""
    data = OrderWorkflow.export()
    data["module"] = "non.existent.module"

    cls = load_from_json(data)
    result = await cls().run()

    assert result.result == "done"


def test_load_raises_when_no_code_and_no_module():
    """Raises WorkflowValidationError when module is missing and code is absent."""
    data = OrderWorkflow.export()
    data["module"] = "non.existent.module"
    data["code"] = None

    with pytest.raises(WorkflowValidationError, match="Cannot load workflow"):
        load_from_json(data)


class TimedEvent(Event):
    # Requires `Optional`/`datetime` to be re-imported for exec reconstruction to work.
    note: Optional[str] = None
    seen_at: datetime = datetime(2024, 1, 1)


class TimedWorkflow(Workflow):
    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> TimedEvent:
        return TimedEvent(note="hi")

    @step(when=TimedEvent)
    async def finish(self, ctx: Context, ev: TimedEvent) -> StopEvent:
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_load_fallback_resolves_installed_external_imports():
    """Fallback exec re-imports external names (Optional, datetime) that are installed."""
    data = TimedWorkflow.export()
    data["module"] = "non.existent.module"

    cls = load_from_json(data)
    result = await cls().run()

    assert result.result == "done"


def test_load_raises_clear_error_for_missing_dependency():
    """A dependency captured at export time but absent at load time fails clearly, not with NameError."""
    data = OrderWorkflow.export()
    data["module"] = "non.existent.module"
    data["dependencies"] = {
        "imports": ["import totally_not_installed_lib_xyz as tnil"],
        "packages": [],
    }

    with pytest.raises(WorkflowValidationError, match="missing dependencies"):
        load_from_json(data)


class ZBaseEvent(Event):
    value: int = 0


class ABaseEvent(ZBaseEvent):
    label: str = ""


class InheritanceWorkflow(Workflow):
    """ZBaseEvent is never a direct trigger/produces, only an ancestor of ABaseEvent."""

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> ABaseEvent:
        return ABaseEvent(value=1, label="ok")

    @step(when=ABaseEvent)
    async def finish(self, ctx: Context, ev: ABaseEvent) -> StopEvent:
        return StopEvent(result=ev.value)


def test_export_includes_ancestor_only_event():
    """ZBaseEvent must appear in the manifest even though it is never a direct trigger/produces."""
    data = InheritanceWorkflow.export()
    assert "ZBaseEvent" in data["events"], (
        "ZBaseEvent is an ancestor of ABaseEvent but was not captured in the manifest"
    )


@pytest.mark.asyncio
async def test_load_fallback_reconstructs_inherited_events():
    """Fallback exec must reconstruct ZBaseEvent before ABaseEvent regardless of manifest order."""
    data = InheritanceWorkflow.export()
    data["module"] = "non.existent.module"

    cls = load_from_json(data)
    result = await cls().run()

    assert result.result == 1
