import os
import tempfile

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
