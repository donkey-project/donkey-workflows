import json
import os
import tempfile

import pytest
from pydantic import BaseModel

from donkey_workflows import Context, Event, StartEvent, StopEvent, Workflow, step


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

    @step(when=OrderEvent, timeout=5.0, max_retries=2, retry_delay=0.5)
    async def confirm(self, ctx: Context[OrderState], ev: OrderEvent) -> ConfirmEvent:
        return ConfirmEvent(confirmed=True)

    @step(when=ConfirmEvent)
    async def finish(self, ctx: Context[OrderState], ev: ConfirmEvent) -> StopEvent:
        return StopEvent(result="done")


class JoinEvent(Event):
    value: int


class BranchAEvent(Event):
    pass


class BranchBEvent(Event):
    pass


class FanInWorkflow(Workflow):
    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> None:
        await ctx.send_event(BranchAEvent())
        await ctx.send_event(BranchBEvent())

    @step(when=[BranchAEvent, BranchBEvent])
    async def join(self, ctx: Context, events: dict) -> StopEvent:
        return StopEvent(result="joined")


def test_export_returns_dict():
    data = OrderWorkflow.export()

    assert isinstance(data, dict)
    for key in ("id_", "api_version", "kind", "name", "module", "description", "data"):
        assert key in data, f"Missing key: {key}"

    for key in ("state_type", "state_code", "code", "steps", "events", "dependencies"):
        assert key in data["data"], f"Missing key in data: {key}"

    assert data["api_version"] == "v1.0"
    assert data["kind"] == "Workflow"
    assert data["name"] == "OrderWorkflow"
    assert (
        data["description"] == "Processes an order through validation and confirmation."
    )


def test_export_steps_metadata():
    data = OrderWorkflow.export()
    steps = {s["name"]: s for s in data["data"]["steps"]}

    assert set(steps) == {"validate", "confirm", "finish"}

    validate = steps["validate"]
    assert validate["inputs"] == ["StartEvent"]
    assert validate["outputs"] == ["OrderEvent"]
    assert validate["is_join_step"] is False
    assert validate["timeout"] is None
    assert validate["max_retries"] == 0
    assert validate["retry_delay"] == 1.0

    confirm = steps["confirm"]
    assert confirm["inputs"] == ["OrderEvent"]
    assert confirm["outputs"] == ["ConfirmEvent"]
    assert confirm["timeout"] == 5.0
    assert confirm["max_retries"] == 2
    assert confirm["retry_delay"] == 0.5


def test_export_includes_code():
    data = OrderWorkflow.export()

    assert data["data"]["code"] is not None
    assert "class OrderWorkflow" in data["data"]["code"]

    steps = {s["name"]: s for s in data["data"]["steps"]}
    for step_data in steps.values():
        assert step_data["code"] is not None
        assert f"async def {step_data['name']}" in step_data["code"]


def test_export_events_schema():
    data = OrderWorkflow.export()
    events = {e["name"]: e for e in data["data"]["events"]}

    assert "OrderEvent" in events
    assert "ConfirmEvent" in events
    assert "StartEvent" in events
    assert "StopEvent" in events

    order = events["OrderEvent"]
    assert order["code"] is not None
    assert "class OrderEvent" in order["code"]
    assert "order_id" in order["fields"]
    assert "amount" in order["fields"]
    assert order["fields"]["order_id"]["type"] == "str"
    assert order["fields"]["order_id"]["required"] is True
    assert order["fields"]["amount"]["type"] == "float"

    confirm = events["ConfirmEvent"]
    assert confirm["fields"]["confirmed"]["type"] == "bool"
    assert confirm["fields"]["confirmed"]["required"] is True


def test_export_state():
    data = OrderWorkflow.export()

    assert data["data"]["state_type"] == "OrderState"
    assert data["data"]["state_code"] is not None
    assert "class OrderState" in data["data"]["state_code"]


def test_export_to_file():
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = f.name

    try:
        result = OrderWorkflow.export(path=tmp_path)

        assert os.path.exists(tmp_path)
        with open(tmp_path, encoding="utf-8") as f:
            loaded = json.load(f)

        assert loaded["name"] == result["name"]
        assert loaded["api_version"] == "v1.0"
        assert len(loaded["data"]["steps"]) == len(result["data"]["steps"])
    finally:
        os.unlink(tmp_path)


def test_export_join_step():
    data = FanInWorkflow.export()
    steps = {s["name"]: s for s in data["data"]["steps"]}

    join = steps["join"]
    assert join["is_join_step"] is True
    assert set(join["inputs"]) == {"BranchAEvent", "BranchBEvent"}
