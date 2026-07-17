import pytest

from donkey_workflows import Workflow
from donkey_workflows.context import Context
from donkey_workflows.decorators import step
from donkey_workflows.events import Event, StartEvent, StopEvent


class EventA(Event):
    data_a: str


class EventB(Event):
    data_b: str


class EventC(Event):
    data_c: str


class JoinedEvent(Event):
    combined: str


class SimpleFanInWorkflow(Workflow):
    """Simple workflow with fan-in pattern: two events join into one step."""

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> EventA:
        """Emit first event."""
        return EventA(data_a="A")

    @step(when=EventA)
    async def process_a(self, ctx: Context, ev: EventA) -> EventB:
        """Process EventA and emit EventB."""
        return EventB(data_b="B")

    @step(when=[EventA, EventB])
    async def join_step(self, ctx: Context, events: dict[type, Event]) -> JoinedEvent:
        """Join EventA and EventB."""
        event_a = events[EventA]
        event_b = events[EventB]
        combined = f"{event_a.data_a}+{event_b.data_b}"
        return JoinedEvent(combined=combined)

    @step(when=JoinedEvent)
    async def finish(self, ctx: Context, ev: JoinedEvent) -> StopEvent:
        """Complete workflow with joined result."""
        return StopEvent(result=ev.combined)


class ThreeWayFanInWorkflow(Workflow):
    """Workflow with three-way fan-in pattern."""

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> EventA:
        """Emit first event."""
        return EventA(data_a="A")

    @step(when=EventA)
    async def branch_b(self, ctx: Context, ev: EventA) -> EventB:
        """Branch to EventB."""
        return EventB(data_b="B")

    @step(when=EventA)
    async def branch_c(self, ctx: Context, ev: EventA) -> EventC:
        """Branch to EventC."""
        return EventC(data_c="C")

    @step(when=[EventA, EventB, EventC])
    async def join_all(self, ctx: Context, events: dict[type, Event]) -> StopEvent:
        """Join all three events."""
        event_a = events[EventA]
        event_b = events[EventB]
        event_c = events[EventC]
        combined = f"{event_a.data_a}+{event_b.data_b}+{event_c.data_c}"
        return StopEvent(result=combined)


class MixedPatternWorkflow(Workflow):
    """Workflow mixing single-event and join steps."""

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> EventA:
        """Emit first event."""
        return EventA(data_a="start")

    @step(when=EventA)
    async def single_step(self, ctx: Context, ev: EventA) -> EventB:
        """Single-event step."""
        return EventB(data_b=f"{ev.data_a}_single")

    @step(when=[EventA, EventB])
    async def join_step(self, ctx: Context, events: dict[type, Event]) -> EventC:
        """Join step."""
        event_a = events[EventA]
        event_b = events[EventB]
        return EventC(data_c=f"{event_a.data_a}+{event_b.data_b}")

    @step(when=EventC)
    async def finish(self, ctx: Context, ev: EventC) -> StopEvent:
        """Complete workflow."""
        return StopEvent(result=ev.data_c)


@pytest.mark.asyncio
async def test_simple_fan_in():
    """Test basic fan-in pattern with two events."""
    workflow = SimpleFanInWorkflow()
    workflow_result = await workflow.run()

    assert workflow_result.result == "A+B"


@pytest.mark.asyncio
async def test_three_way_fan_in():
    """Test fan-in pattern with three events."""
    workflow = ThreeWayFanInWorkflow()
    workflow_result = await workflow.run()

    assert workflow_result.result == "A+B+C"


@pytest.mark.asyncio
async def test_mixed_pattern():
    """Test workflow mixing single-event and join steps."""
    workflow = MixedPatternWorkflow()
    workflow_result = await workflow.run()

    assert workflow_result.result == "start+start_single"


@pytest.mark.asyncio
async def test_fan_in_with_context():
    """Test fan-in pattern with context state."""

    class StateWorkflow(Workflow):
        @step(when=StartEvent)
        async def start(self, ctx: Context, ev: StartEvent) -> EventA:
            ctx.state["counter"] = 0
            return EventA(data_a="A")

        @step(when=EventA)
        async def increment_a(self, ctx: Context, ev: EventA) -> EventB:
            ctx.state["counter"] += 1
            return EventB(data_b="B")

        @step(when=[EventA, EventB])
        async def join_and_check(
            self, ctx: Context, events: dict[type, Event]
        ) -> StopEvent:
            ctx.state["counter"] += 10
            return StopEvent(result=ctx.state["counter"])

    workflow = StateWorkflow()
    workflow_result = await workflow.run()

    # Counter should be: 0 + 1 (from increment_a) + 10 (from join_and_check) = 11
    assert workflow_result.result == 11


@pytest.mark.asyncio
async def test_fan_in_event_order_independence():
    """Test that fan-in works regardless of event arrival order."""

    class OrderTestWorkflow(Workflow):
        @step(when=StartEvent)
        async def start(self, ctx: Context, ev: StartEvent) -> EventA:
            return EventA(data_a="first")

        @step(when=EventA)
        async def emit_b(self, ctx: Context, ev: EventA) -> EventB:
            # EventB arrives after EventA
            return EventB(data_b="second")

        @step(when=[EventB, EventA])
        async def join_reverse_order(
            self, ctx: Context, events: dict[type, Event]
        ) -> StopEvent:
            # Note: when=[EventB, EventA] - different order than emission
            event_a = events[EventA]
            event_b = events[EventB]
            return StopEvent(result=f"{event_a.data_a}-{event_b.data_b}")

    workflow = OrderTestWorkflow()
    workflow_result = await workflow.run()

    assert workflow_result.result == "first-second"


@pytest.mark.asyncio
async def test_multiple_parallel_join_steps():
    """Test workflow with multiple independent parallel join steps."""

    class MultiJoinWorkflow(Workflow):
        @step(when=StartEvent)
        async def start(self, ctx: Context, ev: StartEvent) -> EventA:
            return EventA(data_a="A")

        @step(when=EventA)
        async def branch_b(self, ctx: Context, ev: EventA) -> EventB:
            return EventB(data_b="B")

        @step(when=EventA)
        async def branch_c(self, ctx: Context, ev: EventA) -> EventC:
            return EventC(data_c="C")

        @step(when=[EventA, EventB])
        async def join_ab(self, ctx: Context, events: dict[type, Event]) -> JoinedEvent:
            event_a = events[EventA]
            event_b = events[EventB]
            return JoinedEvent(combined=f"AB:{event_a.data_a}{event_b.data_b}")

        @step(when=[EventA, EventC])
        async def join_ac(self, ctx: Context, events: dict[type, Event]) -> JoinedEvent:
            event_a = events[EventA]
            event_c = events[EventC]
            return JoinedEvent(combined=f"AC:{event_a.data_a}{event_c.data_c}")

        @step(when=JoinedEvent)
        async def collect(self, ctx: Context, ev: JoinedEvent) -> StopEvent:
            # Store results in context
            if "results" not in ctx.state:
                ctx.state["results"] = []
            ctx.state["results"].append(ev.combined)

            # Stop when we have both results
            if len(ctx.state["results"]) == 2:
                return StopEvent(result=sorted(ctx.state["results"]))

            return None  # Continue processing

    workflow = MultiJoinWorkflow()
    workflow_result = await workflow.run()

    # Both fan-in steps should execute
    assert workflow_result.result == ["AB:AB", "AC:AC"]
