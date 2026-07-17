import pytest

from donkey_workflows import Context, StartEvent, StopEvent, Workflow, step


@pytest.fixture
def simple_workflow():
    """
    Minimal one-step workflow: StartEvent → StopEvent.
    Use as a lightweight base for runtime and context tests.
    """

    class _SimpleWorkflow(Workflow):
        @step(when=StartEvent)
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
            return StopEvent(result=ev.get("result", "ok"))

    return _SimpleWorkflow()


@pytest.fixture
def fresh_context(simple_workflow):
    """Isolated Context instance, recreated for each test."""
    return Context(simple_workflow)
