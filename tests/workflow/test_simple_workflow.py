import pytest

from donkey_workflows import Context, Event, StartEvent, StopEvent, Workflow, step


class MyEvent(Event):
    message: str


class SimpleWorkflow(Workflow):
    """
    - Workflow without state management.
    - Simple event chaining: StartEvent → MyEvent → StopEvent.
    - Basic decorator usage with @step(when=...).
    - Returning events from step methods.
    """

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> MyEvent:
        input_msg = ev.get("input_msg", "")
        return MyEvent(message=f"Processed: {input_msg}")

    @step(when=MyEvent)
    async def process(self, ctx: Context, ev: MyEvent) -> StopEvent:
        return StopEvent(result=ev.message)


@pytest.mark.asyncio
async def test_simple_workflow():
    """Test simple workflow execution with basic event chaining."""
    workflow = SimpleWorkflow()
    workflow_result = await workflow.run(input_msg="Hello, World!")

    assert workflow_result.result == "Processed: Hello, World!"
