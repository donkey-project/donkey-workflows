import pytest
from pydantic import BaseModel, Field

from donkey_workflows import Context, Event, StartEvent, StopEvent, Workflow, step
from donkey_workflows.exceptions import WorkflowRuntimeError


class DataCleanEvent(Event):
    raw_data: str


class DataValidateEvent(Event):
    cleaned_data: str


class DataTransformEvent(Event):
    validated_data: str


class PipelineState(BaseModel):
    cleaning_done: bool = Field(default=False)
    validation_done: bool = Field(default=False)


class DataCleaningWorkflow(Workflow):
    """
    - Reusable workflow component.
    - Isolated state (independent from parent).
    """

    @step(when=StartEvent)
    async def clean(self, ctx: Context, ev: StartEvent) -> DataCleanEvent:
        raw = ev.get("input_msg", "")
        cleaned = raw.strip().lower()
        return DataCleanEvent(raw_data=cleaned)

    @step(when=DataCleanEvent)
    async def finish_cleaning(self, ctx: Context, ev: DataCleanEvent) -> StopEvent:
        return StopEvent(result=ev.raw_data)


class DataValidationWorkflow(Workflow):
    """
    - Reusable validation logic.
    - Error handling in sub-workflows.
    - Isolated state (independent from parent).
    """

    @step(when=StartEvent)
    async def validate(self, ctx: Context, ev: StartEvent) -> DataValidateEvent:
        data = ev.get("input_msg", "")
        # Simple validation: check if not empty
        if not data:
            raise ValueError("Data cannot be empty")
        return DataValidateEvent(cleaned_data=data)

    @step(when=DataValidateEvent)
    async def finish_validation(self, ctx: Context, ev: DataValidateEvent) -> StopEvent:
        return StopEvent(result=ev.cleaned_data)


class DataPipelineWorkflow(Workflow):
    """
    - Simple sub-workflow execution via direct run() calls.
    - Complete context isolation (each sub-workflow is independent).
    - No automatic state sharing between workflows.
    - Explicit communication via parameters and return values.
    - Modular and reusable workflow design.
    """

    @step(when=StartEvent)
    async def start_pipeline(self, ctx: Context, ev: StartEvent) -> DataTransformEvent:
        if ctx.state is None:
            ctx._store._state = PipelineState()

        # Step 1: Clean data using sub-workflow (isolated context)
        cleaning_workflow = DataCleaningWorkflow()
        clean_workflow_result = await cleaning_workflow.run(
            input_msg=ev.get("input_msg", "")
        )
        clean_result = clean_workflow_result.result

        # Update parent state after sub-workflow completes
        async with ctx.store.edit_state() as state:
            state.cleaning_done = True

        # Step 2: Validate cleaned data using sub-workflow (isolated context)
        validation_workflow = DataValidationWorkflow()
        validate_workflow_result = await validation_workflow.run(input_msg=clean_result)
        validate_result = validate_workflow_result.result

        # Update parent state after sub-workflow completes
        async with ctx.store.edit_state() as state:
            state.validation_done = True

        # Step 3: Transform in main workflow
        transformed = validate_result.upper()

        return DataTransformEvent(validated_data=transformed)

    @step(when=DataTransformEvent)
    async def finish_pipeline(self, ctx: Context, ev: DataTransformEvent) -> StopEvent:
        assert ctx.state.cleaning_done is True
        assert ctx.state.validation_done is True

        return StopEvent(result=ev.validated_data)


@pytest.mark.asyncio
async def test_workflow_composition():
    workflow = DataPipelineWorkflow()
    ctx = Context(workflow)
    workflow_result = await workflow.run(ctx=ctx, input_msg="  Hello World  ")

    assert workflow_result.result == "HELLO WORLD"


@pytest.mark.asyncio
async def test_workflow_composition_lowercase_input():
    workflow = DataPipelineWorkflow()
    ctx = Context(workflow)
    workflow_result = await workflow.run(ctx=ctx, input_msg="  test data  ")

    assert workflow_result.result == "TEST DATA"


@pytest.mark.asyncio
async def test_workflow_composition_mixed_case():
    workflow = DataPipelineWorkflow()
    ctx = Context(workflow)
    workflow_result = await workflow.run(ctx=ctx, input_msg="  MiXeD CaSe  ")

    assert workflow_result.result == "MIXED CASE"


@pytest.mark.asyncio
async def test_sub_workflow_isolation():
    # Test cleaning workflow independently
    cleaning_workflow = DataCleaningWorkflow()
    clean_workflow_result = await cleaning_workflow.run(input_msg="  UPPERCASE  ")
    assert clean_workflow_result.result == "uppercase"

    # Test validation workflow independently
    validation_workflow = DataValidationWorkflow()
    validate_workflow_result = await validation_workflow.run(input_msg="valid data")
    assert validate_workflow_result.result == "valid data"


@pytest.mark.asyncio
async def test_validation_workflow_empty_data():
    validation_workflow = DataValidationWorkflow()

    with pytest.raises(WorkflowRuntimeError, match="Data cannot be empty"):
        await validation_workflow.run(input_msg="")
