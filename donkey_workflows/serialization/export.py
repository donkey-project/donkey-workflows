from pydantic import BaseModel, Field


class EventFieldExport(BaseModel):
    type: str
    required: bool


class EventExport(BaseModel):
    code: str | None = None
    fields: dict[str, EventFieldExport] = Field(default_factory=dict)


class StepExport(BaseModel):
    name: str
    triggers: list[str]
    produces: list[str]
    is_join: bool
    timeout: float | None = None
    max_retries: int = 0
    retry_delay: float = 1.0
    code: str | None = None


class WorkflowExport(BaseModel):
    version: str = "1.0.0"
    kind: str = "Workflow"
    name: str
    module: str | None = None
    description: str = ""
    state_type: str
    state_code: str | None = None
    code: str | None = None
    steps: list[StepExport] = Field(default_factory=list)
    events: dict[str, EventExport] = Field(default_factory=dict)
