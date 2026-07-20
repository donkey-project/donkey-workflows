from pydantic import BaseModel, Field


class DependencyPackageSpec(BaseModel):
    name: str
    version: str


class DependenciesSpec(BaseModel):
    imports: list[str] = Field(default_factory=list)
    packages: list[DependencyPackageSpec] = Field(default_factory=list)


class EventFieldSpec(BaseModel):
    type: str
    required: bool


class EventSpec(BaseModel):
    name: str
    code: str | None = None
    fields: dict[str, EventFieldSpec] = Field(default_factory=dict)


class StepSpec(BaseModel):
    name: str
    inputs: list[str]
    outputs: list[str]
    is_join_step: bool
    timeout: float | None = None
    max_retries: int = 0
    retry_delay: float = 1.0
    code: str | None = None


class WorkflowSpec(BaseModel):
    state_type: str
    state_code: str | None = None
    code: str | None = None
    steps: list[StepSpec] = Field(default_factory=list)
    events: list[EventSpec] = Field(default_factory=list)
    dependencies: DependenciesSpec = Field(default_factory=DependenciesSpec)


class WorkflowManifest(BaseModel):
    id_: str
    api_version: str = "v1.0"
    kind: str = "Workflow"
    name: str
    module: str | None = None
    description: str = ""
    data: WorkflowSpec
