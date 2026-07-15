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
    code: str | None = None
    fields: dict[str, EventFieldSpec] = Field(default_factory=dict)


class StepSpec(BaseModel):
    name: str
    triggers: list[str]
    produces: list[str]
    is_join: bool
    timeout: float | None = None
    max_retries: int = 0
    retry_delay: float = 1.0
    code: str | None = None


class WorkflowSpec(BaseModel):
    version: str = "1.0.0"
    kind: str = "Workflow"
    name: str
    module: str | None = None
    description: str = ""
    state_type: str
    state_code: str | None = None
    code: str | None = None
    steps: list[StepSpec] = Field(default_factory=list)
    events: dict[str, EventSpec] = Field(default_factory=dict)
    dependencies: DependenciesSpec = Field(default_factory=DependenciesSpec)
