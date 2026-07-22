from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class WorkflowMetadata(BaseModel):
    """
    Workflow metadata.

    Attributes:
        id_: Workflow definition identifier (UUID v5 of class name).
        deployment_id: Unique deployment identifier (UUID v5 of deploy name).
        name: The deployment alias.
        description: Optional workflow description.
    """

    id_: str = Field(..., description="Workflow definition identifier")
    deployment_id: str = Field(..., description="Unique deployment identifier")
    name: str = Field(..., description="Deployment alias")
    description: str | None = Field(None, description="Workflow description")


class WorkflowListResponse(BaseModel):
    """
    Response model for listing all registered workflows.

    Attributes:
        workflows: List of workflow metadata.
    """

    workflows: list[WorkflowMetadata] = Field(
        default_factory=list, description="List of registered workflows"
    )


class WorkflowRunRequest(BaseModel):
    """
    Request model for executing a workflow.

    Attributes:
        start_event: Dictionary of input parameters to pass to the workflow.
        context: Dictionary of input parameters to pass to the workflow.
    """

    start_event: dict[str, Any] = Field(
        default_factory=dict, description="StartEvent parameters for workflow execution"
    )
    context: dict[str, Any] = Field(
        default_factory=dict, description="Context parameters for workflow execution"
    )


class WorkflowRunResponse(BaseModel):
    """
    Response model for workflow execution.

    Attributes:
        run_id: Unique identifier for this execution run.
        status: Execution status (success, error).
        result: The workflow execution result.
        execution_duration: Execution time in seconds.
    """

    run_id: str = Field(..., description="Unique execution run identifier")
    status: str = Field(..., description="Execution status")
    result: Any = Field(None, description="Workflow execution result")
    execution_duration: float = Field(..., description="Execution duration in seconds")
