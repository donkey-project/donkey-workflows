import asyncio
import uuid

from pydantic import BaseModel, Field

from donkey_workflows.server.exceptions import WorkflowNotFoundError
from donkey_workflows.workflow import Workflow


def generate_workflow_id(workflow_name: str) -> str:
    """
    Generate a deterministic UUID v5 from a workflow name.

    Uses a fixed namespace to ensure the same workflow name always
    generates the same UUID across different server instances.

    Args:
        workflow_name: The name of the workflow.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, workflow_name))


class WorkflowInstance(BaseModel):
    """
    Internal workflow registration for in-memory execution.

    Contains the actual workflow instance for execution. Used by the
    server's registry to manage and execute workflows.

    Attributes:
        id_: Unique workflow identifier.
        name: The workflow name.
        workflow_instance: The actual workflow instance.
    """

    model_config = {"arbitrary_types_allowed": True}

    id_: str = Field(..., description="Unique workflow identifier")
    name: str = Field(..., description="Workflow name")
    workflow_instance: Workflow = Field(..., description="Workflow instance")


class WorkflowRegistry:
    """
    Thread-safe registry for managing workflow instances.

    Uses asyncio.Lock to ensure thread-safe access to the registry.
    """

    def __init__(self):
        """Initialize the workflow registry."""
        self._workflows: dict[str, WorkflowInstance] = {}
        self._lock = asyncio.Lock()

    async def track_workflow(self, name: str, workflow: Workflow) -> str:
        """
        Track a workflow instance at launch time.

        If a workflow with the same name already exists, it will be replaced.

        Args:
            name: The workflow name.
            workflow: The workflow instance to register.
        """
        workflow_id = generate_workflow_id(name)

        async with self._lock:
            self._workflows[workflow_id] = WorkflowInstance(
                id_=workflow_id,
                name=name,
                workflow_instance=workflow,
            )

        return workflow_id

    async def get(self, workflow_id: str) -> WorkflowInstance:
        """
        Get a workflow by its ID.

        Args:
            workflow_id: The ID of the workflow to retrieve.
        """
        async with self._lock:
            if workflow_id not in self._workflows:
                raise WorkflowNotFoundError(
                    f"The specified workflow '{workflow_id}' could not be found"
                )
            return self._workflows[workflow_id]

    async def list(self) -> list[WorkflowInstance]:
        """
        List all registered workflows for in-memory execution.

        Returns:
            A list of all workflow instances in-memory.
        """
        async with self._lock:
            return list(self._workflows.values())
