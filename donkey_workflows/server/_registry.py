import asyncio
import uuid

from pydantic import BaseModel, Field

from donkey_workflows.server.exceptions import WorkflowNotFoundError
from donkey_workflows.workflow import Workflow


class WorkflowInstance(BaseModel):
    """
    Internal workflow registration for in-memory execution.

    Contains the actual workflow instance for execution. Used by the
    server's registry to manage and execute workflows.

    Attributes:
        id_: Stable workflow identifier definition across different deployments.
        deployment_id: Unique identifier derived from the deploy name (UUID v5).
        name: The deployment name (e.g. "workflow_prod", "workflow_dev").
        workflow_instance: The actual workflow instance.
    """

    model_config = {"arbitrary_types_allowed": True}

    id_: str = Field(
        ..., description="Workflow definition identifier"
    )
    deployment_id: str = Field(
        ..., description="Unique deployment identifier (UUID)"
    )
    name: str = Field(..., description="Deployment name")
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

        The deployment_id is derived deterministically from the deploy name,
        so registering the same name twice replaces the previous deployment.
        Different names always produce different deployment_ids, allowing
        multiple deployments of the same workflow class (e.g. prod/dev).

        Args:
            name: The deploy name (e.g. "workflow_prod", "workflow_dev").
            workflow: The workflow instance to register.
        """
        id_ = str(uuid.uuid5(uuid.NAMESPACE_DNS, workflow.__class__.__name__))
        deployment_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, name))

        async with self._lock:
            self._workflows[deployment_id] = WorkflowInstance(
                id_=id_,
                deployment_id=deployment_id,
                name=name,
                workflow_instance=workflow,
            )

        return deployment_id

    async def get(self, deployment_id: str) -> WorkflowInstance:
        """
        Get a workflow by its deployment_id.

        Args:
            deployment_id: The deployment_id to retrieve.
        """
        async with self._lock:
            if deployment_id not in self._workflows:
                raise WorkflowNotFoundError(
                    f"The specified deployment '{deployment_id}' could not be found"
                )
            return self._workflows[deployment_id]

    async def list(self) -> list[WorkflowInstance]:
        """
        List all registered workflows for in-memory execution.

        Returns:
            A list of all workflow instances in-memory.
        """
        async with self._lock:
            return list(self._workflows.values())
