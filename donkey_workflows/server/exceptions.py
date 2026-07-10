from donkey_workflows.exceptions import WorkflowError


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow ID is not found in the registry."""
