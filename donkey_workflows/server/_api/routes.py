import time
from datetime import datetime

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse

from donkey_workflows.exceptions import WorkflowRuntimeError
from donkey_workflows.server._api.models import (
    WorkflowListResponse,
    WorkflowMetadata,
    WorkflowRunRequest,
    WorkflowRunResponse,
)
from donkey_workflows.server.exceptions import WorkflowNotFoundError


def setup_routes(app):
    """Setup API routes and exception handlers for the workflow server."""

    @app.get("/workflows", response_model=WorkflowListResponse)
    async def list_workflows() -> WorkflowListResponse:
        """List all registered workflows."""
        workflows = await app._registered_workflows.list()

        workflow_infos = [
            WorkflowMetadata(
                id_=wf.id_,
                name=wf.name,
                description=None,
            )
            for wf in workflows
        ]

        return WorkflowListResponse(workflows=workflow_infos)

    @app.post("/workflows/{workflow_id}/run", response_model=WorkflowRunResponse)
    async def run_workflow(
        workflow_id: str,
        request: WorkflowRunRequest,
    ) -> WorkflowRunResponse:
        """Execute a workflow and return the result."""
        try:
            metadata = await app._registered_workflows.get(workflow_id)
        except WorkflowNotFoundError as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": {
                        "status_code": status.HTTP_404_NOT_FOUND,
                        "code": "WorkflowNotFoundError",
                        "message": f"The specified workflow '{workflow_id}' could not be found",
                    }
                },
            )

        start_time = time.perf_counter()

        try:
            workflow_result = await metadata.workflow_instance.run(
                **request.start_event, **request.context
            )
            execution_duration = time.perf_counter() - start_time

            return WorkflowRunResponse(
                run_id=workflow_result.run_id,
                status=workflow_result.status,
                result=workflow_result.result,
                execution_duration=execution_duration,
            )
        except Exception as e:
            execution_duration = time.perf_counter() - start_time

            if isinstance(e, WorkflowRuntimeError):
                error_code = "WorkflowRuntimeError"
                error_message = (
                    "The workflow execution encountered an error during processing"
                )
            else:
                error_code = "InternalServerError"
                error_message = (
                    "An unexpected error occurred while executing the workflow"
                )

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error": {
                        "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                        "code": error_code,
                        "message": error_message,
                        "workflow_id": str(workflow_id),
                    }
                },
            )

    @app.exception_handler(WorkflowNotFoundError)
    async def workflow_not_found_handler(request, exc: WorkflowNotFoundError):
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=exc,
        )

    @app.exception_handler(WorkflowRuntimeError)
    async def workflow_runtime_error_handler(request, exc: WorkflowRuntimeError):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=exc,
        )
