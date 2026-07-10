import asyncio
from typing import Any

import uvicorn
from fastapi import FastAPI

from donkey_workflows.server._registry import WorkflowRegistry
from donkey_workflows.workflow import Workflow


class WorkflowServer(FastAPI):
    """
    HTTP server runtime for workflow management and execution.

    Provides a production-ready HTTP API server for managing and executing workflows.
    This FastAPI-based server exposes RESTful endpoints for workflow registration,
    discovery. The server maintains an internal registry of workflowsand handles
    concurrent execution.
    """

    def __init__(self):
        """Initialize the workflow server."""
        super().__init__()
        self._registered_workflows = WorkflowRegistry()

        from donkey_workflows.server._api.routes import setup_routes

        setup_routes(self)

    def add_workflow(
        self,
        name: str,
        workflow: Workflow,
    ) -> str:
        """
        Register a workflow with the server (synchronous method).

        Args:
            name: The workflow name.
            workflow: The workflow instance to register.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            workflow_id = loop.run_until_complete(
                self._registered_workflows.track_workflow(name, workflow)
            )
        finally:
            loop.close()

        return workflow_id

    async def serve(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        **kwargs: Any,
    ) -> None:
        """
        Start the workflow server.

        Args:
            host: The host to bind to (default: "0.0.0.0").
            port: The port to bind to (default: 8080).
            **kwargs: Additional arguments passed to uvicorn.Server.
        """
        config = uvicorn.Config(
            app=self,
            host=host,
            port=port,
            **kwargs,
        )
        server = uvicorn.Server(config)
        await server.serve()
