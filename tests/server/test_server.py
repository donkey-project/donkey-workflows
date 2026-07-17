import asyncio
import uuid

import pytest
import pytest_asyncio
from httpx import AsyncClient

from donkey_workflows import Context, Workflow, step
from donkey_workflows.events import StartEvent, StopEvent
from donkey_workflows.server import WorkflowNotFoundError, WorkflowServer
from donkey_workflows.server._registry import WorkflowRegistry


@pytest.fixture
def test_workflow():
    """Create a simple test workflow."""

    class TestWorkflow(Workflow):
        @step(when=StartEvent)
        async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
            name = ev.get("name", "World")
            return StopEvent(result=f"Hello, {name}!")

    return TestWorkflow()


@pytest.fixture
def complex_workflow():
    """Create a workflow with complex input handling."""

    class ComplexWorkflow(Workflow):
        @step(when=StartEvent)
        async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
            value1 = ev.get("value1", "default1")
            value2 = ev.get("value2", 0)
            value3 = ev.get("value3", [])

            result = {
                "value1": value1,
                "value2": value2,
                "value3": value3,
                "total": value2 + len(value3),
            }
            return StopEvent(result=result)

    return ComplexWorkflow()


@pytest.fixture
def error_workflow():
    """Create a workflow that raises an error."""

    class ErrorWorkflow(Workflow):
        """Workflow that intentionally raises an error."""

        @step(when=StartEvent)
        async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
            raise ValueError("Intentional test error")

    return ErrorWorkflow()


@pytest.fixture
def server():
    """Create a WorkflowServer instance."""
    return WorkflowServer()


@pytest_asyncio.fixture
async def client(server):
    """Create an async HTTP client for testing."""
    from httpx import ASGITransport

    async with AsyncClient(
        transport=ASGITransport(app=server), base_url="http://test"
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_registry_track_workflow(test_workflow):
    """Test workflow registration."""
    registry = WorkflowRegistry()

    deployment_id = await registry.track_workflow(
        name="test_workflow", workflow=test_workflow
    )

    assert isinstance(deployment_id, str)

    instance = await registry.get(deployment_id)
    assert instance.id_ == str(uuid.uuid5(uuid.NAMESPACE_DNS, "TestWorkflow"))
    assert instance.deployment_id == deployment_id
    assert instance.name == "test_workflow"
    assert instance.workflow_instance == test_workflow


@pytest.mark.asyncio
async def test_registry_track_duplicate_replaces(test_workflow):
    """Test that registering duplicate workflow name replaces the old one."""
    registry = WorkflowRegistry()

    deployment_id1 = await registry.track_workflow(
        name="duplicate", workflow=test_workflow
    )

    deployment_id2 = await registry.track_workflow(
        name="duplicate", workflow=test_workflow
    )

    assert deployment_id1 == deployment_id2

    instance = await registry.get(deployment_id1)
    assert instance.name == "duplicate"


@pytest.mark.asyncio
async def test_registry_get_workflow(test_workflow):
    """Test retrieving registered workflows."""
    registry = WorkflowRegistry()

    deployment_id = await registry.track_workflow("test", test_workflow)
    instance = await registry.get(deployment_id)

    assert instance.id_ == str(uuid.uuid5(uuid.NAMESPACE_DNS, "TestWorkflow"))
    assert instance.deployment_id == deployment_id
    assert instance.name == "test"
    assert instance.workflow_instance == test_workflow


@pytest.mark.asyncio
async def test_registry_get_nonexistent_raises_error():
    """Test that getting non-existent workflow raises WorkflowNotFoundError."""
    registry = WorkflowRegistry()

    fake_id = "12345678-1234-5678-1234-567812345678"

    with pytest.raises(WorkflowNotFoundError) as exc_info:
        await registry.get(fake_id)

    assert fake_id in str(exc_info.value)


@pytest.mark.asyncio
async def test_registry_list_empty():
    """Test listing workflows when registry is empty."""
    registry = WorkflowRegistry()

    workflows = await registry.list()
    assert workflows == []


@pytest.mark.asyncio
async def test_registry_list(test_workflow, complex_workflow):
    """Test listing registered workflows."""
    registry = WorkflowRegistry()

    id1 = await registry.track_workflow("workflow1", test_workflow)
    id2 = await registry.track_workflow("workflow2", complex_workflow)

    workflows = await registry.list()

    assert len(workflows) == 2

    deployment_ids = {w.deployment_id for w in workflows}
    assert id1 in deployment_ids
    assert id2 in deployment_ids

    names = {w.name for w in workflows}
    assert "workflow1" in names
    assert "workflow2" in names


@pytest.mark.asyncio
async def test_registry_thread_safety(test_workflow):
    """Test thread-safety with concurrent registration attempts."""
    registry = WorkflowRegistry()

    async def register_workflow(name: str):
        """Helper to register a workflow."""
        return await registry.track_workflow(name, test_workflow)

    tasks = [register_workflow(f"workflow_{i}") for i in range(10)]

    workflow_ids = await asyncio.gather(*tasks)

    assert len(workflow_ids) == 10
    assert all(isinstance(wid, str) for wid in workflow_ids)

    workflows = await registry.list()
    assert len(workflows) == 10


@pytest.mark.asyncio
async def test_list_workflows_empty(client):
    """Test GET /workflows with no workflows registered."""
    response = await client.get("/workflows")

    assert response.status_code == 200
    data = response.json()
    assert "workflows" in data
    assert data["workflows"] == []


@pytest.mark.asyncio
async def test_list_workflows_with_registered(server, client, test_workflow):
    """Test GET /workflows returns list of registered workflows."""
    workflow_id = await asyncio.to_thread(
        server.add_workflow,
        name="test_workflow",
        workflow=test_workflow,
    )

    response = await client.get("/workflows")

    assert response.status_code == 200
    data = response.json()
    assert "workflows" in data
    assert len(data["workflows"]) == 1

    workflow_info = data["workflows"][0]
    assert workflow_info["deployment_id"] == str(workflow_id)
    assert workflow_info["name"] == "test_workflow"


@pytest.mark.asyncio
async def test_list_workflows_multiple(server, client, test_workflow, complex_workflow):
    """Test GET /workflows with multiple registered workflows."""
    await asyncio.to_thread(server.add_workflow, "workflow1", test_workflow)
    await asyncio.to_thread(server.add_workflow, "workflow2", complex_workflow)

    response = await client.get("/workflows")

    assert response.status_code == 200
    data = response.json()
    assert len(data["workflows"]) == 2

    names = {w["name"] for w in data["workflows"]}
    assert "workflow1" in names
    assert "workflow2" in names


@pytest.mark.asyncio
async def test_run_workflow_success(server, client, test_workflow):
    """Test POST /workflows/{id}/run executes workflow successfully."""
    workflow_id = await asyncio.to_thread(server.add_workflow, "greet", test_workflow)

    response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {"name": "Alice"}}
    )

    assert response.status_code == 200
    data = response.json()

    assert "run_id" in data
    assert "status" in data
    assert "result" in data
    assert "execution_duration" in data

    assert data["status"] == "completed"
    assert data["result"] == "Hello, Alice!"
    assert isinstance(data["execution_duration"], (int, float))
    assert data["execution_duration"] >= 0


@pytest.mark.asyncio
async def test_run_workflow_with_default_params(server, client, test_workflow):
    """Test workflow execution with default parameters."""
    workflow_id = await asyncio.to_thread(server.add_workflow, "greet", test_workflow)

    response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {}}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["result"] == "Hello, World!"


@pytest.mark.asyncio
async def test_run_workflow_not_found(client):
    """Test POST /workflows/{id}/run returns 404 for non-existent workflow."""
    fake_id = "12345678-1234-5678-1234-567812345678"

    response = await client.post(f"/workflows/{fake_id}/run", json={"start_event": {}})

    assert response.status_code == 404
    data = response.json()
    assert "detail" in data

    detail = data["detail"]
    assert "error" in detail
    error = detail["error"]
    assert error["status_code"] == 404
    assert error["code"] == "WorkflowNotFoundError"
    assert fake_id in error["message"]


@pytest.mark.asyncio
async def test_run_workflow_execution_error(server, client, error_workflow):
    """Test POST /workflows/{id}/run returns 500 for workflow execution errors."""
    workflow_id = await asyncio.to_thread(server.add_workflow, "error", error_workflow)

    response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {}}
    )

    assert response.status_code == 500
    data = response.json()
    assert "detail" in data

    detail = data["detail"]
    assert "error" in detail
    error = detail["error"]
    assert "code" in error
    assert "message" in error
    assert error["code"] == "WorkflowRuntimeError"


@pytest.mark.asyncio
async def test_run_workflow_complex_input(server, client, complex_workflow):
    """Test workflow execution with multiple input parameters."""
    workflow_id = await asyncio.to_thread(
        server.add_workflow, "complex", complex_workflow
    )

    response = await client.post(
        f"/workflows/{workflow_id}/run",
        json={
            "start_event": {
                "value1": "test_string",
                "value2": 10,
                "value3": ["a", "b", "c"],
            }
        },
    )

    assert response.status_code == 200
    data = response.json()

    result = data["result"]
    assert result["value1"] == "test_string"
    assert result["value2"] == 10
    assert result["value3"] == ["a", "b", "c"]
    assert result["total"] == 13


@pytest.mark.asyncio
async def test_run_workflow_no_input(server, client, test_workflow):
    """Test workflow execution with no input parameters."""
    workflow_id = await asyncio.to_thread(server.add_workflow, "greet", test_workflow)

    response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {}}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["result"] == "Hello, World!"


@pytest.mark.asyncio
async def test_run_workflow_timing(server, client, test_workflow):
    """Test that execution time is tracked correctly."""
    workflow_id = await asyncio.to_thread(server.add_workflow, "greet", test_workflow)

    response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {"name": "Bob"}}
    )

    assert response.status_code == 200
    data = response.json()

    assert data["execution_duration"] > 0
    assert data["execution_duration"] < 10


@pytest.mark.asyncio
async def test_complete_workflow_lifecycle(server, client, test_workflow):
    """Test complete workflow: register → list → execute."""
    workflow_id = await asyncio.to_thread(
        server.add_workflow,
        name="lifecycle_test",
        workflow=test_workflow,
    )
    assert isinstance(workflow_id, str)

    list_response = await client.get("/workflows")
    assert list_response.status_code == 200
    workflows = list_response.json()["workflows"]
    assert len(workflows) == 1
    assert workflows[0]["deployment_id"] == str(workflow_id)
    assert workflows[0]["name"] == "lifecycle_test"

    run_response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {"name": "Integration"}}
    )
    assert run_response.status_code == 200
    result = run_response.json()
    assert result["status"] == "completed"
    assert result["result"] == "Hello, Integration!"


@pytest.mark.asyncio
async def test_multiple_workflows_registration_and_execution(
    server, client, test_workflow, complex_workflow
):
    """Test multiple workflows can be registered and executed."""
    id1 = await asyncio.to_thread(server.add_workflow, "workflow1", test_workflow)
    id2 = await asyncio.to_thread(server.add_workflow, "workflow2", complex_workflow)

    list_response = await client.get("/workflows")
    workflows = list_response.json()["workflows"]
    assert len(workflows) == 2

    response1 = await client.post(
        f"/workflows/{id1}/run", json={"start_event": {"name": "Test1"}}
    )
    assert response1.status_code == 200
    assert response1.json()["status"] == "completed"
    assert response1.json()["result"] == "Hello, Test1!"

    response2 = await client.post(
        f"/workflows/{id2}/run",
        json={"start_event": {"value1": "test", "value2": 5, "value3": [1, 2]}},
    )
    assert response2.status_code == 200
    result2 = response2.json()["result"]
    assert result2["total"] == 7


@pytest.mark.asyncio
async def test_workflow_with_no_input_parameters(server, client):
    """Test workflow with no input parameters."""

    class NoInputWorkflow(Workflow):
        @step(when=StartEvent)
        async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
            return StopEvent(result="No input needed")

    workflow = NoInputWorkflow()
    workflow_id = await asyncio.to_thread(server.add_workflow, "no_input", workflow)

    response = await client.post(
        f"/workflows/{workflow_id}/run", json={"start_event": {}}
    )

    assert response.status_code == 200
    assert response.json()["result"] == "No input needed"


@pytest.mark.asyncio
async def test_concurrent_workflow_executions(server, client, test_workflow):
    """Test concurrent executions of the same workflow."""
    workflow_id = await asyncio.to_thread(
        server.add_workflow, "concurrent", test_workflow
    )

    async def execute_workflow(name: str):
        response = await client.post(
            f"/workflows/{workflow_id}/run", json={"start_event": {"name": name}}
        )
        return response.json()

    tasks = [execute_workflow(f"User{i}") for i in range(5)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 5
    for i, result in enumerate(results):
        assert result["status"] == "completed"
        assert result["result"] == f"Hello, User{i}!"


@pytest.mark.asyncio
async def test_workflow_deployment_id_deterministic_across_servers(test_workflow):
    """Test that deployment IDs are deterministic across different server instances."""
    server1 = WorkflowServer()
    server2 = WorkflowServer()

    id1 = await asyncio.to_thread(server1.add_workflow, "same_name", test_workflow)
    id2 = await asyncio.to_thread(server2.add_workflow, "same_name", test_workflow)

    assert id1 == id2


@pytest.mark.asyncio
async def test_server_add_workflow_returns_uuid(server, test_workflow):
    """Test that add_workflow returns a valid UUID."""
    deployment_id = await asyncio.to_thread(server.add_workflow, "test", test_workflow)

    assert isinstance(deployment_id, str)

    from uuid import UUID

    uuid_obj = UUID(deployment_id)
    assert uuid_obj.version == 5


@pytest.mark.asyncio
async def test_workflow_metadata_persistence(server, client, test_workflow):
    """Test that workflow metadata persists correctly."""
    workflow_id = await asyncio.to_thread(
        server.add_workflow,
        name="metadata_test",
        workflow=test_workflow,
    )

    response = await client.get("/workflows")
    workflows = response.json()["workflows"]

    assert len(workflows) == 1
    workflow_info = workflows[0]

    assert workflow_info["deployment_id"] == str(workflow_id)
    assert workflow_info["name"] == "metadata_test"
