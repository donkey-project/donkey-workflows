<div align="center">
  <img alt="Donkey Logo" src=".github/images/favicon.png" width="80px">
  <h2>Donkey Workflows</h2>
  <p><strong>High performance and flexible event-driven workflow engine, designed to build complex tasks.</strong></p>
</div>

<div align="center">
  <img src="https://github.com/donkey-project/donkey-workflows/actions/workflows/test.yml/badge.svg" alt="Testing">
  <img src="https://img.shields.io/pypi/v/donkey-workflows?style=flat&colorA=black&colorB=black" alt="PyPI - Version">
  <a href="https://pepy.tech/projects/donkey-workflows"><img src="https://static.pepy.tech/personalized-badge/donkey-workflows?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=BLACK" alt="Downloads"></a>
  <img src="https://img.shields.io/pypi/l/donkey-workflows?style=flat&colorA=black&colorB=black" alt="PyPI - License">
  <img
    src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"
    alt="Ruff">
</div>

## ✨ Highlight features

<div>
<table>
<thead>
  <tr>
    <th align="left" width="300">Feature</th>
    <th align="center" width="300">Donkey Workflows</th>
  </tr>
</thead>
<tbody>
  <tr><td>Event-driven execution</td><td align="center">✅</td></tr>
  <tr><td>Fan-out (parallelism)</td><td align="center">✅</td></tr>
  <tr><td>Fan-in (joining)</td><td align="center">✅</td></tr>
  <tr><td>Async execution</td><td align="center">✅</td></tr>
  <tr><td>Shared state</td><td align="center">✅</td></tr>
  <tr><td>Internal buffer</td><td align="center">✅</td></tr>
  <tr><td>Declarative API</td><td align="center">✅</td></tr>
  <tr><td>Observability</td><td align="center">✅</td></tr>
</tbody>
</table>
</div>

## 📦 Installation

```bash
pip install donkey-workflows
```

With [uv](https://docs.astral.sh/uv/):

```bash
uv add donkey-workflows
```

## 🚀 Quickstart

Here's a simple example to get you started with Donkey Workflows:

```python
import asyncio
from donkey_workflows import Workflow, Context, step
from donkey_workflows.events import Event, StartEvent, StopEvent


class MessageEvent(Event):
    message: str

class MyWorkflow(Workflow):

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> MessageEvent:
        input_msg = ev.get("message", "")
        return MessageEvent(message=f"Processed: {input_msg}")

    @step(when=MessageEvent)
    async def process(self, ctx: Context, ev: MessageEvent) -> StopEvent:
        return StopEvent(result=ev.message)


async def main():
    workflow = MyWorkflow()
    result = await workflow.run(input_msg="Hello, World!")
    print(result)

asyncio.run(main())
```

## Core Concepts

### Workflow

A workflow is a class that inherits from `Workflow` and contains one or more steps. It orchestrates the execution of steps based on events.

### Steps

Steps are asynchronous methods decorated with `@step(when=EventType)` that define what happens when a specific event is received.

- Steps receive a `Context` and an `Event`.
- Steps can return new events to trigger subsequent steps.

### Events

Events are the building blocks of workflows. They carry data between steps and trigger step execution.

- **StartEvent**: Automatically triggered when a workflow starts
- **StopEvent**: Signals the end of a workflow and carries the final result
- **Custom Events**: Define your own events by inheriting from `Event`

### Context

The `Context` object provides access to workflow state and allows steps to share data throughout the workflow execution.

```python
# Read-only access
current_value = ctx.state.count

# Edit state
async with ctx.store.edit_state() as state:
    state.count = current_counter + 1

# Send events
ctx.send_event(MyEvent(...))
```

## Server

Donkey Workflows includes an optional HTTP server built on FastAPI that exposes your workflows as REST endpoints.

```python
import asyncio
from donkey_workflows.server import WorkflowServer

server = WorkflowServer()
server.add_workflow("my-workflow", MyWorkflow())

asyncio.run(server.serve(host="0.0.0.0", port=8080))
```

| Endpoint | Description |
| :--- | :--- |
| `GET /workflows` | List all registered workflows |
| `POST /workflows/{id}/run` | Execute a workflow |

## License

[Apache License 2.0](LICENSE)
