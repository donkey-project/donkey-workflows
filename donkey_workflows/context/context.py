import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, Field

from donkey_workflows.context.state_store import InMemoryStateStore

if TYPE_CHECKING:
    from donkey_workflows.events import Event

STATE_T = TypeVar("STATE_T", bound=BaseModel)


class SerializedContext(BaseModel):
    version: str = "1.0.0"
    kind: str = "Context"
    state: dict = Field(default_factory=dict)


class Context(Generic[STATE_T]):
    """
    Workflow execution context with copy-on-write state management.

    Provides robust, immutable state management through 'state_store'.

    State is automatically initialized based on the type annotation.
    If no type is provided, uses DictState.

    Example:
        ```python
        class RunState(BaseModel):
            counter: int = 0
            items: list[str] = []


        # With typed state - auto-initialized
        @step
        async def start(self, ctx: Context[RunState], ev: StartEvent):
            async with ctx.store.edit_state() as state:
                state.counter += 1  # RunState auto-initialized


        @step
        async def start(self, ctx: Context, ev: StartEvent):
            async with ctx.store.edit_state() as state:
                state.counter = 1
        ```
    """

    def __init__(
        self,
        workflow: Any,
        event_queue: asyncio.Queue["Event"] | Any | None = None,
    ):
        """
        Initialize workflow context.

        Each context is isolated and maintains its own state store.
        State type is inferred from Context[StateType] annotation or defaults to DictState.

        Args:
            workflow: Workflow instance
            event_queue: Event queue for emission
        """
        self._workflow = workflow

        # Use provided queue or create asyncio.Queue
        if event_queue is None:
            self._event_queue: asyncio.Queue[Event] = asyncio.Queue()
        else:
            self._event_queue = event_queue

        # Copy-on-write state store with auto-initialization
        # Infer state type from workflow's _state_type if available
        state_type = getattr(workflow, "_state_type", None)
        self._store = InMemoryStateStore(state_type=state_type)

    @property
    def workflow(self) -> Any:
        """Get workflow instance."""
        return self._workflow

    @property
    def store(self):  # type: ignore[return]
        """
        Copy-on-write state store.

        Provides immutability guarantees and thread-safety through
        explicit edit contexts.

        Example:
            ```python
            # Edit state
            async with ctx.store.edit_state() as state:
                state.counter += 1
            ```
        """
        return self._store

    @property
    def state(self) -> BaseModel | None:
        """
        Get current state (read-only).

        Convenience property for accessing store state.
        Equivalent to ctx.store.state.

        Example:
            ```python
            # Read-only access
            current_value = ctx.state.counter
            ```
        """
        return self._store.state

    async def send_event(self, event: "Event") -> None:
        """
        Send an event to the workflow queue.

        Args:
            event(Event): Event to send

        Example:
            ```python
            await ctx.send_event(MyEvent(data="processed"))
            ```
        """
        await self._event_queue.put(event)

    def to_dict(self) -> dict[str, Any]:
        """Serialize context to dictionary."""
        return SerializedContext(state=self._store.to_dict()).model_dump()
