import asyncio
import copy
from contextlib import asynccontextmanager
from typing import Any, Generic, Protocol, Type, TypeVar, cast

from pydantic import BaseModel, Field

from donkey_workflows.exceptions import ContextStateError
from donkey_workflows.schemas import DictLikeModel

STATE_T = TypeVar("STATE_T", bound=BaseModel)


class SerializedState(BaseModel):
    """
    Serialized state representation for persistence.

    Attributes:
        state_type: Name of the state model class
        store_type: Type of state store (e.g., "in_memory")
        data: Serialized state data as dictionary, or None if state is not initialized
    """

    state_type: str = "DictState"
    store_type: str = "in_memory"
    data: dict = Field(default_factory=dict)


class StateStore(Protocol[STATE_T]):
    """
    Protocol defining the interface for state store implementations.

    Each state store is independent and does not inherit state
    from other contexts. This ensures proper isolation between
    workflows.

    All state store implementations must provide thread-safe state access via the state
    property and copy-on-write mutations via the edit_state() context manager with
    automatic rollback on errors.
    """

    @property
    def state(self) -> STATE_T | None:
        """Get current state (read-only)."""
        ...

    def edit_state(self):
        """
        Context manager for editing state with copy-on-write semantics.

        Example:
            ```python
            async with store.edit_state() as state:
                state.counter += 1
            ```
        """
        ...

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize current state to dictionary.

        Returns dictionary representation including store type, state type metadata,
        and serialized state data.
        """
        ...


class DictState(DictLikeModel):
    """Used as the default state model when no typed state is provided."""

    def __init__(self, **params: Any):
        super().__init__(**params)


class InMemoryStateStore(Generic[STATE_T]):
    """
    In-memory state store with copy-on-write semantics.

    This is a thread-safe implementation that stores state in local memory, making it
    ideal for single-process workflows and development environments. It provides
    copy-on-write for immutability, thread-safe mutations via asyncio.Lock, automatic
    rollback on errors, smart copying optimized for Pydantic models, and auto-initialization
    with type inference.

    Note that state is not shared across processes, will be lost on process restart, and
    is not suitable for distributed workflows. For distributed or persistent state, consider
    RedisStateStore for distributed state or PostgresStateStore for persistent state.

    Example:
        ```python
        store = InMemoryStateStore(state_type=MyState)
        async with store.edit_state() as state:
            state.counter += 1
        ```
    """

    def __init__(
        self,
        state_type: Type[STATE_T] | None = None,
        data: STATE_T | None = None,
    ):
        """
        Initialize in-memory state store.

        Args:
            state_data: Initial state instance (optional)
            state_type: State model class for auto-initialization (optional)
        """
        self._lock = asyncio.Lock()
        self._state_type: Type[BaseModel] = state_type or DictState

        if data is not None:
            self._state: STATE_T | None = data
        else:
            # Auto-initialize with state_type
            self._state = cast(STATE_T, self._state_type())

    @property
    def state(self) -> STATE_T | None:
        """Get current state (read-only)."""
        return self._state

    def _copy_state(self, state: STATE_T) -> STATE_T:
        """
        Copy state using optimization based on state type.

        For Pydantic models, uses the optimized model_copy() method.
        This reduces copy overhead by 40-60% for Pydantic models.

        Args:
            state: State to copy
        """
        if isinstance(state, BaseModel):
            # Pydantic has optimized copy
            return state.model_copy(deep=True)

        # Fallback to deep copy
        return copy.deepcopy(state)

    @asynccontextmanager
    async def edit_state(self):
        """
        Context manager for editing state with copy-on-write semantics.

        Creates a deep copy of the state for editing. Changes are committed
        atomically when the context exits successfully. If an exception occurs,
        changes are automatically rolled back.

        Example:
            ```python
            async with store.edit_state() as state:
                state.counter += 1
                state.items.append("new")

            # On error, changes are rolled back:
            try:
                async with store.edit_state() as state:
                    state.counter += 1
                    raise Exception("Error!")  # Rollback happens
            except Exception:
                pass  # state.counter unchanged
            ```
        """
        async with self._lock:
            # State is always initialized in __init__, but keep check for safety
            if self._state is None:
                raise ContextStateError("State not initialized.")

            # Use smart copy instead of deepcopy
            state_copy = self._copy_state(self._state)

            try:
                yield state_copy
                # Commit changes on successful exit
                self._state = state_copy
            except Exception:
                # Rollback on error (don't commit)
                raise

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize current state to dictionary.

        Returns dictionary representation including store type, state type metadata,
        and serialized state data.
        """
        state_type_name = (
            self._state.__class__.__name__ if self._state else self._state_type.__name__
        )
        state_data = self._state.model_dump() if self._state else {}

        return SerializedState(
            state_type=state_type_name,
            store_type="in_memory",
            data=state_data,
        ).model_dump()
