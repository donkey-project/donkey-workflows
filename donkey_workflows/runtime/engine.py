import asyncio
import uuid
from typing import TYPE_CHECKING, Any

from donkey_instrumentation import get_dispatcher

from donkey_workflows.context import Context
from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.exceptions import (
    WorkflowRuntimeError,
    WorkflowValidationError,
)
from donkey_workflows.runtime.event_buffer import EventBuffer
from donkey_workflows.runtime.execution_pool import ExecutionPool
from donkey_workflows.runtime.step_function import StepExecutor
from donkey_workflows.schemas import WorkflowResult, WorkflowStatus
from donkey_workflows.step_metadata import (
    get_step_max_retries,
    get_step_retry_delay,
    get_step_timeout,
)

if TYPE_CHECKING:
    from donkey_workflows import Workflow

_dispatcher = get_dispatcher(__name__)


class WorkflowEngine:
    """
    Executes workflow steps based on event-driven architecture with task-driven runtime.

    The WorkflowEngine manages the execution of decorator-based workflow steps
    by processing events through an event queue. It handles event dispatching,
    parallel step execution (fan-out), and error handling.

    The task-driven runtime allows steps to execute independently and asynchronously,
    with downstream events emitted immediately upon step completion rather than
    waiting for sibling steps to finish.

    The workflow runs until one of these conditions:
    - A StopEvent is received
    - No more work available (empty queue + no active tasks)
    - Deadlock detected (buffered events but no executable steps)

    Attributes:
        workflow: The workflow instance containing decorated step methods.
        queue_wait_timeout: Timeout in seconds for queue operations (default: 1.0s).
        max_workers: Maximum concurrent step executions (default: 100).
        max_tasks: Maximum number of tasks in memory (default: 500).
        max_buffer_size: Maximum number of events that can be buffered (default: 1000).
    """

    def __init__(
        self,
        workflow: "Workflow",
        queue_wait_timeout: float = 1.0,
        max_workers: int = 50,
        max_tasks: int = 250,
        max_buffer_size: int = 500,
    ) -> None:
        if queue_wait_timeout <= 0:
            raise WorkflowValidationError("'queue_wait_timeout' must be greater than 0")
        if max_buffer_size <= 0:
            raise WorkflowValidationError("'max_buffer_size' must be greater than 0")
        if max_tasks < max_workers:
            raise WorkflowValidationError(
                f"'max_tasks' ({max_tasks}) must be >= 'max_workers' ({max_workers})"
            )

        self._workflow = workflow
        self._queue_wait_timeout = queue_wait_timeout
        self._pool = ExecutionPool(max_workers=max_workers, max_tasks=max_tasks)
        self._event_buffer = EventBuffer()
        self._max_buffer_size = max_buffer_size
        # Track join steps that are scheduled to prevent duplicate execution
        self._scheduled_join_steps: set[str] = set()
        self._join_lock = asyncio.Lock()
        # Queue for task exceptions
        self._exception_queue: asyncio.Queue[Exception] = asyncio.Queue()
        self._step_executor = StepExecutor(
            exception_queue=self._exception_queue,
            event_buffer=self._event_buffer,
            pool=self._pool,
            workflow=self._workflow,
        )

    async def run(
        self,
        start_events: list[StartEvent] | StartEvent,
        ctx: Context | None = None,
    ) -> WorkflowResult:
        """
        Run the workflow with initial events using task-driven runtime.

        This is the main entry point for workflow execution. It creates a
        Context (if not provided), enqueues initial events, and dispatches
        step tasks asynchronously without blocking on individual step completion.

        The workflow runs until completion (StopEvent), no more work, or deadlock.

        Args:
            start_events: Single event or list of events to start the workflow.
            ctx: Optional pre-configured context with optional state data.

        Returns:
            WorkflowResult with run_id, iteration count, status, and result.

        Note:
            The workflow uses a task-driven runtime where steps execute independently.
            Events are dispatched immediately when ready, and downstream events are
            emitted as soon as steps complete, enabling true parallel execution.
        """

        @_dispatcher.span
        async def _run():
            return await self._run_control_loop(start_events, ctx)

        _run.__name__ = "run"
        _run.__qualname__ = "Workflow.run"

        return await _run()

    async def _run_control_loop(
        self,
        start_events: list[StartEvent] | StartEvent,
        ctx: Context | None = None,
    ) -> WorkflowResult:
        run_id = str(uuid.uuid4())

        # Initialize event queue
        event_queue: asyncio.Queue[Event] = asyncio.Queue()

        # Create or use provided context
        if ctx is None:
            ctx = Context(
                workflow=self._workflow,
                event_queue=event_queue,
            )
        else:
            # Use provided context but ensure it has the event queue
            ctx._event_queue = event_queue

        # Enqueue initial events
        if isinstance(start_events, StartEvent):
            start_events = [start_events]

        for event in start_events:
            await event_queue.put(event)

        # Process event queue with task-driven dispatcher
        result: Any = None

        try:
            while True:
                # Check for task exceptions first (non-blocking)
                if not self._exception_queue.empty():
                    exc = await self._exception_queue.get()
                    await self._pool.shutdown()
                    raise exc

                # Check if we have active tasks or events in queue
                has_work = self._pool.is_active or not event_queue.empty()

                if not has_work:
                    # No work available - check for task exceptions
                    if not self._exception_queue.empty():
                        exc = await self._exception_queue.get()
                        await self._pool.shutdown()
                        raise exc

                    # No work available - check for deadlock before completing
                    self._check_for_deadlock(ctx)
                    break

                # Try to get next event with timeout
                try:
                    event = await asyncio.wait_for(
                        event_queue.get(),
                        timeout=self._queue_wait_timeout,
                    )
                except asyncio.TimeoutError:
                    # Queue empty but tasks may still be running
                    if self._pool.is_active:
                        # Check for exceptions while waiting
                        if not self._exception_queue.empty():
                            exc = await self._exception_queue.get()
                            await self._pool.shutdown()
                            raise exc
                        # Wait for at least one task to complete
                        await self._pool.wait(timeout=self._queue_wait_timeout)
                        continue
                    else:
                        # No tasks and no events - check for deadlock
                        self._check_for_deadlock(ctx)
                        break

                # Check for StopEvent
                if isinstance(event, StopEvent):
                    result = event.result
                    break

                # Check buffer size periodically
                self._check_buffer_size()

                # Dispatch event processing (non-blocking)
                await self._dispatch_event(event, ctx)

            # Wait for all remaining tasks to complete before returning
            # TODO: Should add parameter for graceful_shutdown_timeout?
            # if self._pool.is_active:
            #     await self._pool.wait(return_when="all_completed", timeout=10.0)

            # Final check for task exceptions
            if not self._exception_queue.empty():
                exc = await self._exception_queue.get()
                raise exc

        except WorkflowRuntimeError:
            # Shutdown remaining tasks and re-raise
            await self._pool.shutdown()
            raise
        except Exception as e:
            # Shutdown remaining tasks on unexpected error
            await self._pool.shutdown()
            raise WorkflowRuntimeError(f"Execution failure in workflow: {e!s}.")

        return WorkflowResult(
            run_id=run_id,
            status=WorkflowStatus.COMPLETED,
            result=result,
        )

    async def _dispatch_event(
        self,
        event: Event,
        context: Context,
    ) -> None:
        """
        Dispatch event processing to matching step methods without blocking.

        This method finds all step methods that listen to the event type and
        submits them as independent tasks. Single-event steps are dispatched
        immediately. Fan-in steps buffer events and are dispatched when all
        required events are collected.

        Args:
            event: The Event to dispatch.
            context: The Context for step execution.

        Note:
            Steps execute as independent tasks that emit downstream events
            immediately upon completion, enabling true parallel execution.
        """
        event_type = type(event)

        # Find matching step methods
        matching_steps = self._workflow.get_steps_for_event(event)

        if not matching_steps:
            return

        # Dispatch all matching steps as independent tasks
        for step_name, step_method in matching_steps:
            is_join_step = step_name in self._workflow._join_step_registry

            # Handle join steps with event buffering
            if is_join_step:
                # Check if this join step is already scheduled
                async with self._join_lock:
                    # Buffer the event (whether scheduled or not)
                    try:
                        await self._event_buffer.add_event(step_name, event)
                    except Exception as e:
                        raise WorkflowRuntimeError(
                            f"Failed to buffer event {event_type.__name__} for step '{step_name}': {e!s}"
                        ) from e

                    # If already scheduled, skip further processing
                    if step_name in self._scheduled_join_steps:
                        continue

                    required_events = self._workflow._join_step_registry[step_name]
                    events_dict = await self._event_buffer.get_events(
                        step_name, required_events
                    )

                    # If not all events are present yet, skip this step
                    if events_dict is None:
                        continue

                    # Mark as scheduled to prevent duplicate execution
                    self._scheduled_join_steps.add(step_name)
                    event_or_events = events_dict
            else:
                # Single-event step
                event_or_events = event

            # Get timeout and retry configuration from step metadata
            step_timeout = get_step_timeout(step_method)
            max_retries = get_step_max_retries(step_method)
            retry_delay = get_step_retry_delay(step_method)

            # Submit step as independent task
            task_coro = self._run_step_with_cleanup(
                step_name,
                is_join_step,
                self._step_executor.run(
                    step_name,
                    step_method,
                    context,
                    event_or_events,
                    step_timeout,
                    max_retries,
                    retry_delay,
                    is_join_step=is_join_step,
                ),
            )
            task = await self._pool.create_task(
                task_coro, task_name=f"step:{step_name}"
            )
            # Immediately check if task failed synchronously
            if task.done():
                exc = task.exception()
                if exc:
                    await self._pool.shutdown()
                    raise exc

    async def _run_step_with_cleanup(
        self,
        step_name: str,
        is_join_step: bool,
        coro,
    ) -> None:
        """Wrap a step task coroutine to clean up the join step scheduling marker on completion."""
        try:
            await coro
        finally:
            if is_join_step:
                async with self._join_lock:
                    self._scheduled_join_steps.discard(step_name)

    def _check_buffer_size(self) -> None:
        """
        Check if event buffer size exceeds threshold.

        This method monitors the event buffer to detect potential issues
        like deadlocks or missing events that cause events to accumulate
        without being processed.
        """
        size = self._event_buffer.get_buffer_size()
        if size > self._max_buffer_size:
            pending_steps = self._event_buffer.get_pending_steps()
            raise WorkflowRuntimeError(
                f"Event buffer size ({size}) exceeds maximum ({self._max_buffer_size}). "
                f"This may indicate a deadlock or missing events. "
                f"Steps with pending events: {', '.join(pending_steps)}"
            )

    def _check_for_deadlock(self, ctx: Context) -> None:
        """
        Check if workflow is deadlocked waiting for events.

        A deadlock occurs when:
        - Event queue is empty
        - Event buffer has pending events
        - No steps can execute

        This indicates that join steps are waiting for events that
        will never arrive, preventing workflow completion.

        Args:
            ctx: The workflow context
        """
        if ctx._event_queue.empty():
            buffer_size = self._event_buffer.get_buffer_size()
            if buffer_size > 0:
                # Get details of pending events for error message
                pending_steps = self._event_buffer.get_pending_steps()

                # Build detailed error message with step status
                details = []
                for step_name in pending_steps:
                    status = self._event_buffer.get_step_status(step_name)
                    required_events = self._workflow._join_step_registry.get(
                        step_name, set()
                    )
                    required_names = [et.__name__ for et in required_events]
                    received_names = list(status.keys())
                    missing_names = [
                        name for name in required_names if name not in received_names
                    ]

                    details.append(
                        f"  - '{step_name}': received {received_names}, "
                        f"missing {missing_names}"
                    )

                raise WorkflowRuntimeError(
                    f"Workflow deadlock detected: {buffer_size} events buffered "
                    f"but no steps can execute. Check for missing event emissions.\n"
                    f"Pending steps:\n" + "\n".join(details)
                )
