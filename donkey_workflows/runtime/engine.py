import asyncio
import inspect
import uuid
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from donkey_instrumentation import get_dispatcher
from donkey_instrumentation.span import active_span_id
from donkey_toolkit.retry import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

from donkey_workflows.context import Context
from donkey_workflows.decorators import (
    get_step_max_retries,
    get_step_retry_delay,
    get_step_timeout,
)
from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.exceptions import (
    WorkflowRuntimeError,
    WorkflowTimeoutError,
    WorkflowValidationError,
)
from donkey_workflows.runtime.event_buffer import EventBuffer
from donkey_workflows.runtime.execution_pool import ExecutionPool
from donkey_workflows.schemas import WorkflowResult, WorkflowStatus

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
            return await self._control_loop_run(start_events, ctx)

        _run.__name__ = "run"
        _run.__qualname__ = "Workflow.run"

        return await _run()

    async def _control_loop_run(
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
        immediately. Join steps buffer events and are dispatched when all
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
            task_coro = self._run_step_task(
                step_name,
                step_method,
                self._workflow,
                context,
                event_or_events,
                step_timeout,
                max_retries,
                retry_delay,
                is_join_step=is_join_step,
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

    async def _run_step_task(
        self,
        step_name: str,
        step_method: Any,
        workflow: Any,
        context: Context,
        event_or_events: Event | dict[type, Event],
        timeout: float | None,
        max_retries: int,
        retry_delay: float,
        is_join_step: bool = False,
    ) -> None:
        """
        Execute a step as an independent task with immediate event emission.

        This method wraps step execution with retry/timeout logic and emits
        downstream events immediately upon completion, without waiting for
        sibling steps.

        Args:
            step_name: Name of the step.
            step_method: The step method to execute.
            workflow: The workflow instance.
            context: The Context for step execution.
            event_or_events: Single Event or dict of event types to Event instances.
            timeout: Optional timeout in seconds for step execution.
            max_retries: Number of retry attempts on failure.
            retry_delay: Delay in seconds between retry attempts.
            is_join_step: Whether this is a join step.
        """
        try:
            result = await self._step_worker(
                step_name=step_name,
                step_method=step_method,
                workflow=workflow,
                context=context,
                event_or_events=event_or_events,
                timeout=timeout,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

            # Clear buffer after successful join step execution
            if is_join_step:
                await self._event_buffer.clear_events(step_name)

            # Emit downstream immediately if step returned event
            if result is not None:
                if isinstance(result, Event):
                    await context.send_event(result)
                else:
                    raise WorkflowRuntimeError(
                        f"Step '{step_name}' expected Event, got {type(result).__name__}. "
                        "Steps must return a single Event or None."
                    )
        except asyncio.TimeoutError as e:
            if is_join_step:
                # Clear buffer on failure for join steps
                await self._event_buffer.clear_events(step_name)

                required_events = self._workflow._join_step_registry.get(
                    step_name, set()
                )
                event_names = [et.__name__ for et in required_events]

                error = WorkflowTimeoutError(
                    f"Step '{step_name}' execution timeout after {timeout}s "
                )
            else:
                error = WorkflowTimeoutError(
                    f"Step '{step_name}' execution timeout after {timeout}s"
                )

            await self._exception_queue.put(error)
            raise error from e
        except Exception as e:
            # Clear buffer on failure for join steps
            if is_join_step:
                await self._event_buffer.clear_events(step_name)

            await self._exception_queue.put(e)
        finally:
            # Clean up join step scheduling marker
            if is_join_step:
                async with self._join_lock:
                    self._scheduled_join_steps.discard(step_name)

    async def _step_worker(
        self,
        step_name: str,
        step_method: Any,
        workflow: Any,
        context: Context,
        event_or_events: Event | dict[type, Event],
        timeout: float | None,
        max_retries: int,
        retry_delay: float,
    ) -> Event | None:
        """
        Execute a step with timeout, applying retry decorator dynamically.

        This method applies retry logic based on the step's configuration
        (max_retries and retry_delay). Each step execution is instrumented.
        """
        bound_args = inspect.BoundArguments(
            signature=inspect.Signature(parameters=[]),
            arguments=OrderedDict(
                [
                    ("event_or_events", event_or_events),
                    ("context", context),
                ]
            ),
        )

        cls_name = self._workflow.__class__.__name__
        span_id = f"{cls_name}.{step_name}-{uuid.uuid4()}"
        parent_span_id = active_span_id.get()
        span_token = active_span_id.set(span_id)

        _dispatcher.span_start(
            id_=span_id,
            bound_args=bound_args,
            instance=workflow,
            parent_id=parent_span_id,
        )

        try:

            @retry(
                stop=stop_after_attempt(max_retries + 1),
                wait=wait_fixed(retry_delay),
                when=retry_if_exception(),
                reraise=True,
            )
            async def execute():
                if timeout is not None:
                    return await asyncio.wait_for(
                        self._pool.run_coroutine(
                            step_method(workflow, context, event_or_events)
                        ),
                        timeout=timeout,
                    )
                else:
                    return await self._pool.run_coroutine(
                        step_method(workflow, context, event_or_events)
                    )

            result = await execute()  # type: ignore[misc]

            _dispatcher.span_end(
                id_=span_id,
                bound_args=bound_args,
                instance=workflow,
                result=result,
            )

            return result

        except Exception as e:
            _dispatcher.span_exception(
                id_=span_id,
                bound_args=bound_args,  # type: ignore
                instance=workflow,
                err=e,
            )
            raise
        finally:
            active_span_id.reset(span_token)

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
