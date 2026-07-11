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
from donkey_workflows.events import Event
from donkey_workflows.exceptions import WorkflowRuntimeError, WorkflowTimeoutError
from donkey_workflows.runtime.event_buffer import EventBuffer
from donkey_workflows.runtime.execution_pool import ExecutionPool

if TYPE_CHECKING:
    from donkey_workflows import Workflow

_dispatcher = get_dispatcher(__name__)


class StepExecutor:
    """Private class to encapsulate execution of individual step."""

    def __init__(
        self,
        exception_queue: asyncio.Queue[Exception],
        event_buffer: EventBuffer,
        pool: ExecutionPool,
        workflow: "Workflow",
    ) -> None:
        self._exception_queue = exception_queue
        self._event_buffer = event_buffer
        self._pool = pool
        self._workflow = workflow

    async def run(
        self,
        step_name: str,
        step_method: Any,
        context: Context,
        event: Event | dict[type, Event],
        timeout: float | None,
        max_retries: int,
        retry_delay: float,
        is_join_step: bool = False,
    ) -> None:
        """
        Execute a step as an independent task with immediate event emission.

        Args:
            step_name: Name of the step.
            step_method: The step method to execute.
            context: The Context for step execution.
            event: Single Event or dict of event types to Event instances.
            timeout: Optional timeout in seconds for step execution.
            max_retries: Number of retry attempts on failure.
            retry_delay: Delay in seconds between retry attempts.
            is_join_step: Whether this is a join step.
        """
        try:
            result = await self._step_worker(
                step_name=step_name,
                step_method=step_method,
                context=context,
                event=event,
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
            if is_join_step:
                await self._event_buffer.clear_events(step_name)

            await self._exception_queue.put(e)

    async def _step_worker(
        self,
        step_name: str,
        step_method: Any,
        context: Context,
        event: Event | dict[type, Event],
        timeout: float | None,
        max_retries: int,
        retry_delay: float,
    ) -> Event | None:
        """
        Execute a step with timeout, applying retry decorator dynamically.

        Applies retry logic based on the step's configuration and instruments
        each execution with observability spans.
        """
        bound_args = inspect.BoundArguments(
            signature=inspect.Signature(parameters=[]),
            arguments=OrderedDict(
                [
                    ("ctx", context),
                    ("ev", event),
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
            instance=self._workflow,
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
                            step_method(self._workflow, context, event)
                        ),
                        timeout=timeout,
                    )
                else:
                    return await self._pool.run_coroutine(
                        step_method(self._workflow, context, event)
                    )

            result = await execute()  # type: ignore[misc]

            _dispatcher.span_end(
                id_=span_id,
                bound_args=bound_args,
                instance=self._workflow,
                result=result,
            )

            return result

        except Exception as e:
            _dispatcher.span_exception(
                id_=span_id,
                bound_args=bound_args,  # type: ignore
                instance=self._workflow,
                err=e,
            )
            raise
        finally:
            active_span_id.reset(span_token)
