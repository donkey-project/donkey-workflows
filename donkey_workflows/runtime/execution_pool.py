import asyncio
from typing import Any, Coroutine, Literal


class ExecutionPool:
    """
    Pool for executing tasks with two-level concurrency control.

    Manages independent task execution, tracking, and cancellation for
    the task-driven workflow runtime with robust resource management.

    Two-levels concurrency control:
    1. task_semaphore: Limits the number of tasks that can be created (in memory)
    2. execution_semaphore: Limits the number of tasks that can execute simultaneously

    This prevents memory exhaustion from creating too many tasks while maintaining
    fine-grained control over actual concurrent executions.
    """

    def __init__(self, max_workers: int = 50, max_tasks: int = 250):
        """
        Initialize the execution pool with two-level concurrency control.

        Args:
            max_workers: Maximum number of concurrent step executions (default: 50)
            max_tasks: Maximum number of tasks that can exist in memory (default: 250)
        """
        self._execution_semaphore = asyncio.Semaphore(max_workers)
        self._task_semaphore = asyncio.Semaphore(max_tasks)
        self._active_tasks: set[asyncio.Task] = set()

    async def create_task(
        self, coro: Coroutine[Any, Any, Any], task_name: str | None = None
    ) -> asyncio.Task:
        """
        Create independent background task with task creation control.

        This method enforces the first level of concurrency control by limiting
        the number of tasks that can be created. If max_tasks is reached, this
        method will block until a task completes and releases a slot.

        Args:
            coro: Coroutine to execute
            task_name: Optional name for the task

        Returns:
            asyncio.Task
        """
        # Control task creation (blocks if max_tasks reached)
        await self._task_semaphore.acquire()

        task = asyncio.create_task(coro, name=task_name)
        self._active_tasks.add(task)
        task.add_done_callback(self._task_done_callback)
        return task

    def _task_done_callback(self, task: asyncio.Task) -> None:
        """
        Remove completed task from tracking set and release task semaphore.
        Called automatically when a task completes.
        """
        self._active_tasks.discard(task)
        self._task_semaphore.release()

    async def run_coroutine(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """
        Run a coroutine with execution concurrency control.

        This method enforces the second level of concurrency control by limiting
        the number of coroutines that can execute simultaneously. The coroutine
        is only executed after acquiring the semaphore, ensuring proper
        concurrency control.

        Args:
            coro: Coroutine to execute.
        """
        async with self._execution_semaphore:
            return await coro

    async def wait(
        self,
        return_when: Literal["first_completed", "all_completed"] = "first_completed",
        timeout: float | None = None,
    ) -> set[asyncio.Task]:
        """
        Wait for active tasks to complete based on the specified strategy.

        This method provides a unified interface for waiting on tasks, similar
        to asyncio.wait() but operating on the pool's active tasks.

        Args:
            return_when: When to return:
                - FIRST_COMPLETED: Return when any task completes (default)
                - ALL_COMPLETED: Return when all tasks complete
            timeout: Optional timeout in seconds
        """
        if not self._active_tasks:
            return set()

        tasks = set(self._active_tasks)

        if return_when == "first_completed":
            done, _ = await asyncio.wait(
                tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            return done
        elif return_when == "all_completed":
            await asyncio.wait(tasks, timeout=timeout)
            return tasks
        else:
            raise ValueError(
                f"Invalid return_when '{return_when!r}'. "
                f"Input should be: ['first_completed', 'all_completed']"
            )

    async def shutdown(self) -> None:
        """
        Shutdown all active tasks and wait for cancellation to complete.
        """
        if not self._active_tasks:
            return

        # Snapshot of active tasks (set copy is atomic in CPython)
        tasks = set(self._active_tasks)

        for task in tasks:
            if not task.done():
                task.cancel()

        # Wait for cancellation to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def num_active_tasks(self) -> int:
        """Number of currently active tasks in the pool."""
        return len(self._active_tasks)

    @property
    def is_active(self) -> bool:
        """Whether the pool is actively processing tasks."""
        return len(self._active_tasks) > 0
