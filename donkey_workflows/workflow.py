import inspect
import json
import warnings
from typing import Any, Type, get_args, get_origin

from donkey_workflows.context import Context
from donkey_workflows.context.state_store import DictState
from donkey_workflows.decorators import (
    get_step_event_types,
    get_step_max_retries,
    get_step_name,
    get_step_retry_delay,
    get_step_timeout,
    is_join_step,
    is_step_method,
)
from donkey_workflows.events import Event, StartEvent, StopEvent
from donkey_workflows.exceptions import (
    WorkflowValidationError,
)
from donkey_workflows.runtime.engine import WorkflowEngine


class Workflow:
    """
    Event-driven workflow orchestration.

    Workflows are defined by decorating methods with @step(when=EventClass).
    Steps are automatically discovered and registered when the class is defined.

    State type consistency is enforced: all steps must use the same Context[StateType].

    Example:
        ```python
        class MyWorkflow(Workflow):
            @step(when=StartEvent)
            async def start(self, ctx: Context, ev: StartEvent) -> MyEvent:
                return MyEvent(data="processed")

            @step(when=MyEvent)
            async def process(self, ctx: Context, ev: MyEvent) -> StopEvent:
                return StopEvent(result="done")


        workflow = MyWorkflow()
        result = await workflow.run(input_msg="Hello, World!")
        ```
    """

    def __init_subclass__(cls, **kwargs):
        """
        Automatically discover and register @step decorated methods.

        This is called when a subclass is defined, allowing automatic
        step discovery and state type validation.
        """
        super().__init_subclass__(**kwargs)

        cls._step_registry: dict[Type[Event], list[tuple[str, Any]]] = {}
        cls._join_step_registry: dict[str, set[Type[Event]]] = {}

        # Track state types across steps for consistency validation
        state_types: dict[str, Type] = {}

        # Discover all @step decorated methods
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if is_step_method(method):
                event_types = get_step_event_types(method)
                if event_types:
                    if is_join_step(method):
                        cls._join_step_registry[name] = set(event_types)

                    # Add to step_registry for each event type
                    for single_event_type in event_types:
                        if single_event_type not in cls._step_registry:
                            cls._step_registry[single_event_type] = []
                        cls._step_registry[single_event_type].append((name, method))

                    # Extract and validate Context state type
                    # Parameter name is enforced to be 'ctx' by decorator validation
                    sig = inspect.signature(method)
                    ctx_param = sig.parameters.get("ctx")
                    if ctx_param and ctx_param.annotation != inspect.Parameter.empty:
                        origin = get_origin(ctx_param.annotation)
                        if origin is Context:
                            args = get_args(ctx_param.annotation)
                            if args:
                                # Context[StateType]
                                state_type = args[0]
                            else:
                                # Context without type - defaults to DictState
                                state_type = DictState

                            state_types[name] = state_type

        # Validate state type consistency across all steps
        if state_types:
            unique_types = set(state_types.values())
            if len(unique_types) > 1:
                type_details = "\n".join(
                    f"  - {step_name}: Context[{state_type.__name__}]"
                    for step_name, state_type in state_types.items()
                )
                raise WorkflowValidationError(
                    f"Inconsistent state types in workflow '{cls.__name__}'.\n"
                    f"All steps must use the same Context[StateType].\n"
                    f"Found:\n{type_details}"
                )

            # Store the validated state type for the workflow
            cls._state_type = next(iter(unique_types))
        else:
            cls._state_type = DictState

        cls._validate_single_start_step()
        cls._warn_multiple_stop_events()

        # Detect circular dependencies in join steps
        cls._detect_circular_dependencies()

    @classmethod
    def _extract_produced_events(cls, method) -> set[Type[Event]]:
        """Extract event types produced by a step method from its return annotation."""
        produced_events: set[Type[Event]] = set()
        sig = inspect.signature(method)
        return_annotation = sig.return_annotation

        if return_annotation != inspect.Parameter.empty:
            origin = get_origin(return_annotation)
            if origin is not None:
                # Handle Union types (Event | None)
                args = get_args(return_annotation)
                for arg in args:
                    if isinstance(arg, type) and issubclass(arg, Event):
                        produced_events.add(arg)
            elif isinstance(return_annotation, type) and issubclass(
                return_annotation, Event
            ):
                # Direct Event type
                produced_events.add(return_annotation)

        return produced_events

    @classmethod
    def _build_event_producers_map(cls) -> dict[Type[Event], list[str]]:
        """Build a map of event types to the step names that produce them."""
        event_producers: dict[Type[Event], list[str]] = {}

        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if is_step_method(method):
                produced_events = cls._extract_produced_events(method)
                for event_type in produced_events:
                    if event_type not in event_producers:
                        event_producers[event_type] = []
                    event_producers[event_type].append(name)

        return event_producers

    @classmethod
    def _detect_circular_dependencies(cls) -> None:
        """
        Detect circular dependencies in join steps.

        This method builds a dependency graph and checks for cycles where
        a join step depends on events that can only be produced by
        steps that depend on this step's output.
        """
        if not cls._join_step_registry:
            return

        # Build event producers map
        event_producers = cls._build_event_producers_map()

        # Check each join step for circular dependencies
        for step_name, required_events in cls._join_step_registry.items():
            step_method = getattr(cls, step_name)
            produced_events = cls._extract_produced_events(step_method)

            # Check if any required event creates a circular dependency
            for required_event in required_events:
                if required_event not in event_producers:
                    continue

                producers = event_producers[required_event]

                # Check if any producer is a join step that requires this step's output
                for producer_name in producers:
                    if producer_name in cls._join_step_registry:
                        producer_required = cls._join_step_registry[producer_name]

                        if produced_events & producer_required:
                            raise WorkflowValidationError(
                                f"Circular dependency detected: Step '{step_name}' requires "
                                f"{required_event.__name__} which is produced by '{producer_name}', "
                                f"but '{producer_name}' requires events from '{step_name}'. "
                                f"This creates a deadlock where neither step can execute."
                            )

    @classmethod
    def _validate_single_start_step(cls) -> None:
        """
        Validate that workflow has exactly one StartEvent handler.
        """
        start_event_handlers = cls._step_registry.get(StartEvent, [])

        if len(start_event_handlers) > 1:
            handler_names = [name for name, _ in start_event_handlers]
            raise WorkflowValidationError(
                f"Workflow '{cls.__name__}' must have exactly one StartEvent handler. "
                f"Found {len(start_event_handlers)}: {', '.join(handler_names)}"
            )

    @classmethod
    def _warn_multiple_stop_events(cls) -> None:
        """
        Warn if multiple steps can produce StopEvent.

        Multiple StopEvent producers can cause race conditions where the first
        step to complete determines the workflow result, leading to non-deterministic behavior.
        """
        event_producers = cls._build_event_producers_map()
        stop_event_producers = event_producers.get(StopEvent, [])

        if len(stop_event_producers) > 1:
            warnings.warn(
                f"Workflow '{cls.__name__}' has {len(stop_event_producers)} steps that produce StopEvent: "
                f"{', '.join(stop_event_producers)}. This may cause race conditions.",
                UserWarning,
                stacklevel=2,
            )

    @classmethod
    def export(cls, path: str | None = None) -> dict:
        """
        Export the workflow definition to a portable JSON-compatible dict.

        Captures structure, step metadata, event schemas, and source code —
        enough for documentation, auditing, or future reimport via from_export().

        Args:
            path: Optional file path to write the JSON (e.g. "workflow.json").

        Returns:
            dict with the complete workflow manifest.
        """
        from donkey_workflows.serialization.export import (
            EventExport,
            EventFieldExport,
            StepExport,
            WorkflowExport,
        )

        step_methods = [
            (name, method)
            for name, method in inspect.getmembers(cls, predicate=inspect.isfunction)
            if is_step_method(method)
        ]

        steps: list[StepExport] = []
        all_event_types: set[Type[Event]] = set()

        for name, method in step_methods:
            triggers = get_step_event_types(method) or []
            produces = cls._extract_produced_events(method)

            all_event_types.update(triggers)
            all_event_types.update(produces)

            try:
                code = inspect.getsource(method)
            except OSError:
                code = None

            steps.append(
                StepExport(
                    name=get_step_name(method) or name,
                    triggers=[e.__name__ for e in triggers],
                    produces=sorted(e.__name__ for e in produces),
                    is_join=is_join_step(method),
                    timeout=get_step_timeout(method),
                    max_retries=get_step_max_retries(method),
                    retry_delay=get_step_retry_delay(method),
                    code=code,
                )
            )

        events: dict[str, EventExport] = {}
        for evt_cls in sorted(all_event_types, key=lambda e: e.__name__):
            try:
                evt_code = inspect.getsource(evt_cls)
            except OSError:
                evt_code = None

            fields: dict[str, EventFieldExport] = {}
            for field_name, field_info in evt_cls.model_fields.items():
                annotation = field_info.annotation
                type_str = (
                    annotation.__name__
                    if hasattr(annotation, "__name__")
                    else str(annotation)
                )
                fields[field_name] = EventFieldExport(
                    type=type_str,
                    required=field_info.is_required(),
                )

            events[evt_cls.__name__] = EventExport(
                code=evt_code,
                fields=fields,
            )

        state_type = cls._state_type
        try:
            state_code = inspect.getsource(state_type)
        except OSError:
            state_code = None

        cls_module = inspect.getmodule(cls)
        try:
            cls_code = inspect.getsource(cls)
        except OSError:
            cls_code = None

        manifest = WorkflowExport(
            name=cls.__name__,
            module=cls_module.__name__ if cls_module else None,
            description=(cls.__doc__ or "").strip(),
            state_type=state_type.__name__,
            state_code=state_code,
            code=cls_code,
            steps=steps,
            events=events,
        )

        if path is not None:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(manifest.model_dump(), f, indent=2)

        return manifest.model_dump()

    def get_steps_for_event(self, event: Event) -> list[tuple[str, Any]]:
        """Get all step methods that handle the given event type."""
        event_class = type(event)
        return self._step_registry.get(event_class, [])

    async def run(
        self,
        ctx: Context | None = None,
        start_event: StartEvent | None = None,
        **kwargs,
    ) -> Any:
        """
        Run the workflow and return results.

        Args:
            ctx: Optional context to use. If None, creates new context.
            start_event: Optional explicit start event. If None, creates StartEvent(**kwargs).
            **kwargs: Additional arguments. If start_event is None, used to create StartEvent.
                     If start_event is provided, kwargs are added as attributes to the event.

        Examples:
            # Using default StartEvent with kwargs
            result = await workflow.run(input_msg="hello", user_id=123)

            # Using custom start event
            custom_event = StartEvent(data="important")
            result = await workflow.run(start_event=custom_event)
        """
        # Create or merge start_event
        if start_event is None:
            start_event = StartEvent(**kwargs)
        elif kwargs:
            # Warn about merging kwargs into start_event
            warnings.warn(
                "Merging **kwargs into StartEvent. "
                "These will overwrite any existing attributes with the same name/key.",
                UserWarning,
                stacklevel=2,
            )

            # Merge kwargs into start_event
            for key, value in kwargs.items():
                setattr(start_event, key, value)

        # Create engine and execute
        runtime_engine = WorkflowEngine(workflow=self)

        return await runtime_engine.run(start_events=start_event, ctx=ctx)
