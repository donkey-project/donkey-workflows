import asyncio
import platform
import sys
import time
import tracemalloc
from pathlib import Path
from statistics import mean, stdev
from typing import Any

from donkey_workflows import Context, Event, StartEvent, StopEvent, Workflow, step

# ============================================================================
# Event Definitions
# ============================================================================


class MessageEvent(Event):
    """Simple message event."""

    message: str


class DataEvent(Event):
    """Event carrying data to be processed."""

    data: list[int]
    size: int


class SumEvent(Event):
    """Event with sum result."""

    result: int


class ProductEvent(Event):
    """Event with product result."""

    result: int


class StatsEvent(Event):
    """Event with statistics result."""

    result: int


# ============================================================================
# Workflow Definitions
# ============================================================================


class O1Workflow(Workflow):
    """Complexity: O(1)."""

    @step(when=StartEvent)
    async def start(self, ctx: Context, ev: StartEvent) -> MessageEvent:
        input_msg = ev.get("input_msg", "")
        return MessageEvent(message=f"Processed: {input_msg}")

    @step(when=MessageEvent)
    async def process(self, ctx: Context, ev: MessageEvent) -> StopEvent:
        return StopEvent(result=ev.message)


class OnWorkflow(Workflow):
    """Complexity: O(n)."""

    @step(when=StartEvent)
    async def initialize(self, ctx: Context, ev: StartEvent) -> DataEvent:
        data = ev.get("data", list(range(100)))

        async with ctx.store.edit_state() as state:
            state["start_time"] = time.time()
            state["data_size"] = len(data)

        return DataEvent(data=data, size=len(data))

    @step(when=DataEvent)
    async def process_sum(self, ctx: Context, ev: DataEvent) -> SumEvent:
        result = sum(ev.data)
        return SumEvent(result=result)

    @step(when=DataEvent)
    async def process_product(self, ctx: Context, ev: DataEvent) -> ProductEvent:
        result = 1
        for num in ev.data:
            result *= num
            if result > 10**15:
                result = result % (10**15)
        return ProductEvent(result=result)

    @step(when=DataEvent)
    async def process_stats(self, ctx: Context, ev: DataEvent) -> StatsEvent:
        result = int(sum(ev.data) / len(ev.data)) if ev.data else 0
        return StatsEvent(result=result)

    @step(when=[SumEvent, ProductEvent, StatsEvent])
    async def aggregate(self, ctx: Context, events: dict[type, Event]) -> StopEvent:
        sum_ev: SumEvent = events[SumEvent]  # type: ignore
        product_ev: ProductEvent = events[ProductEvent]  # type: ignore
        stats_ev: StatsEvent = events[StatsEvent]  # type: ignore

        async with ctx.store.edit_state() as state:
            start_time = state.get("start_time", 0)
            data_size = state.get("data_size", 0)

        duration = time.time() - start_time

        return StopEvent(
            result={
                "results": {
                    "sum": sum_ev.result,
                    "product": product_ev.result,
                    "average": stats_ev.result,
                },
                "data_size": data_size,
                "duration": duration,
            }
        )


# ============================================================================
# Benchmark Functions
# ============================================================================


def get_environment_info() -> dict[str, Any]:
    """Get system and environment information."""
    try:
        import psutil

        cpu_freq = psutil.cpu_freq()
        cpu_freq_str = f"{cpu_freq.current:.0f} MHz" if cpu_freq else "N/A"
        memory = psutil.virtual_memory()
        total_memory_gb = memory.total / (1024**3)
        cpu_count = psutil.cpu_count(logical=False)
        cpu_count_logical = psutil.cpu_count(logical=True)
    except ImportError:
        cpu_freq_str = "N/A"
        total_memory_gb = 0
        cpu_count = "N/A"
        cpu_count_logical = "N/A"

    return {
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "platform": platform.platform(),
        "system": platform.system(),
        "machine": platform.machine(),
        "processor": platform.processor() or "Unknown",
        "cpu_count": cpu_count,
        "cpu_count_logical": cpu_count_logical,
        "cpu_freq": cpu_freq_str,
        "total_memory_gb": f"{total_memory_gb:.2f} GB"
        if total_memory_gb > 0
        else "N/A",
    }


async def benchmark_scalability() -> dict[str, Any]:
    """
    Benchmark scalability with different workflow complexities and data sizes
    Tests both O(1) and O(n) workflows.
    """
    print("\n📈 Benchmarking Scalability...")

    results = {"o1_simple": [], "on_variable": []}

    print("\n  O(1) Workflow:")
    workflow = O1Workflow()
    iterations = 100

    # Warm-up
    await workflow.run(input_msg="warmup")

    tracemalloc.start()
    times = []
    start = time.perf_counter()

    for i in range(iterations):
        iter_start = time.perf_counter()
        await workflow.run(input_msg=f"Test {i}")
        times.append(time.perf_counter() - iter_start)

    total_time = time.perf_counter() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    results["o1_simple"] = {  # type: ignore
        "complexity": "O(1)",
        "iterations": iterations,
        "avg_latency": mean(times),
        "min_latency": min(times),
        "max_latency": max(times),
        "std_latency": stdev(times) if len(times) > 1 else 0,
        "throughput": iterations / total_time,
        "peak_memory_mb": peak / 1024 / 1024,
    }

    print(f"    ✓ {mean(times) * 1000:.2f}ms avg, {iterations / total_time:.2f} wf/s")

    print("\n  O(n) Workflow (variable data sizes):")
    data_sizes = [10, 100, 1000, 10000]

    for size in data_sizes:
        print(f"    Size {size:,}...", end=" ")
        workflow = OnWorkflow()
        data = list(range(size))

        # Warm-up
        await workflow.run(data=data[:10])

        iterations = 20
        tracemalloc.start()
        times = []
        start = time.perf_counter()

        for _ in range(iterations):
            iter_start = time.perf_counter()
            await workflow.run(data=data)
            times.append(time.perf_counter() - iter_start)

        total_time = time.perf_counter() - start
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        results["on_variable"].append(
            {
                "size": size,
                "complexity": "O(n)",
                "iterations": iterations,
                "avg_latency": mean(times),
                "min_latency": min(times),
                "max_latency": max(times),
                "throughput": iterations / total_time,
                "peak_memory_mb": peak / 1024 / 1024,
            }
        )

        print(f"✓ {mean(times) * 1000:.2f}ms avg")

    return results


async def benchmark_concurrency() -> dict[str, Any]:
    """
    Benchmark concurrency with different workflow complexities
    Tests both O(1) and O(n) workflows under concurrent load.
    """
    print("\n🔄 Benchmarking Concurrency...")

    results = {"o1_simple": [], "on_complex": []}

    levels = [1, 10, 50, 100]

    print("\n  O(1) Workflow:")
    for level in levels:
        print(f"    {level} concurrent...", end=" ")

        iterations = 3
        tracemalloc.start()
        times = []

        for _ in range(iterations):
            workflows = [O1Workflow() for _ in range(level)]

            iter_start = time.perf_counter()
            await asyncio.gather(
                *[wf.run(input_msg=f"Test {i}") for i, wf in enumerate(workflows)]
            )
            times.append(time.perf_counter() - iter_start)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        avg_time = mean(times)

        results["o1_simple"].append(
            {
                "complexity": "O(1)",
                "concurrency": level,
                "iterations": iterations,
                "total_time": avg_time,
                "avg_latency_per_workflow": avg_time / level,
                "throughput": level / avg_time,
                "peak_memory_mb": peak / 1024 / 1024,
            }
        )

        print(f"✓ {level / avg_time:.2f} wf/s")

    print("\n  O(n) Workflow:")
    data = list(range(100))

    for level in levels:
        print(f"    {level} concurrent...", end=" ")

        iterations = 3
        tracemalloc.start()
        times = []

        for _ in range(iterations):
            workflows = [OnWorkflow() for _ in range(level)]

            iter_start = time.perf_counter()
            await asyncio.gather(*[wf.run(data=data) for wf in workflows])
            times.append(time.perf_counter() - iter_start)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        avg_time = mean(times)

        results["on_complex"].append(
            {
                "complexity": "O(n)",
                "concurrency": level,
                "iterations": iterations,
                "total_time": avg_time,
                "avg_latency_per_workflow": avg_time / level,
                "throughput": level / avg_time,
                "peak_memory_mb": peak / 1024 / 1024,
            }
        )

        print(f"✓ {level / avg_time:.2f} wf/s")

    return results


# ============================================================================
# Report Generation
# ============================================================================


def generate_markdown_report(
    scalability_result: dict[str, Any],
    concurrency_result: dict[str, Any],
    env_info: dict[str, Any],
) -> None:
    """Generate comprehensive markdown report."""
    output_file = Path(__file__).parent / "benchmark_report.md"

    with open(output_file, "w") as f:
        f.write("# 🫏 Donkey Workflows - Performance Benchmark Report\n\n")

        # Environment
        f.write("## 🧪 Environment\n\n")
        f.write(f"- **Python Version**: {env_info['python_version']}\n")
        f.write(f"- **Platform**: {env_info['platform']}\n")
        f.write(f"- **System**: {env_info['system']}\n")
        f.write(f"- **Machine**: {env_info['machine']}\n")
        f.write(f"- **Processor**: {env_info['processor']}\n")
        f.write(
            f"- **CPU Cores**: {env_info['cpu_count']} physical, {env_info['cpu_count_logical']} logical\n"
        )
        f.write(f"- **CPU Frequency**: {env_info['cpu_freq']}\n")
        f.write(f"- **Total Memory**: {env_info['total_memory_gb']}\n\n")

        # Scalability Results
        f.write("## 1. 📈 Scalability Benchmark\n\n")
        f.write("How performance scales with workflow complexity and data size.\n\n")

        # O(1)
        o1 = scalability_result["o1_simple"]
        f.write("### O(1) Workflow\n\n")
        f.write("| Metric | Value |\n")
        f.write("|--------|-------|\n")
        f.write(f"| Iterations | {o1['iterations']} |\n")
        f.write(f"| Avg Latency | {o1['avg_latency'] * 1000:.2f} ms |\n")
        f.write(f"| Min Latency | {o1['min_latency'] * 1000:.2f} ms |\n")
        f.write(f"| Max Latency | {o1['max_latency'] * 1000:.2f} ms |\n")
        f.write(f"| Std Deviation | {o1['std_latency'] * 1000:.2f} ms |\n")
        f.write(f"| Throughput | {o1['throughput']:.2f} wf/s |\n")
        f.write(f"| Peak Memory | {o1['peak_memory_mb']:.2f} MB |\n\n")

        # O(n)
        f.write("### O(n) Workflow\n\n")
        f.write("| Data Size | Avg Latency | Throughput | Peak Memory |\n")
        f.write("|-----------|-------------|------------|-------------|\n")
        for r in scalability_result["on_variable"]:
            f.write(
                f"| {r['size']:,} | {r['avg_latency'] * 1000:.2f} ms | "
                f"{r['throughput']:.2f} wf/s | {r['peak_memory_mb']:.2f} MB |\n"
            )
        f.write("\n")

        # Concurrency Results
        f.write("## 2. 🔄 Concurrency Benchmark\n\n")
        f.write("How performance scales with parallel workflow execution.\n\n")

        # O(1) Concurrency
        f.write("### O(1) Workflow\n\n")
        f.write("| Concurrency | Throughput | Avg Latency/WF | Peak Memory |\n")
        f.write("|-------------|------------|----------------|-------------|\n")
        for r in concurrency_result["o1_simple"]:
            f.write(
                f"| {r['concurrency']} | {r['throughput']:.2f} wf/s | "
                f"{r['avg_latency_per_workflow'] * 1000:.2f} ms | {r['peak_memory_mb']:.2f} MB |\n"
            )
        f.write("\n")

        # O(n) Concurrency
        f.write("### O(n) Workflow\n\n")
        f.write("| Concurrency | Throughput | Avg Latency/WF | Peak Memory |\n")
        f.write("|-------------|------------|----------------|-------------|\n")
        for r in concurrency_result["on_complex"]:
            f.write(
                f"| {r['concurrency']} | {r['throughput']:.2f} wf/s | "
                f"{r['avg_latency_per_workflow'] * 1000:.2f} ms | {r['peak_memory_mb']:.2f} MB |\n"
            )

    print(f"\n✅ Report generated: {output_file}")


# ============================================================================
# Main
# ============================================================================


async def main():
    print("=" * 60)
    print("🫏 Donkey Workflows - Performance Benchmark")
    print("=" * 60)

    print("\n📊 Collecting environment information...")
    env_info = get_environment_info()
    print(f"  Python: {env_info['python_version']}")
    print(f"  Platform: {env_info['system']} {env_info['machine']}")
    print(f"  CPU: {env_info['cpu_count']} cores @ {env_info['cpu_freq']}")
    print(f"  Memory: {env_info['total_memory_gb']}")

    # Run benchmarks
    scalability_result = await benchmark_scalability()
    concurrency_result = await benchmark_concurrency()

    generate_markdown_report(scalability_result, concurrency_result, env_info)

    print("\n" + "=" * 60)
    print("✅ All benchmarks completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
