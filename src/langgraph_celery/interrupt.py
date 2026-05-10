from __future__ import annotations

from typing import Any

from langgraph_celery.bridge import invoke, run_sync
from langgraph_celery.events import GraphResult


async def _run_with_interrupt(graph: Any, graph_input: Any, config: dict) -> GraphResult:
    output = await invoke(graph, graph_input, config)
    interrupts = output.get("__interrupt__")
    if interrupts:
        interrupt_values = [i.value for i in interrupts]
        result = GraphResult(
            output=output,
            interrupted=True,
        )
        result.output["__interrupt_values__"] = interrupt_values
        return result
    return GraphResult.from_output(output)


async def _resume(graph: Any, resume_value: Any, config: dict) -> GraphResult:
    from langgraph.types import Command

    output = await invoke(graph, Command(resume=resume_value), config)
    interrupts = output.get("__interrupt__")
    if interrupts:
        interrupt_values = [i.value for i in interrupts]
        result = GraphResult(output=output, interrupted=True)
        result.output["__interrupt_values__"] = interrupt_values
        return result
    return GraphResult.from_output(output)


def run_with_interrupt(graph: Any, graph_input: Any, config: dict) -> GraphResult:
    return run_sync(_run_with_interrupt(graph, graph_input, config))


def resume(graph: Any, resume_value: Any, config: dict) -> GraphResult:
    return run_sync(_resume(graph, resume_value, config))
