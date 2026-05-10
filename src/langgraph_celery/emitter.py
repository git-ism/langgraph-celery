from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langgraph_celery.bridge import stream_events
from langgraph_celery.events import GraphResult, NodeEvent


class NodeEventEmitter:
    def __init__(self, callback: Callable[[NodeEvent], None]) -> None:
        self._callback = callback

    async def run(self, graph: Any, input: dict, config: dict | None = None) -> GraphResult:
        result = await GraphResult.from_stream(stream_events(graph, input, config))
        for event in result.node_events:
            self._callback(event)
        return result
