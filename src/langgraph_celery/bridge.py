from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any


async def invoke(graph: Any, input: dict, config: dict | None = None) -> dict:
    return await graph.ainvoke(input, config=config or {})


async def stream_events(
    graph: Any,
    input: dict,
    config: dict | None = None,
) -> AsyncIterator[tuple[str, str, dict]]:
    async for event in graph.astream_events(input, config=config or {}, version="v2"):
        kind = event.get("event", "")
        name = event.get("name", "")
        data = event.get("data", {})
        yield kind, name, data


def run_sync(coro) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()
