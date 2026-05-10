from __future__ import annotations

import json
from typing import Any

from langgraph_celery.bridge import stream_events
from langgraph_celery.events import GraphResult


class RedisStreamer:
    def __init__(self, redis_url: str, channel: str) -> None:
        self._redis_url = redis_url
        self._channel = channel

    def _resolve_channel(self, kwargs: dict) -> str:
        try:
            return self._channel.format(**kwargs)
        except KeyError:
            return self._channel

    async def run(
        self,
        graph: Any,
        input: dict,
        config: dict | None = None,
        *,
        task_kwargs: dict | None = None,
    ) -> GraphResult:
        import redis.asyncio as aioredis

        channel = self._resolve_channel(task_kwargs or {})
        client = aioredis.from_url(self._redis_url)

        try:
            result = await GraphResult.from_stream(
                self._publish_stream(client, channel, graph, input, config)
            )
        finally:
            await client.aclose()

        return result

    async def _publish_stream(
        self, client: Any, channel: str, graph: Any, input: dict, config: dict | None
    ):
        async for kind, name, data in stream_events(graph, input, config):
            if kind == "on_chat_model_stream":
                chunk = data.get("chunk")
                if chunk is not None:
                    content = getattr(chunk, "content", None)
                    if isinstance(content, str) and content:
                        msg = json.dumps({"type": "token", "content": content})
                        await client.publish(channel, msg)
                    elif isinstance(content, list):
                        for part in content:
                            if isinstance(part, dict) and part.get("type") == "text":
                                text = part.get("text", "")
                                if text:
                                    msg = json.dumps({"type": "token", "content": text})
                                    await client.publish(channel, msg)
            elif kind == "on_tool_start":
                await client.publish(channel, json.dumps({"type": "tool_start", "name": name}))
            elif kind == "on_tool_end":
                await client.publish(channel, json.dumps({"type": "tool_end", "name": name}))
            elif kind == "on_chain_end" and name == "LangGraph":
                await client.publish(channel, json.dumps({"type": "done"}))
            yield kind, name, data
