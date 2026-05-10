from __future__ import annotations

from typing import Any


def build_checkpointer(checkpointer: str) -> Any:
    if checkpointer == "memory":
        from langgraph.checkpoint.memory import MemorySaver
        return MemorySaver()
    if checkpointer == "postgres":
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        return AsyncPostgresSaver
    raise ValueError(f"Unknown checkpointer: {checkpointer!r}. Use 'memory' or 'postgres'.")


def resolve_thread_id(thread_id_from: str, kwargs: dict) -> str:
    value = kwargs.get(thread_id_from)
    if value is None:
        raise KeyError(
            f"thread_id_from={thread_id_from!r} not found in task kwargs: {list(kwargs)}"
        )
    return f"task:{value}"


def inject_thread_id(config: dict, thread_id: str) -> dict:
    config = dict(config)
    configurable = dict(config.get("configurable", {}))
    configurable["thread_id"] = thread_id
    config["configurable"] = configurable
    return config
