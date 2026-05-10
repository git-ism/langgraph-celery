from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any

from langgraph_celery.bridge import invoke, run_sync


def task(
    graph_factory: Callable[..., Any] | None = None,
    *,
    streaming: str | None = None,
    channel: str | None = None,
    checkpointer: str | None = None,
    thread_id_from: str | None = None,
    emit_node_events: bool = False,
    on_node_event: Callable | None = None,
    interrupt_before: list[str] | None = None,
):
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            graph = _resolve_graph(graph_factory, fn, args, kwargs)
            graph_input = _build_input(fn, args, kwargs)
            config = _build_config(checkpointer, thread_id_from, kwargs, graph)

            if streaming == "redis":
                return run_sync(_run_redis(graph, graph_input, config, channel, kwargs))

            if emit_node_events:
                return run_sync(_run_emit(graph, graph_input, config, on_node_event))

            if interrupt_before:
                from langgraph_celery.interrupt import run_with_interrupt

                return run_with_interrupt(graph, graph_input, config)

            return run_sync(invoke(graph, graph_input, config))

        wrapper._langgraph_celery = {
            "graph_factory": graph_factory,
            "streaming": streaming,
            "channel": channel,
            "checkpointer": checkpointer,
            "thread_id_from": thread_id_from,
            "emit_node_events": emit_node_events,
            "interrupt_before": interrupt_before,
        }
        return wrapper

    if callable(graph_factory):
        fn = graph_factory
        graph_factory = None
        return decorator(fn)

    return decorator


def _resolve_graph(graph_factory, fn, args, kwargs):
    if graph_factory is not None:
        sig = inspect.signature(graph_factory)
        params = list(sig.parameters.keys())
        bound_kwargs = {k: v for k, v in kwargs.items() if k in params}
        return graph_factory(**bound_kwargs)
    raise ValueError("graph_factory must be provided via task(graph=...)")


def _build_input(fn, args, kwargs) -> dict:
    sig = inspect.signature(fn)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    return dict(bound.arguments)


def _build_config(checkpointer_name, thread_id_from, kwargs, graph) -> dict:
    config: dict = {}

    if checkpointer_name is not None:
        from langgraph_celery.checkpointing import (
            build_checkpointer,
            inject_thread_id,
            resolve_thread_id,
        )

        cp = build_checkpointer(checkpointer_name)
        try:
            graph.checkpointer = cp
        except Exception:
            pass

        if thread_id_from:
            thread_id = resolve_thread_id(thread_id_from, kwargs)
            config = inject_thread_id(config, thread_id)

    return config


async def _run_redis(graph, graph_input, config, channel, task_kwargs) -> Any:
    from langgraph_celery.streaming.redis import RedisStreamer

    if channel is None:
        raise ValueError("channel must be set when streaming='redis'")

    redis_url = task_kwargs.get("redis_url", "redis://localhost:6379")
    streamer = RedisStreamer(redis_url=redis_url, channel=channel)
    return await streamer.run(graph, graph_input, config, task_kwargs=task_kwargs)


async def _run_emit(graph, graph_input, config, on_node_event) -> Any:
    from langgraph_celery.emitter import NodeEventEmitter

    callback = on_node_event if on_node_event is not None else lambda e: None
    emitter = NodeEventEmitter(callback=callback)
    return await emitter.run(graph, graph_input, config)
