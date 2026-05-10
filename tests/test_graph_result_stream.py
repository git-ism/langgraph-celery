import pytest
from unittest.mock import MagicMock

from langgraph_celery.events import GraphResult, NodeEvent


async def _make_iter(events):
    for e in events:
        yield e


def _chunk(text):
    m = MagicMock()
    m.content = text
    return m


async def test_from_stream_tokens():
    events = [
        ("on_chat_model_stream", "model", {"chunk": _chunk("Hello ")}),
        ("on_chat_model_stream", "model", {"chunk": _chunk("world")}),
        ("on_chain_end", "LangGraph", {"output": {}}),
    ]
    result = await GraphResult.from_stream(_make_iter(events))
    assert result.full_answer == "Hello world"


async def test_from_stream_tool_events():
    events = [
        ("on_tool_start", "search", {"input": {"query": "foo"}}),
        ("on_tool_end", "search", {"output": "bar", "input": {"query": "foo"}}),
        ("on_chain_end", "LangGraph", {"output": {}}),
    ]
    result = await GraphResult.from_stream(_make_iter(events))
    assert len(result.node_events) == 2
    assert result.node_events[0].kind == "on_tool_start"
    assert result.node_events[1].kind == "on_tool_end"
    assert result.tool_calls[0]["name"] == "search"


async def test_from_stream_prefers_chain_end_output():
    from langchain_core.messages import AIMessage

    msg = AIMessage(content="From output")
    events = [
        ("on_chat_model_stream", "model", {"chunk": _chunk("From stream")}),
        ("on_chain_end", "LangGraph", {"output": {"messages": [msg]}}),
    ]
    result = await GraphResult.from_stream(_make_iter(events))
    assert result.full_answer == "From output"
