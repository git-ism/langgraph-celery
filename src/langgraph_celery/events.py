from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any


@dataclass
class NodeEvent:
    kind: str
    node: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphResult:
    output: dict[str, Any]
    full_answer: str = ""
    tool_calls: list[dict] = field(default_factory=list)
    node_events: list[NodeEvent] = field(default_factory=list)
    interrupted: bool = False

    @classmethod
    def from_output(cls, output: dict) -> GraphResult:
        from langchain_core.messages import AIMessage

        messages = output.get("messages", [])
        full_answer = ""
        tool_calls = []

        for msg in reversed(messages):
            if isinstance(msg, AIMessage):
                if msg.content and not getattr(msg, "tool_calls", None):
                    full_answer = msg.content
                    break
                if getattr(msg, "tool_calls", None):
                    tool_calls.extend(msg.tool_calls)

        return cls(output=output, full_answer=full_answer, tool_calls=tool_calls)

    @classmethod
    async def from_stream(
        cls,
        event_iter: AsyncIterator[tuple[str, str, dict]],
    ) -> GraphResult:
        tokens: list[str] = []
        tool_calls: list[dict] = []
        node_events: list[NodeEvent] = []
        output: dict[str, Any] = {}

        async for kind, name, data in event_iter:
            if kind == "on_chat_model_stream":
                chunk = data.get("chunk")
                if chunk is not None:
                    content = getattr(chunk, "content", None)
                    if isinstance(content, str) and content:
                        tokens.append(content)
                    elif isinstance(content, list):
                        for part in content:
                            if isinstance(part, dict) and part.get("type") == "text":
                                tokens.append(part.get("text", ""))

            elif kind == "on_tool_start":
                node_events.append(NodeEvent(kind=kind, node=name, data=data))

            elif kind == "on_tool_end":
                node_events.append(NodeEvent(kind=kind, node=name, data=data))
                tool_input = data.get("input") or {}
                tool_output = data.get("output")
                tool_calls.append({"name": name, "input": tool_input, "output": tool_output})

            elif kind == "on_chain_end" and name == "LangGraph":
                output = data.get("output", {})

        if not output and tokens:
            output = {}

        result = cls.from_output(output)
        if tokens and not result.full_answer:
            result.full_answer = "".join(tokens)
        result.node_events = node_events
        if not result.tool_calls:
            result.tool_calls = tool_calls
        return result
