import pytest
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, MessagesState, StateGraph
from langgraph.types import interrupt

from langgraph_celery.interrupt import resume, run_with_interrupt


def _build_graph(mem: MemorySaver):
    def node_a(state):
        return {"messages": [HumanMessage(content="a_done")]}

    def node_b(state):
        answer = interrupt({"question": "approve?"})
        return {"messages": [HumanMessage(content=f"b_got:{answer}")]}

    builder = StateGraph(MessagesState)
    builder.add_node("a", node_a)
    builder.add_node("b", node_b)
    builder.set_entry_point("a")
    builder.add_edge("a", "b")
    builder.add_edge("b", END)
    return builder.compile(checkpointer=mem)


def test_run_with_interrupt_returns_interrupted():
    mem = MemorySaver()
    graph = _build_graph(mem)
    config = {"configurable": {"thread_id": "t1"}}

    result = run_with_interrupt(graph, {"messages": [HumanMessage(content="go")]}, config)

    assert result.interrupted is True
    assert "__interrupt__" in result.output
    assert result.output["__interrupt_values__"] == [{"question": "approve?"}]


def test_resume_completes_graph():
    mem = MemorySaver()
    graph = _build_graph(mem)
    config = {"configurable": {"thread_id": "t2"}}

    run_with_interrupt(graph, {"messages": [HumanMessage(content="go")]}, config)
    result = resume(graph, "yes", config)

    assert result.interrupted is False
    contents = [m.content for m in result.output["messages"]]
    assert "b_got:yes" in contents


def test_resume_with_another_interrupt():
    mem = MemorySaver()

    def node_a(state):
        return {"messages": [HumanMessage(content="a")]}

    def node_b(state):
        interrupt({"q": "first"})
        return {"messages": [HumanMessage(content="b")]}

    def node_c(state):
        interrupt({"q": "second"})
        return {"messages": [HumanMessage(content="c")]}

    builder = StateGraph(MessagesState)
    builder.add_node("a", node_a)
    builder.add_node("b", node_b)
    builder.add_node("c", node_c)
    builder.set_entry_point("a")
    builder.add_edge("a", "b")
    builder.add_edge("b", "c")
    builder.add_edge("c", END)

    mem2 = MemorySaver()
    graph2 = builder.compile(checkpointer=mem2)
    config = {"configurable": {"thread_id": "t3"}}

    r1 = run_with_interrupt(graph2, {"messages": [HumanMessage(content="start")]}, config)
    assert r1.interrupted is True

    r2 = resume(graph2, "ans1", config)
    assert r2.interrupted is True
    assert r2.output["__interrupt_values__"] == [{"q": "second"}]
