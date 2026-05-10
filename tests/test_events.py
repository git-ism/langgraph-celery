from langchain_core.messages import AIMessage

from langgraph_celery.events import GraphResult


def test_graph_result_extracts_final_answer():
    msg = AIMessage(content="Hello world")
    result = GraphResult.from_output({"messages": [msg]})
    assert result.full_answer == "Hello world"
    assert not result.interrupted


def test_graph_result_skips_tool_call_messages():
    tool_msg = AIMessage(content="", tool_calls=[{"name": "search", "args": {}, "id": "1"}])
    final_msg = AIMessage(content="Final answer")
    result = GraphResult.from_output({"messages": [tool_msg, final_msg]})
    assert result.full_answer == "Final answer"


def test_graph_result_empty_messages():
    result = GraphResult.from_output({"messages": []})
    assert result.full_answer == ""
