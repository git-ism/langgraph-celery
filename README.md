# langgraph-celery

Bridges stateless Celery workers with stateful LangGraph agents. Eliminates the boilerplate of running async graphs inside sync tasks, streaming events to Redis, checkpointing state for retries, and implementing human-in-the-loop flows.

## Installation

```bash
pip install langgraph-celery
```

Optional extras:

```bash
pip install langgraph-celery redis          # for streaming="redis"
pip install langgraph-celery langgraph-checkpoint-postgres  # for checkpointer="postgres"
```

## The problem it solves

Running a LangGraph agent inside a Celery task requires glue code that is easy to get wrong:

| Problem | Naive approach | What this library does |
|---|---|---|
| Async in sync worker | `asyncio.run()` — breaks gevent/eventlet | Thread-safe bridge that detects the running loop |
| Streaming tokens | 50-line `astream_events` parsing loop | `GraphResult.from_stream()` handles it |
| Node status tracking | Write to DB inside graph nodes | `emit_node_events` callback, nodes stay pure |
| Real-time token push | Custom Redis publish code per task | `streaming="redis"` with channel interpolation |
| Crash recovery | Full graph re-run on retry | `checkpointer=` + `thread_id_from=` resumes from last checkpoint |
| Human-in-the-loop | No standard pattern | `interrupt_before=` + `resume()` |

---

## Quick start

```python
import langgraph_celery
from celery import Celery

app = Celery("myapp")

def build_graph(**kwargs):
    # return a compiled LangGraph graph
    ...

@app.task
@langgraph_celery.task(graph=build_graph)
def run_agent(job_id: int, user_input: str):
    pass
```

Calling `run_agent.delay(job_id=1, user_input="hello")` invokes the graph with `{"job_id": 1, "user_input": "hello"}` as input and returns the raw graph output dict.

---

## `@langgraph_celery.task` — all options

```python
@app.task
@langgraph_celery.task(
    graph=build_graph,           # callable that returns a compiled graph
    streaming="redis",           # None (default) | "redis"
    channel="stream:{job_id}",   # Redis channel, {kwarg} interpolation supported
    checkpointer="memory",       # None (default) | "memory" | "postgres"
    thread_id_from="job_id",     # task kwarg to use as stable thread_id
    emit_node_events=True,       # fire on_node_event for each tool start/end
    on_node_event=my_callback,   # callable(NodeEvent) -> None
    interrupt_before=["review"],  # node names to pause before (requires checkpointer)
)
def my_task(job_id: int, ...):
    pass
```

### `graph=`

A callable that returns a compiled LangGraph graph. Called once per task invocation. Receives task kwargs that match its own parameter names.

```python
def build_graph(workspace_id: int):
    # workspace_id is extracted from task kwargs automatically
    llm = get_llm_for_workspace(workspace_id)
    ...
    return graph.compile()

@app.task
@langgraph_celery.task(graph=build_graph)
def run_agent(job_id: int, workspace_id: int):
    pass
```

### `streaming="redis"`

Publishes events to a Redis channel in real time as the graph runs. The task still returns a `GraphResult` when complete.

```python
@app.task
@langgraph_celery.task(
    graph=build_graph,
    streaming="redis",
    channel="myapp:stream:{job_id}",
)
def run_agent(job_id: int, redis_url: str = "redis://localhost:6379"):
    pass
```

`redis_url` is read from task kwargs (key `redis_url`), defaulting to `redis://localhost:6379`.

**Channel interpolation:** any `{kwarg}` in the channel string is replaced with the matching task kwarg value.

**Published message shapes** (JSON strings):

```json
{"type": "token",      "content": "Hello"}
{"type": "tool_start", "name": "search"}
{"type": "tool_end",   "name": "search"}
{"type": "done"}
```

Subscribe example:

```python
import redis, json

r = redis.Redis()
p = r.pubsub()
p.subscribe("myapp:stream:42")

for msg in p.listen():
    if msg["type"] == "message":
        data = json.loads(msg["data"])
        if data["type"] == "token":
            print(data["content"], end="", flush=True)
        elif data["type"] == "done":
            break
```

### `checkpointer=`

Persists graph state between task invocations. Required for HITL and useful for crash recovery (Celery retry → graph resumes from last checkpoint instead of restarting).

```python
@app.task(autoretry_for=(Exception,), max_retries=3)
@langgraph_celery.task(
    graph=build_graph,
    checkpointer="memory",   # or "postgres"
    thread_id_from="job_id",
)
def run_agent(job_id: int):
    pass
```

`thread_id_from="job_id"` generates `thread_id = f"task:{job_id}"` — the same graph run is resumed on retry.

**`checkpointer="memory"`** — in-process `MemorySaver`. Fine for single-worker setups or tests. State is lost when the worker process restarts.

**`checkpointer="postgres"`** — uses `AsyncPostgresSaver` from `langgraph-checkpoint-postgres`. State survives worker restarts. Requires the package:

```bash
pip install langgraph-checkpoint-postgres
```

### `emit_node_events=True`

Fires a callback for every tool start/end transition without modifying graph node code.

```python
def handle_node_event(event: langgraph_celery.NodeEvent):
    print(f"{event.kind} | node={event.node}")
    # event.data contains the raw LangGraph event data

@app.task
@langgraph_celery.task(
    graph=build_graph,
    emit_node_events=True,
    on_node_event=handle_node_event,
)
def run_agent(job_id: int):
    pass
```

`on_node_event` defaults to a no-op if omitted.

### `interrupt_before=`

Pauses graph execution before the named nodes. Returns immediately with `GraphResult(interrupted=True)`. The caller stores the interrupt values, gets human input, then calls `langgraph_celery.resume()`.

**Requires a `checkpointer` — state must persist between the two task calls.**

```python
@app.task
@langgraph_celery.task(
    graph=build_graph,
    checkpointer="memory",
    thread_id_from="job_id",
    interrupt_before=["review_node"],
)
def run_agent(job_id: int):
    pass
```

---

## Human-in-the-loop (HITL) pattern

```python
from langchain_core.messages import HumanMessage
import langgraph_celery

# 1. First invocation — pauses at interrupt node
result = run_agent(job_id=42)

if result.interrupted:
    questions = result.output["__interrupt_values__"]
    # e.g. [{"question": "Should I proceed with deletion?"}]

    # 2. Get human answer (from API endpoint, Slack, etc.)
    human_answer = "yes"

    # 3. Resume — pass the same config the graph was compiled with
    config = {"configurable": {"thread_id": "task:42"}}
    graph = build_graph()
    result = langgraph_celery.resume(graph, human_answer, config)

print(result.full_answer)
```

Chained interrupts (multiple human approval steps) work the same way — each `resume()` call either returns another `GraphResult(interrupted=True)` or a final completed result.

---

## `GraphResult`

Returned by all execution paths.

```python
@dataclass
class GraphResult:
    output: dict[str, Any]        # raw graph output dict
    full_answer: str              # last non-tool-call AIMessage content
    tool_calls: list[dict]        # all tool calls: [{"name", "input", "output"}]
    node_events: list[NodeEvent]  # tool start/end events (when emit_node_events=True or streaming)
    interrupted: bool             # True when graph paused at an interrupt node

    # only present when interrupted=True:
    # result.output["__interrupt_values__"] -> list of interrupt payloads
```

```python
@dataclass
class NodeEvent:
    kind: str          # "on_tool_start" | "on_tool_end"
    node: str          # tool/node name
    data: dict         # raw LangGraph event data
```

---

## Async/sync bridge

`run_sync()` is safe to call from gevent- or eventlet-patched Celery workers. It detects whether an event loop is already running and if so executes the coroutine in a fresh thread rather than calling `asyncio.run()` directly (which would deadlock).

```python
from langgraph_celery.bridge import run_sync

result = run_sync(my_coroutine())
```

---

## `langgraph_celery.resume(graph, value, config)`

Resumes a graph that previously returned `interrupted=True`.

```python
result = langgraph_celery.resume(
    graph,         # compiled graph (must have same checkpointer)
    "approved",    # value passed to interrupt() inside the graph node
    config,        # {"configurable": {"thread_id": "task:42"}}
)
```

Returns a `GraphResult`. If the graph hits another interrupt node, `result.interrupted` is `True` again.

---

## Full example

```python
from celery import Celery
from langchain_core.messages import HumanMessage
from langgraph.graph import END, MessagesState, StateGraph
from langgraph.types import interrupt
import langgraph_celery

app = Celery("example", broker="redis://localhost:6379/0")

def build_graph(**kwargs):
    def agent(state):
        # call your LLM here
        ...

    def human_review(state):
        decision = interrupt({"question": "Approve the agent's plan?"})
        if decision != "yes":
            raise ValueError("Rejected")
        return {}

    builder = StateGraph(MessagesState)
    builder.add_node("agent", agent)
    builder.add_node("human_review", human_review)
    builder.set_entry_point("agent")
    builder.add_edge("agent", "human_review")
    builder.add_edge("human_review", END)
    return builder.compile()

@app.task
@langgraph_celery.task(
    graph=build_graph,
    streaming="redis",
    channel="myapp:stream:{job_id}",
    checkpointer="memory",
    thread_id_from="job_id",
    emit_node_events=True,
    on_node_event=lambda e: print(f"[{e.kind}] {e.node}"),
    interrupt_before=["human_review"],
)
def run_agent(job_id: int, user_input: str):
    pass
```

---

## Dev setup

```bash
git clone https://github.com/git-ism/langgraph-celery
cd langgraph-celery
uv sync
uv run pytest
uv run ruff check src/
```

## Release

Tag a version to publish to PyPI via GitHub Actions:

```bash
git tag v0.1.2
git push origin v0.1.2
```

Requires a [trusted publisher](https://docs.pypi.org/trusted-publishers/) configured on PyPI and a `release` environment on the GitHub repo.
