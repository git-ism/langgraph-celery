import asyncio

import pytest

from langgraph_celery.bridge import run_sync


async def _add(x, y):
    return x + y


def test_run_sync_no_loop():
    result = run_sync(_add(1, 2))
    assert result == 3


def test_run_sync_inside_loop():
    async def inner():
        return run_sync(_add(3, 4))

    result = asyncio.run(inner())
    assert result == 7
