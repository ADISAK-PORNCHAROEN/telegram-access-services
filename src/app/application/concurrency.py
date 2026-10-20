"""Await every child before a caller may close shared files or workspaces."""

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


async def gather_owned(*operations: Coroutine[Any, Any, T]) -> list[T]:
    tasks = [asyncio.create_task(operation) for operation in operations]
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
