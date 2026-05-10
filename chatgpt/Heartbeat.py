import asyncio
import time
from typing import AsyncIterator

SSE_HEARTBEAT = b": ping\n\n"


async def heartbeat_wrap(source: AsyncIterator[bytes],
                        interval: float = 25.0) -> AsyncIterator[bytes]:
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    sentinel = object()

    async def producer():
        try:
            async for item in source:
                await queue.put(item)
        finally:
            await queue.put(sentinel)

    task = asyncio.create_task(producer())
    last_emit = time.time()
    try:
        while True:
            timeout = max(0.0, interval - (time.time() - last_emit))
            try:
                item = await asyncio.wait_for(queue.get(), timeout=timeout if timeout > 0 else interval)
            except asyncio.TimeoutError:
                yield SSE_HEARTBEAT
                last_emit = time.time()
                continue
            if item is sentinel:
                return
            last_emit = time.time()
            yield item
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
