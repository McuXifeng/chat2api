import asyncio


async def run(driver, slot, task_id: str, payload: dict):
    """Execute a chat job: pipe SSE chunks from in-page fetch into Redis."""
    request_body = payload.get("request") or payload
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)

    def on_chunk(chunk):
        try:
            queue.put_nowait(chunk)
        except asyncio.QueueFull:
            pass

    pumper = asyncio.create_task(_pump(driver, task_id, queue))
    try:
        await slot.run_chat(task_id, request_body, on_chunk)
    finally:
        await queue.put(None)
        try:
            await asyncio.wait_for(pumper, timeout=10)
        except asyncio.TimeoutError:
            pumper.cancel()
        await driver.append_done(task_id, status="done")


async def _pump(driver, task_id: str, queue: asyncio.Queue):
    while True:
        item = await queue.get()
        if item is None:
            return
        await driver.append_chunk(task_id, item)
