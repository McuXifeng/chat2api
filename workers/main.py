import asyncio
import json
import os
import signal
import sys
import time

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

# Reuse gateway-side modules so the chunk wire format stays identical.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from chatgpt.Driver import (
    Driver, JOBS_DISPATCH, JOBS_CONTROL, JOBS_DLQ, WORKER_GROUP,
    _task_key, _chunks_key,
)
from chatgpt.InstancePool import InstancePool

from workers.browser_slot import BrowserSlot
from workers.ops import chat as op_chat, ping as op_ping, cancel as op_cancel


WORKER_ID = os.getenv("WORKER_ID", os.getenv("HOSTNAME", f"worker-{os.getpid()}"))
SLOTS_PER_WORKER = int(os.getenv("SLOTS_PER_WORKER", "2"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COOKIE_KEY = os.getenv("COOKIE_ENCRYPTION_KEY")
DISPATCH_STREAM = os.getenv("WORKER_DISPATCH_STREAM", JOBS_DISPATCH)
CONTROL_STREAM = os.getenv("WORKER_CONTROL_STREAM", JOBS_CONTROL)
GROUP = os.getenv("WORKER_DISPATCH_GROUP", WORKER_GROUP)
HEADLESS = os.getenv("BROWSER_HEADLESS", "false").lower() in ("1", "true", "yes")


class Worker:
    def __init__(self):
        self.driver = Driver(redis_url=REDIS_URL,
                             dispatch_stream=DISPATCH_STREAM,
                             control_stream=CONTROL_STREAM,
                             worker_group=GROUP)
        self.pool = InstancePool(redis_url=REDIS_URL, gateway_id=WORKER_ID,
                                 cookie_key=COOKIE_KEY)
        self.slots: dict[str, BrowserSlot] = {}
        self._slot_lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._active_tasks: dict[str, asyncio.Task] = {}

    async def get_slot(self, instance_id: str) -> BrowserSlot:
        async with self._slot_lock:
            slot = self.slots.get(instance_id)
            if slot is None:
                cookies = await self.pool.get_cookies(instance_id)
                r = await self.pool.connect()
                raw = await r.hgetall(f"instance:{instance_id}")
                fingerprint = json.loads(
                    (raw.get(b"fingerprint", b"{}") or b"{}").decode()
                )
                proxy_url = (raw.get(b"proxy_url", b"") or b"").decode()
                slot = BrowserSlot(
                    instance_id=instance_id,
                    cookies=cookies,
                    fingerprint=fingerprint,
                    proxy_url=proxy_url,
                    headless=HEADLESS,
                )
                await slot.start()
                self.slots[instance_id] = slot
            return slot

    async def evict_slot(self, instance_id: str):
        async with self._slot_lock:
            slot = self.slots.pop(instance_id, None)
        if slot:
            await slot.close()

    async def consume_loop(self, consumer_name: str):
        await self.driver.ensure_groups()
        r = await self.driver.connect()
        while not self._stopping.is_set():
            try:
                resp = await r.xreadgroup(
                    self.driver.worker_group, consumer_name,
                    {self.driver.dispatch_stream: ">"},
                    count=4, block=5000,
                )
            except ResponseError as e:
                print(f"[worker] xreadgroup error: {e}", flush=True)
                await asyncio.sleep(2)
                continue
            if not resp:
                continue
            for _stream, entries in resp:
                for entry_id, fields in entries:
                    task_id = (fields.get(b"task_id") or b"").decode()
                    op = (fields.get(b"op") or b"chat").decode()
                    asyncio.create_task(
                        self._handle_one(entry_id, task_id, op)
                    )

    async def _handle_one(self, entry_id, task_id, op):
        r = await self.driver.connect()
        try:
            claimed = await self.driver.claim_task(task_id, WORKER_ID, ttl=120)
            if not claimed:
                await r.xack(self.driver.dispatch_stream,
                             self.driver.worker_group, entry_id)
                return
            task = await self.driver.get_task(task_id)
            payload = json.loads(task.get("payload", "{}"))
            if op == "chat":
                slot = await self.get_slot(payload["instance_id"])
                t = asyncio.create_task(
                    op_chat.run(self.driver, slot, task_id, payload)
                )
                self._active_tasks[task_id] = t
                try:
                    await t
                finally:
                    self._active_tasks.pop(task_id, None)
            elif op == "ping":
                slot = await self.get_slot(payload["instance_id"])
                await op_ping.run(self.driver, slot, task_id, payload)
            else:
                print(f"[worker] unknown op={op}", flush=True)
                await self.driver.append_done(task_id, status="failed",
                                              output={"error": f"unknown op {op}"})
        except Exception as e:
            print(f"[worker] task {task_id} failed: {e}", flush=True)
            try:
                await self.driver.append_done(task_id, status="failed",
                                              output={"error": str(e)})
            except Exception:
                pass
            try:
                await r.xadd(JOBS_DLQ, {"task_id": task_id, "error": str(e)[:200]})
            except Exception:
                pass
        finally:
            try:
                await r.xack(self.driver.dispatch_stream,
                             self.driver.worker_group, entry_id)
            except Exception:
                pass

    async def control_loop(self):
        r = await self.driver.connect()
        last = "$"
        while not self._stopping.is_set():
            try:
                resp = await r.xread({CONTROL_STREAM: last}, block=5000, count=16)
            except Exception as e:
                print(f"[worker] control xread error: {e}", flush=True)
                await asyncio.sleep(2)
                continue
            if not resp:
                continue
            for _stream, entries in resp:
                for eid, fields in entries:
                    last = eid
                    task_id = (fields.get(b"task_id") or b"").decode()
                    cop = (fields.get(b"op") or b"").decode()
                    if cop == "cancel":
                        await op_cancel.run(self, task_id)

    async def renew_loop(self):
        while not self._stopping.is_set():
            for tid in list(self._active_tasks.keys()):
                try:
                    await self.driver.renew_claim(tid, WORKER_ID, ttl=120)
                except Exception:
                    pass
            await asyncio.sleep(30)

    async def shutdown(self):
        self._stopping.set()
        for slot in list(self.slots.values()):
            try:
                await slot.close()
            except Exception:
                pass

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            except NotImplementedError:
                pass

        consumers = [
            self.consume_loop(f"{WORKER_ID}:{i}") for i in range(SLOTS_PER_WORKER)
        ]
        await asyncio.gather(
            *consumers,
            self.control_loop(),
            self.renew_loop(),
        )


async def main():
    print(f"[worker] starting WORKER_ID={WORKER_ID} slots={SLOTS_PER_WORKER}", flush=True)
    w = Worker()
    await w.run()


if __name__ == "__main__":
    asyncio.run(main())
