import asyncio
import json
import time
import uuid
from typing import AsyncIterator, Optional

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from utils.Logger import logger


JOBS_DISPATCH = "jobs.dispatch"
JOBS_CONTROL = "jobs.control"
JOBS_DLQ = "jobs.dlq"
WORKER_GROUP = "workers"

TASK_KEY = "tasks:{task_id}"
CURSOR_KEY = "tasks:{task_id}:cursor"
CHUNKS_KEY = "chunks:{task_id}"
TASK_LOCK_KEY = "tasks:{task_id}:lock"


def _task_key(task_id: str) -> str:
    return TASK_KEY.format(task_id=task_id)


def _cursor_key(task_id: str) -> str:
    return CURSOR_KEY.format(task_id=task_id)


def _chunks_key(task_id: str) -> str:
    return CHUNKS_KEY.format(task_id=task_id)


class Driver:
    def __init__(self, redis_url: str,
                 dispatch_stream: str = JOBS_DISPATCH,
                 control_stream: str = JOBS_CONTROL,
                 dlq_stream: str = JOBS_DLQ,
                 worker_group: str = WORKER_GROUP,
                 task_ttl: int = 10800,
                 chunks_maxlen: int = 5000):
        self.redis_url = redis_url
        self.dispatch_stream = dispatch_stream
        self.control_stream = control_stream
        self.dlq_stream = dlq_stream
        self.worker_group = worker_group
        self.task_ttl = task_ttl
        self.chunks_maxlen = chunks_maxlen
        self._redis: Optional[aioredis.Redis] = None

    async def connect(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(
                self.redis_url, decode_responses=False, encoding="utf-8"
            )
        return self._redis

    async def close(self):
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def ensure_groups(self):
        r = await self.connect()
        for stream in (self.dispatch_stream, self.control_stream):
            try:
                await r.xgroup_create(stream, self.worker_group, id="$", mkstream=True)
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    async def dispatch(self, payload: dict, *, ttl: Optional[int] = None) -> str:
        r = await self.connect()
        task_id = uuid.uuid4().hex
        ttl = ttl or self.task_ttl
        op = payload.get("op", "chat")
        instance_id = payload.get("instance_id", "")
        model = payload.get("model", "")
        now = int(time.time())

        task_key = _task_key(task_id)
        chunks_key = _chunks_key(task_id)

        pipe = r.pipeline(transaction=True)
        pipe.hset(task_key, mapping={
            "task_id": task_id,
            "status": "queued",
            "op": op,
            "model": model,
            "instance_id": instance_id,
            "payload": json.dumps(payload, ensure_ascii=False),
            "created_at": now,
            "ttl": ttl,
        })
        pipe.expire(task_key, ttl)
        pipe.expire(chunks_key, ttl)
        pipe.xadd(self.dispatch_stream, {
            "task_id": task_id,
            "op": op,
            "instance_id": instance_id,
        })
        await pipe.execute()
        logger.info(f"Driver.dispatch task_id={task_id} op={op} instance_id={instance_id}")
        return task_id

    async def get_task(self, task_id: str) -> dict:
        r = await self.connect()
        raw = await r.hgetall(_task_key(task_id))
        if not raw:
            return {}
        return {
            (k.decode() if isinstance(k, bytes) else k):
            (v.decode() if isinstance(v, bytes) else v)
            for k, v in raw.items()
        }

    async def cursor_advance(self, task_id: str, last_id: str) -> None:
        r = await self.connect()
        await r.set(_cursor_key(task_id), last_id, ex=self.task_ttl)

    async def cursor_get(self, task_id: str) -> str:
        r = await self.connect()
        v = await r.get(_cursor_key(task_id))
        if not v:
            return "0-0"
        return v.decode() if isinstance(v, bytes) else v

    async def cancel(self, task_id: str) -> None:
        r = await self.connect()
        pipe = r.pipeline(transaction=True)
        pipe.hset(_task_key(task_id), "status", "cancelling")
        pipe.xadd(self.control_stream, {"task_id": task_id, "op": "cancel"})
        await pipe.execute()
        logger.info(f"Driver.cancel task_id={task_id}")

    async def iter_chunks(self, task_id: str, *,
                          last_id: str = "0-0",
                          heartbeat_every: float = 25.0,
                          overall_timeout: Optional[float] = None
                          ) -> AsyncIterator[bytes]:
        r = await self.connect()
        chunks_key = _chunks_key(task_id)
        block_ms = max(1000, int(heartbeat_every * 1000))
        started = time.time()
        cur = last_id
        while True:
            if overall_timeout is not None and (time.time() - started) > overall_timeout:
                logger.info(f"iter_chunks timeout task_id={task_id}")
                return
            try:
                resp = await r.xread({chunks_key: cur}, block=block_ms, count=64)
            except Exception as e:
                logger.error(f"iter_chunks xread failed task_id={task_id}: {e}")
                await asyncio.sleep(1)
                continue

            if not resp:
                yield b": ping\n\n"
                continue

            for _stream, entries in resp:
                for entry_id, fields in entries:
                    eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    cur = eid
                    f = {
                        (k.decode() if isinstance(k, bytes) else k):
                        (v if isinstance(v, bytes) else v.encode() if isinstance(v, str) else v)
                        for k, v in fields.items()
                    }
                    if b"1" == f.get("done", b"0"):
                        await self.cursor_advance(task_id, eid)
                        return
                    if b"1" == f.get("cancelled", b"0"):
                        await self.cursor_advance(task_id, eid)
                        return
                    if b"1" == f.get("heartbeat", b"0"):
                        yield b": ping\n\n"
                        await self.cursor_advance(task_id, eid)
                        continue
                    data = f.get("data")
                    if data is None:
                        continue
                    yield data
                    await self.cursor_advance(task_id, eid)

    async def append_chunk(self, task_id: str, data: bytes, *, seq: Optional[int] = None) -> str:
        r = await self.connect()
        fields = {"data": data}
        if seq is not None:
            fields["seq"] = str(seq)
        eid = await r.xadd(_chunks_key(task_id), fields,
                           maxlen=self.chunks_maxlen, approximate=True)
        return eid.decode() if isinstance(eid, bytes) else str(eid)

    async def append_done(self, task_id: str, *, status: str = "done",
                          output: Optional[dict] = None) -> None:
        r = await self.connect()
        pipe = r.pipeline(transaction=True)
        pipe.xadd(_chunks_key(task_id), {"done": "1"},
                  maxlen=self.chunks_maxlen, approximate=True)
        finish_fields = {"status": status, "finished_at": int(time.time())}
        if output is not None:
            finish_fields["output"] = json.dumps(output, ensure_ascii=False)
        pipe.hset(_task_key(task_id), mapping=finish_fields)
        await pipe.execute()

    async def append_heartbeat(self, task_id: str) -> None:
        r = await self.connect()
        await r.xadd(_chunks_key(task_id), {"heartbeat": "1"},
                     maxlen=self.chunks_maxlen, approximate=True)

    async def claim_task(self, task_id: str, worker_id: str, *, ttl: int = 60) -> bool:
        r = await self.connect()
        ok = await r.set(TASK_LOCK_KEY.format(task_id=task_id),
                         worker_id, nx=True, ex=ttl)
        if ok:
            await r.hset(_task_key(task_id), mapping={
                "status": "running",
                "claimed_by": worker_id,
                "claimed_at": int(time.time()),
            })
        return bool(ok)

    async def renew_claim(self, task_id: str, worker_id: str, *, ttl: int = 60) -> bool:
        r = await self.connect()
        cur = await r.get(TASK_LOCK_KEY.format(task_id=task_id))
        cur_s = cur.decode() if isinstance(cur, bytes) else cur
        if cur_s != worker_id:
            return False
        await r.expire(TASK_LOCK_KEY.format(task_id=task_id), ttl)
        return True

    async def autoclaim_orphans(self, consumer: str, *, min_idle_ms: int = 60000,
                                count: int = 50) -> list:
        r = await self.connect()
        try:
            resp = await r.xautoclaim(
                self.dispatch_stream, self.worker_group, consumer,
                min_idle_time=min_idle_ms, start_id="0-0", count=count
            )
        except Exception as e:
            logger.error(f"autoclaim failed: {e}")
            return []
        return resp[1] if isinstance(resp, (list, tuple)) and len(resp) > 1 else []


_singleton: Optional[Driver] = None


def get_driver() -> Driver:
    global _singleton
    if _singleton is None:
        from utils.configs import (
            redis_url, worker_dispatch_stream, worker_control_stream,
            worker_dispatch_group, task_ttl_seconds,
        )
        _singleton = Driver(
            redis_url=redis_url,
            dispatch_stream=worker_dispatch_stream,
            control_stream=worker_control_stream,
            worker_group=worker_dispatch_group,
            task_ttl=task_ttl_seconds,
        )
    return _singleton
