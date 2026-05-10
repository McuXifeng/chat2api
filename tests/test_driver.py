import asyncio
import json

import fakeredis.aioredis
import pytest

from chatgpt.Driver import Driver, _task_key, _chunks_key


@pytest.fixture
async def driver(monkeypatch):
    fake = fakeredis.aioredis.FakeRedis(decode_responses=False)
    d = Driver(redis_url="redis://unused")
    d._redis = fake
    await d.ensure_groups()
    yield d
    await fake.aclose()


@pytest.mark.asyncio
async def test_dispatch_writes_task_hash_and_stream(driver):
    task_id = await driver.dispatch({
        "op": "chat", "instance_id": "inst-1", "model": "gpt-4o",
    })
    assert len(task_id) == 32
    task = await driver.get_task(task_id)
    assert task["status"] == "queued"
    assert task["op"] == "chat"
    assert task["instance_id"] == "inst-1"
    assert task["model"] == "gpt-4o"
    assert json.loads(task["payload"])["op"] == "chat"

    r = driver._redis
    entries = await r.xrange(driver.dispatch_stream, count=10)
    assert len(entries) == 1
    eid, fields = entries[0]
    assert fields[b"task_id"] == task_id.encode()
    assert fields[b"op"] == b"chat"


@pytest.mark.asyncio
async def test_iter_chunks_streams_data_until_done(driver):
    task_id = await driver.dispatch({"op": "chat", "instance_id": "i", "model": "m"})

    async def writer():
        await asyncio.sleep(0.05)
        await driver.append_chunk(task_id, b"data: {\"v\":1}\n\n", seq=1)
        await driver.append_chunk(task_id, b"data: {\"v\":2}\n\n", seq=2)
        await driver.append_done(task_id, status="done")

    received = []

    async def reader():
        async for chunk in driver.iter_chunks(task_id, heartbeat_every=0.5):
            received.append(chunk)

    await asyncio.gather(reader(), writer())
    assert b"data: {\"v\":1}\n\n" in received
    assert b"data: {\"v\":2}\n\n" in received


@pytest.mark.asyncio
async def test_iter_chunks_resumes_from_last_id(driver):
    task_id = await driver.dispatch({"op": "chat", "instance_id": "i", "model": "m"})
    await driver.append_chunk(task_id, b"chunk-A", seq=1)
    eid_a = (await driver._redis.xrange(_chunks_key(task_id)))[0][0]
    await driver.append_chunk(task_id, b"chunk-B", seq=2)
    await driver.append_done(task_id)

    received = []
    async for chunk in driver.iter_chunks(
        task_id, last_id=eid_a.decode(), heartbeat_every=0.5
    ):
        received.append(chunk)
    assert b"chunk-A" not in received
    assert b"chunk-B" in received


@pytest.mark.asyncio
async def test_cancel_marks_task_and_writes_control_stream(driver):
    task_id = await driver.dispatch({"op": "chat", "instance_id": "i", "model": "m"})
    await driver.cancel(task_id)
    task = await driver.get_task(task_id)
    assert task["status"] == "cancelling"
    entries = await driver._redis.xrange(driver.control_stream, count=10)
    assert len(entries) == 1
    assert entries[0][1][b"op"] == b"cancel"
    assert entries[0][1][b"task_id"] == task_id.encode()


@pytest.mark.asyncio
async def test_claim_task_is_exclusive(driver):
    task_id = await driver.dispatch({"op": "chat", "instance_id": "i", "model": "m"})
    a = await driver.claim_task(task_id, "worker-1")
    b = await driver.claim_task(task_id, "worker-2")
    assert a is True
    assert b is False
    task = await driver.get_task(task_id)
    assert task["claimed_by"] == "worker-1"


@pytest.mark.asyncio
async def test_cursor_persistence(driver):
    task_id = await driver.dispatch({"op": "chat", "instance_id": "i", "model": "m"})
    await driver.cursor_advance(task_id, "1700000000000-0")
    cur = await driver.cursor_get(task_id)
    assert cur == "1700000000000-0"
