import asyncio
import json

import fakeredis.aioredis
import pytest
from cryptography.fernet import Fernet

from chatgpt.InstancePool import InstancePool, INSTANCE_KEY, POOL_IDLE, POOL_BUSY


def _make_key() -> str:
    return Fernet.generate_key().decode()


@pytest.fixture
async def pool():
    fake = fakeredis.aioredis.FakeRedis(decode_responses=False)
    p = InstancePool(redis_url="redis://unused", gateway_id="gw-test",
                     cookie_key=_make_key(), lease_ttl=600, cooldown_seconds=60)
    p._redis = fake
    yield p
    await fake.aclose()


async def _seed(pool, *, instance_id=None, caps=("gpt-4o",), tier="plus"):
    return await pool.add_instance(
        cookies={"name": "cookie-blob"},
        fingerprint={"ua": "Mozilla/5.0"},
        proxy_url="socks5://proxy:1080",
        account_email="x@example.com",
        tier=tier,
        plan_caps=list(caps),
        instance_id=instance_id,
    )


@pytest.mark.asyncio
async def test_add_then_acquire_then_release(pool):
    instance_id = await _seed(pool)
    lease = await pool.acquire(model_slug="gpt-4o")
    assert lease is not None
    assert lease.instance_id == instance_id
    assert lease.proxy_url == "socks5://proxy:1080"
    assert lease.fingerprint["ua"] == "Mozilla/5.0"
    # busy set should contain it
    busy = await pool._redis.smembers(POOL_BUSY)
    assert instance_id.encode() in busy
    await pool.release(lease, outcome="ok")
    busy = await pool._redis.smembers(POOL_BUSY)
    assert instance_id.encode() not in busy


@pytest.mark.asyncio
async def test_acquire_filters_by_plan_caps(pool):
    free_id = await _seed(pool, caps=("gpt-3.5-turbo",), tier="free")
    pro_id = await _seed(pool, caps=("gpt-5.5-pro",), tier="pro")
    lease = await pool.acquire(model_slug="gpt-5.5-pro")
    assert lease.instance_id == pro_id
    await pool.release(lease)


@pytest.mark.asyncio
async def test_concurrent_acquire_does_not_double_lease(pool):
    instance_id = await _seed(pool)
    leases = await asyncio.gather(*[
        pool.acquire(model_slug="gpt-4o") for _ in range(50)
    ])
    successes = [x for x in leases if x is not None]
    assert len(successes) == 1
    assert successes[0].instance_id == instance_id


@pytest.mark.asyncio
async def test_release_rate_limited_pushes_score_into_future(pool):
    instance_id = await _seed(pool)
    lease = await pool.acquire(model_slug="gpt-4o")
    await pool.release(lease, outcome="rate_limited")
    score = await pool._redis.zscore(POOL_IDLE, instance_id.encode())
    import time
    assert score is not None
    assert score > time.time()


@pytest.mark.asyncio
async def test_release_dead_moves_to_dead_pool(pool):
    instance_id = await _seed(pool)
    lease = await pool.acquire(model_slug="gpt-4o")
    await pool.release(lease, outcome="dead")
    members = await pool._redis.smembers("pool:dead")
    assert instance_id.encode() in members


@pytest.mark.asyncio
async def test_get_cookies_roundtrip(pool):
    instance_id = await pool.add_instance(
        cookies={"session": "abc"},
        fingerprint={},
        plan_caps=["gpt-4o"],
    )
    cookies = await pool.get_cookies(instance_id)
    assert cookies == {"session": "abc"}


@pytest.mark.asyncio
async def test_sticky_caller_returns_same_instance(pool):
    a = await _seed(pool)
    b = await _seed(pool)
    lease1 = await pool.acquire(model_slug="gpt-4o", sticky_caller="user-1")
    await pool.release(lease1)
    lease2 = await pool.acquire(model_slug="gpt-4o", sticky_caller="user-1")
    assert lease2.instance_id == lease1.instance_id


@pytest.mark.asyncio
async def test_limits_storage(pool):
    instance_id = await _seed(pool)
    await pool.set_limit(instance_id, "gpt-4o", 1700000000)
    v = await pool.get_limit(instance_id, "gpt-4o")
    assert v == 1700000000


@pytest.mark.asyncio
async def test_no_capacity_returns_none(pool):
    lease = await pool.acquire(model_slug="gpt-4o")
    assert lease is None
