import json
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Optional

import redis.asyncio as aioredis
from cryptography.fernet import Fernet, InvalidToken

from utils.Logger import logger


POOL_IDLE = "pool:idle"
POOL_BUSY = "pool:busy"
POOL_DEAD = "pool:dead"
INSTANCE_KEY = "instance:{instance_id}"
INSTANCE_LIMITS_KEY = "instance:{instance_id}:limits"
INSTANCE_LOCK_KEY = "instance:lock:{instance_id}"
STICKY_KEY = "sticky:{caller}"

ACQUIRE_LUA = """
local idle_key = KEYS[1]
local busy_key = KEYS[2]
local instance_prefix = KEYS[3]
local lock_prefix = KEYS[4]
local model = ARGV[1]
local now = tonumber(ARGV[2])
local lease_owner = ARGV[3]
local lease_ttl = tonumber(ARGV[4])
local sticky_id = ARGV[5]

local function try_take(id)
    if not id or id == '' then return nil end
    local lock_key = lock_prefix .. id
    if redis.call('EXISTS', lock_key) == 1 then return nil end
    local caps_raw = redis.call('HGET', instance_prefix .. id, 'plan_caps')
    if model ~= '' and caps_raw and caps_raw ~= '' then
        if string.find(caps_raw, model, 1, true) == nil then return nil end
    end
    redis.call('SET', lock_key, lease_owner, 'EX', lease_ttl)
    redis.call('ZREM', idle_key, id)
    redis.call('SADD', busy_key, id)
    redis.call('HSET', instance_prefix .. id, 'status', 'busy', 'last_used_ts', now)
    return id
end

if sticky_id ~= '' then
    local got = try_take(sticky_id)
    if got then return got end
end

local candidates = redis.call('ZRANGE', idle_key, 0, 31)
for _, id in ipairs(candidates) do
    local got = try_take(id)
    if got then return got end
end
return nil
"""

RELEASE_LUA = """
local idle_key = KEYS[1]
local busy_key = KEYS[2]
local dead_key = KEYS[3]
local instance_key = KEYS[4]
local lock_key = KEYS[5]
local outcome = ARGV[1]
local now = tonumber(ARGV[2])
local cooldown = tonumber(ARGV[3])
local lease_owner = ARGV[4]

local cur = redis.call('GET', lock_key)
if cur and cur ~= lease_owner then
    return 0
end
redis.call('DEL', lock_key)
redis.call('SREM', busy_key, ARGV[5])

if outcome == 'dead' then
    redis.call('SADD', dead_key, ARGV[5])
    redis.call('HSET', instance_key, 'status', 'dead', 'last_used_ts', now)
    return 1
end

local score = now
if outcome == 'rate_limited' then
    score = now + cooldown
end
redis.call('ZADD', idle_key, score, ARGV[5])
redis.call('HSET', instance_key, 'status', 'idle', 'last_used_ts', now)
if outcome == 'ok' then
    redis.call('HSET', instance_key, 'consecutive_errors', 0)
else
    redis.call('HINCRBY', instance_key, 'consecutive_errors', 1)
end
return 1
"""


@dataclass
class InstanceLease:
    instance_id: str
    fingerprint: dict
    proxy_url: str
    account_email: str
    tier: str
    expires_at: float
    owner: str

    def to_dict(self) -> dict:
        return asdict(self)


class CookieVault:
    def __init__(self, key: str):
        if not key:
            raise ValueError("COOKIE_ENCRYPTION_KEY is empty")
        self._fernet = Fernet(key.encode() if isinstance(key, str) else key)

    def encrypt(self, payload: dict) -> bytes:
        return self._fernet.encrypt(json.dumps(payload).encode())

    def decrypt(self, blob: bytes) -> dict:
        try:
            return json.loads(self._fernet.decrypt(blob).decode())
        except InvalidToken:
            raise ValueError("Cookie decryption failed (key mismatch)")


class InstancePool:
    def __init__(self, redis_url: str, gateway_id: str,
                 cookie_key: Optional[str] = None,
                 lease_ttl: int = 14400,
                 cooldown_seconds: int = 60):
        self.redis_url = redis_url
        self.gateway_id = gateway_id
        self.lease_ttl = lease_ttl
        self.cooldown = cooldown_seconds
        self._redis: Optional[aioredis.Redis] = None
        self._vault = CookieVault(cookie_key) if cookie_key else None
        self._acquire_sha: Optional[str] = None
        self._release_sha: Optional[str] = None

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

    async def _load_scripts(self):
        r = await self.connect()
        if self._acquire_sha is None:
            self._acquire_sha = await r.script_load(ACQUIRE_LUA)
        if self._release_sha is None:
            self._release_sha = await r.script_load(RELEASE_LUA)

    async def add_instance(self, *,
                           cookies: dict,
                           fingerprint: dict,
                           proxy_url: str = "",
                           account_email: str = "",
                           tier: str = "plus",
                           plan_caps: Optional[list] = None,
                           notes: str = "",
                           instance_id: Optional[str] = None) -> str:
        if self._vault is None:
            raise RuntimeError("CookieVault not initialised — set COOKIE_ENCRYPTION_KEY")
        instance_id = instance_id or uuid.uuid4().hex
        plan_caps = plan_caps or []
        cookie_blob = self._vault.encrypt(cookies)
        r = await self.connect()
        now = int(time.time())
        instance_key = INSTANCE_KEY.format(instance_id=instance_id)
        pipe = r.pipeline(transaction=True)
        pipe.hset(instance_key, mapping={
            "instance_id": instance_id,
            "cookies": cookie_blob,
            "fingerprint": json.dumps(fingerprint),
            "proxy_url": proxy_url,
            "account_email": account_email,
            "tier": tier,
            "plan_caps": ",".join(plan_caps),
            "status": "idle",
            "consecutive_errors": 0,
            "created_at": now,
            "last_used_ts": now,
            "notes": notes,
        })
        pipe.zadd(POOL_IDLE, {instance_id: now})
        await pipe.execute()
        logger.info(f"InstancePool.add_instance instance_id={instance_id} tier={tier} caps={plan_caps}")
        return instance_id

    async def remove_instance(self, instance_id: str) -> None:
        r = await self.connect()
        pipe = r.pipeline(transaction=True)
        pipe.zrem(POOL_IDLE, instance_id)
        pipe.srem(POOL_BUSY, instance_id)
        pipe.srem(POOL_DEAD, instance_id)
        pipe.delete(INSTANCE_KEY.format(instance_id=instance_id))
        pipe.delete(INSTANCE_LIMITS_KEY.format(instance_id=instance_id))
        pipe.delete(INSTANCE_LOCK_KEY.format(instance_id=instance_id))
        await pipe.execute()
        logger.info(f"InstancePool.remove_instance instance_id={instance_id}")

    async def list_instances(self) -> list:
        r = await self.connect()
        ids = []
        idle = await r.zrange(POOL_IDLE, 0, -1, withscores=True)
        for member, score in idle:
            ids.append((member.decode() if isinstance(member, bytes) else member, "idle", score))
        for s, status in ((POOL_BUSY, "busy"), (POOL_DEAD, "dead")):
            members = await r.smembers(s)
            for m in members:
                ids.append((m.decode() if isinstance(m, bytes) else m, status, 0))
        out = []
        for instance_id, status, score in ids:
            raw = await r.hgetall(INSTANCE_KEY.format(instance_id=instance_id))
            if not raw:
                continue
            decoded = {}
            for k, v in raw.items():
                ks = k.decode() if isinstance(k, bytes) else k
                if ks == "cookies":
                    decoded[ks] = "<encrypted>"
                    continue
                vs = v.decode(errors="replace") if isinstance(v, bytes) else v
                decoded[ks] = vs
            decoded["status"] = status
            decoded["score"] = score
            out.append(decoded)
        return out

    async def acquire(self, model_slug: str = "",
                      sticky_caller: Optional[str] = None) -> Optional[InstanceLease]:
        await self._load_scripts()
        r = await self.connect()
        now = int(time.time())
        sticky_id = ""
        if sticky_caller:
            v = await r.get(STICKY_KEY.format(caller=sticky_caller))
            if v:
                sticky_id = v.decode() if isinstance(v, bytes) else v

        instance_id = await r.evalsha(
            self._acquire_sha, 4,
            POOL_IDLE, POOL_BUSY, "instance:", "instance:lock:",
            model_slug, str(now), self.gateway_id, str(self.lease_ttl), sticky_id,
        )
        if not instance_id:
            return None
        instance_id = instance_id.decode() if isinstance(instance_id, bytes) else instance_id

        if sticky_caller:
            await r.set(STICKY_KEY.format(caller=sticky_caller), instance_id,
                        ex=self.lease_ttl)

        raw = await r.hgetall(INSTANCE_KEY.format(instance_id=instance_id))
        decoded = {}
        for k, v in raw.items():
            ks = k.decode() if isinstance(k, bytes) else k
            vs = v.decode(errors="replace") if isinstance(v, bytes) else v
            decoded[ks] = vs
        fingerprint = json.loads(decoded.get("fingerprint", "{}"))
        proxy_url = decoded.get("proxy_url", "")
        account_email = decoded.get("account_email", "")
        tier = decoded.get("tier", "")

        lease = InstanceLease(
            instance_id=instance_id,
            fingerprint=fingerprint,
            proxy_url=proxy_url,
            account_email=account_email,
            tier=tier,
            expires_at=time.time() + self.lease_ttl,
            owner=self.gateway_id,
        )
        logger.info(f"InstancePool.acquire instance_id={instance_id} model={model_slug} sticky={bool(sticky_id)}")
        return lease

    async def release(self, lease: InstanceLease, outcome: str = "ok") -> None:
        await self._load_scripts()
        r = await self.connect()
        instance_key = INSTANCE_KEY.format(instance_id=lease.instance_id)
        lock_key = INSTANCE_LOCK_KEY.format(instance_id=lease.instance_id)
        await r.evalsha(
            self._release_sha, 5,
            POOL_IDLE, POOL_BUSY, POOL_DEAD, instance_key, lock_key,
            outcome, str(int(time.time())), str(self.cooldown),
            lease.owner, lease.instance_id,
        )
        logger.info(f"InstancePool.release instance_id={lease.instance_id} outcome={outcome}")

    async def get_cookies(self, instance_id: str) -> dict:
        if self._vault is None:
            raise RuntimeError("CookieVault not initialised")
        r = await self.connect()
        blob = await r.hget(INSTANCE_KEY.format(instance_id=instance_id), "cookies")
        if not blob:
            return {}
        return self._vault.decrypt(blob if isinstance(blob, bytes) else blob.encode())

    async def update_cookies(self, instance_id: str, cookies: dict) -> None:
        if self._vault is None:
            raise RuntimeError("CookieVault not initialised")
        r = await self.connect()
        await r.hset(INSTANCE_KEY.format(instance_id=instance_id),
                     "cookies", self._vault.encrypt(cookies))

    async def get_limit(self, instance_id: str, model: str) -> Optional[int]:
        r = await self.connect()
        v = await r.hget(INSTANCE_LIMITS_KEY.format(instance_id=instance_id), model)
        if not v:
            return None
        try:
            return int(v.decode() if isinstance(v, bytes) else v)
        except ValueError:
            return None

    async def set_limit(self, instance_id: str, model: str, clears_at: int) -> None:
        r = await self.connect()
        await r.hset(INSTANCE_LIMITS_KEY.format(instance_id=instance_id),
                     model, str(clears_at))


_singleton: Optional[InstancePool] = None


def get_pool() -> InstancePool:
    global _singleton
    if _singleton is None:
        from utils.configs import (
            redis_url, gateway_id, cookie_encryption_key, lease_ttl_seconds,
        )
        _singleton = InstancePool(
            redis_url=redis_url,
            gateway_id=gateway_id,
            cookie_key=cookie_encryption_key,
            lease_ttl=lease_ttl_seconds,
        )
    return _singleton
