"""
One-shot migration: read data/token.txt + data/fp_map.json + data/refresh_map.json,
push a 'bootstrap_login' job per refresh_token onto Redis. The Worker handles each
job by spinning up a headed Chromium, calling /auth/login_auth_complete with the
access_token, capturing the resulting cookie jar, and calling InstancePool.add_instance.

Run from the gateway container:

    python scripts/migrate_tokens_to_instances.py

This script does NOT delete the legacy data files; once everything is on Redis,
remove them manually or set INSTANCE_POOL_BACKEND=redis in .env to ignore them.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import utils.globals as globals  # noqa: E402
from chatgpt.Driver import Driver  # noqa: E402
from utils.Logger import logger  # noqa: E402


async def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    driver = Driver(redis_url=redis_url)
    await driver.ensure_groups()

    tokens = list(set(globals.token_list) - set(globals.error_token_list))
    fp_map = globals.fp_map
    refresh_map = globals.refresh_map
    logger.info(f"Migrating {len(tokens)} refresh_tokens → bootstrap jobs")

    for rt in tokens:
        if len(rt) != 45:
            continue
        access_token = (refresh_map.get(rt, {}) or {}).get("token", "")
        fp = fp_map.get(rt, {}) or {}
        payload = {
            "op": "bootstrap_login",
            "refresh_token": rt,
            "access_token": access_token,
            "fingerprint": fp,
        }
        task_id = await driver.dispatch(payload)
        logger.info(f"queued bootstrap task_id={task_id} for rt[..8]={rt[:8]}")

    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
