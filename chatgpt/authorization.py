import json
import random

from fastapi import HTTPException

import utils.configs as configs
import utils.globals as globals
from utils.Logger import logger


def get_req_token(req_token, seed=None):
    """
    Returns a *caller identity* string used for sticky-routing & limit bucketing.
    No longer represents a real chatgpt credential — Workers carry cookies via
    the InstancePool. The token-pool / seed-map mechanics are preserved so that
    existing client integrations don't break.
    """
    if configs.auto_seed:
        available_token_list = list(set(globals.token_list) - set(globals.error_token_list))
        length = len(available_token_list)
        if seed and length > 0:
            if seed not in globals.seed_map.keys():
                globals.seed_map[seed] = {"token": random.choice(available_token_list), "conversations": []}
                with open(globals.SEED_MAP_FILE, "w") as f:
                    json.dump(globals.seed_map, f, indent=4)
            else:
                req_token = globals.seed_map[seed]["token"]
            return req_token

        if req_token in configs.authorization_list:
            if length > 0:
                if configs.random_token:
                    return random.choice(available_token_list)
                globals.count = (globals.count + 1) % length
                return available_token_list[globals.count]
            return ""
        return req_token
    seed = req_token
    if seed not in globals.seed_map.keys():
        raise HTTPException(status_code=401, detail={"error": "Invalid Seed"})
    return globals.seed_map[seed]["token"]


async def verify_token(req_token):
    """
    Backwards-compat shim: returns the caller identity string itself in
    Browser-Driver mode. Auth is now via cookies on the worker side.
    """
    if not req_token:
        if configs.authorization_list:
            logger.error("Unauthorized with empty token.")
            raise HTTPException(status_code=401)
        return None
    return req_token


async def refresh_all_tokens(force_refresh=False):
    """No-op kept for APScheduler entrypoint stability; will be removed."""
    logger.info("refresh_all_tokens is a no-op in Browser-Driver mode")
    return
