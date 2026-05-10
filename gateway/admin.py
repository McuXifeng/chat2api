import json

from fastapi import HTTPException, Request, Security, Body
from fastapi.responses import HTMLResponse, JSONResponse

from app import app, templates, security_scheme
from chatgpt.Driver import get_driver
from chatgpt.InstancePool import get_pool
from utils.Logger import logger
from utils.configs import api_prefix, authorization_list


def _check_admin(credentials):
    token = credentials.credentials if credentials else None
    if not authorization_list:
        return  # no admin gate configured
    if token not in authorization_list:
        raise HTTPException(status_code=401, detail="Admin auth required")


@app.get(f"/{api_prefix}/instances" if api_prefix else "/instances", response_class=HTMLResponse)
async def instances_html(request: Request):
    return templates.TemplateResponse(
        "instances.html",
        {"request": request, "api_prefix": api_prefix},
    )


@app.get(f"/{api_prefix}/instances.json" if api_prefix else "/instances.json")
async def instances_list(credentials=Security(security_scheme)):
    _check_admin(credentials)
    pool = get_pool()
    instances = await pool.list_instances()
    return JSONResponse({"instances": instances})


@app.post(f"/{api_prefix}/instances" if api_prefix else "/instances")
async def instances_add(payload: dict = Body(...), credentials=Security(security_scheme)):
    _check_admin(credentials)
    pool = get_pool()
    cookies = payload.get("cookies", {})
    fingerprint = payload.get("fingerprint", {}) or {}
    proxy_url = payload.get("proxy_url", "") or ""
    account_email = payload.get("account_email", "") or ""
    tier = payload.get("tier", "plus")
    plan_caps = payload.get("plan_caps", []) or []
    notes = payload.get("notes", "") or ""
    if isinstance(cookies, str):
        try:
            cookies = json.loads(cookies)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="cookies must be valid JSON")
    if not cookies:
        raise HTTPException(status_code=400, detail="cookies are required")
    try:
        instance_id = await pool.add_instance(
            cookies=cookies,
            fingerprint=fingerprint,
            proxy_url=proxy_url,
            account_email=account_email,
            tier=tier,
            plan_caps=plan_caps,
            notes=notes,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    return JSONResponse({"instance_id": instance_id, "status": "added"})


@app.delete(f"/{api_prefix}/instances/{{instance_id}}" if api_prefix else "/instances/{instance_id}")
async def instances_remove(instance_id: str, credentials=Security(security_scheme)):
    _check_admin(credentials)
    pool = get_pool()
    await pool.remove_instance(instance_id)
    return JSONResponse({"instance_id": instance_id, "status": "removed"})


@app.post(f"/{api_prefix}/instances/{{instance_id}}/test" if api_prefix else "/instances/{instance_id}/test")
async def instances_test(instance_id: str, credentials=Security(security_scheme)):
    _check_admin(credentials)
    driver = get_driver()
    task_id = await driver.dispatch({
        "op": "ping",
        "instance_id": instance_id,
    })
    return JSONResponse({"instance_id": instance_id, "task_id": task_id, "status": "queued"})
