import asyncio
import types

from fastapi import Request, HTTPException, Form, Security, Header
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
from starlette.background import BackgroundTask

import utils.globals as globals
from app import app, templates, security_scheme
from chatgpt.ChatService import ChatService
from chatgpt.Driver import get_driver
from chatgpt.Heartbeat import heartbeat_wrap
from utils.Logger import logger
from utils.configs import api_prefix, heartbeat_interval


@app.on_event("startup")
async def app_start():
    try:
        driver = get_driver()
        await driver.ensure_groups()
        logger.info("Driver Redis groups ensured")
    except Exception as e:
        logger.error(f"Driver startup failed (non-fatal): {e}")


async def to_send_conversation(request_data, req_token):
    chat_service = ChatService(req_token)
    try:
        await chat_service.set_dynamic_data(request_data)
        return chat_service
    except HTTPException as e:
        await chat_service.close_client()
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    except Exception as e:
        await chat_service.close_client()
        logger.error(f"Server error, {str(e)}")
        raise HTTPException(status_code=500, detail="Server error")


async def process(request_data, req_token):
    chat_service = await to_send_conversation(request_data, req_token)
    await chat_service.prepare_send_conversation()
    res = await chat_service.send_conversation()
    return chat_service, res


@app.post(f"/{api_prefix}/v1/chat/completions" if api_prefix else "/v1/chat/completions")
async def send_conversation(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(security_scheme),
):
    req_token = credentials.credentials
    try:
        request_data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail={"error": "Invalid JSON body"})

    chat_service, res = await process(request_data, req_token)
    try:
        if chat_service.async_mode:
            background = BackgroundTask(chat_service.close_client)
            return JSONResponse(res, status_code=202, background=background)
        if isinstance(res, types.AsyncGeneratorType):
            background = BackgroundTask(chat_service.close_client)
            return StreamingResponse(
                heartbeat_wrap(res, interval=heartbeat_interval),
                media_type="text/event-stream", background=background,
            )
        background = BackgroundTask(chat_service.close_client)
        return JSONResponse(res, media_type="application/json", background=background)
    except HTTPException as e:
        await chat_service.close_client()
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    except Exception as e:
        await chat_service.close_client()
        logger.error(f"Server error, {str(e)}")
        raise HTTPException(status_code=500, detail="Server error")


@app.get(f"/{api_prefix}/v1/tasks/{{task_id}}" if api_prefix else "/v1/tasks/{task_id}")
async def get_task_status(task_id: str,
                          credentials: HTTPAuthorizationCredentials = Security(security_scheme)):
    driver = get_driver()
    task = await driver.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found or expired")
    safe = {k: v for k, v in task.items() if k != "payload"}
    return JSONResponse(safe)


@app.get(f"/{api_prefix}/v1/tasks/{{task_id}}/events" if api_prefix else "/v1/tasks/{task_id}/events")
async def stream_task_events(task_id: str,
                             last_event_id: str = Header(default="0-0", alias="Last-Event-ID"),
                             credentials: HTTPAuthorizationCredentials = Security(security_scheme)):
    driver = get_driver()
    task = await driver.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found or expired")

    async def event_generator():
        async for chunk in driver.iter_chunks(
            task_id, last_id=last_event_id, heartbeat_every=heartbeat_interval
        ):
            cur = await driver.cursor_get(task_id)
            if chunk.startswith(b": "):
                yield chunk
                continue
            yield b"id: " + cur.encode() + b"\n" + chunk
        yield b"event: done\ndata: {}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post(f"/{api_prefix}/v1/tasks/{{task_id}}/cancel" if api_prefix else "/v1/tasks/{task_id}/cancel")
async def cancel_task(task_id: str,
                      credentials: HTTPAuthorizationCredentials = Security(security_scheme)):
    driver = get_driver()
    task = await driver.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    await driver.cancel(task_id)
    return JSONResponse({"task_id": task_id, "status": "cancelling"})


@app.get(f"/{api_prefix}/tokens" if api_prefix else "/tokens", response_class=HTMLResponse)
async def upload_html(request: Request):
    tokens_count = len(set(globals.token_list) - set(globals.error_token_list))
    return templates.TemplateResponse("tokens.html",
                                      {"request": request, "api_prefix": api_prefix, "tokens_count": tokens_count})


@app.post(f"/{api_prefix}/tokens/upload" if api_prefix else "/tokens/upload")
async def upload_post(text: str = Form(...)):
    lines = text.split("\n")
    for line in lines:
        if line.strip() and not line.startswith("#"):
            globals.token_list.append(line.strip())
            with open(globals.TOKENS_FILE, "a", encoding="utf-8") as f:
                f.write(line.strip() + "\n")
    logger.info(f"Token count: {len(globals.token_list)}, Error token count: {len(globals.error_token_list)}")
    tokens_count = len(set(globals.token_list) - set(globals.error_token_list))
    return {"status": "success", "tokens_count": tokens_count}


@app.post(f"/{api_prefix}/tokens/clear" if api_prefix else "/tokens/clear")
async def clear_tokens():
    globals.token_list.clear()
    globals.error_token_list.clear()
    with open(globals.TOKENS_FILE, "w", encoding="utf-8") as f:
        pass
    logger.info(f"Token count: {len(globals.token_list)}, Error token count: {len(globals.error_token_list)}")
    tokens_count = len(set(globals.token_list) - set(globals.error_token_list))
    return {"status": "success", "tokens_count": tokens_count}


@app.post(f"/{api_prefix}/tokens/error" if api_prefix else "/tokens/error")
async def error_tokens():
    error_tokens_list = list(set(globals.error_token_list))
    return {"status": "success", "error_tokens": error_tokens_list}


@app.get(f"/{api_prefix}/tokens/add/{{token}}" if api_prefix else "/tokens/add/{token}")
async def add_token(token: str):
    if token.strip() and not token.startswith("#"):
        globals.token_list.append(token.strip())
        with open(globals.TOKENS_FILE, "a", encoding="utf-8") as f:
            f.write(token.strip() + "\n")
    logger.info(f"Token count: {len(globals.token_list)}, Error token count: {len(globals.error_token_list)}")
    tokens_count = len(set(globals.token_list) - set(globals.error_token_list))
    return {"status": "success", "tokens_count": tokens_count}


@app.post(f"/{api_prefix}/seed_tokens/clear" if api_prefix else "/seed_tokens/clear")
async def clear_seed_tokens():
    globals.seed_map.clear()
    globals.conversation_map.clear()
    with open(globals.SEED_MAP_FILE, "w", encoding="utf-8") as f:
        f.write("{}")
    with open(globals.CONVERSATION_MAP_FILE, "w", encoding="utf-8") as f:
        f.write("{}")
    logger.info(f"Seed token count: {len(globals.seed_map)}")
    return {"status": "success", "seed_tokens_count": len(globals.seed_map)}
