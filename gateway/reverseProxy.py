import json
import random
import time
from datetime import datetime, timezone

from fastapi import Request, HTTPException
from fastapi.responses import StreamingResponse, Response
from starlette.background import BackgroundTask

from chatgpt.InstancePool import get_pool
from utils.Client import Client
from utils.Logger import logger
from utils.configs import chatgpt_base_url_list, file_host, voice_host


def generate_current_time():
    current_time = datetime.now(timezone.utc)
    formatted_time = current_time.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    return formatted_time


headers_reject_list = [
    "x-real-ip",
    "x-forwarded-for",
    "x-forwarded-proto",
    "x-forwarded-port",
    "x-forwarded-host",
    "x-forwarded-server",
    "cf-warp-tag-id",
    "cf-visitor",
    "cf-ray",
    "cf-connecting-ip",
    "cf-ipcountry",
    "cdn-loop",
    "remote-host",
    "x-frame-options",
    "x-xss-protection",
    "x-content-type-options",
    "content-security-policy",
    "host",
    "cookie",
    "connection",
    "content-length",
    "content-encoding",
    "x-middleware-prefetch",
    "x-nextjs-data",
    "purpose",
    "x-forwarded-uri",
    "x-forwarded-path",
    "x-forwarded-method",
    "x-forwarded-protocol",
    "x-forwarded-scheme",
    "cf-request-id",
    "cf-worker",
    "cf-access-client-id",
    "cf-access-client-device-type",
    "cf-access-client-device-model",
    "cf-access-client-device-name",
    "cf-access-client-device-brand",
    "x-middleware-prefetch",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-server",
    "x-real-ip",
    "x-forwarded-port",
    "cf-connecting-ip",
    "cf-ipcountry",
    "cf-ray",
    "cf-visitor",
]

headers_accept_list = [
    "openai-sentinel-chat-requirements-token",
    "openai-sentinel-proof-token",
    "openai-sentinel-turnstile-token",
    "accept",
    "authorization",
    "accept-encoding",
    "accept-language",
    "content-type",
    "oai-device-id",
    "oai-echo-logs",
    "oai-language",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
]


async def _select_instance():
    """Pick an instance from the pool without acquiring a lease.

    The pool's busy/idle bookkeeping is owned by Workers; reverseProxy is read-only.
    """
    pool = get_pool()
    instances = await pool.list_instances()
    if not instances:
        return None
    idle = next((i for i in instances if i.get("status") == "idle"), None)
    return idle or instances[0]


def _flatten_cookies(raw):
    """Cookie-Editor list / dict → flat dict accepted by curl_cffi."""
    if isinstance(raw, dict):
        return {k: str(v) for k, v in raw.items()}
    out = {}
    for c in raw or []:
        if isinstance(c, dict) and "name" in c and "value" in c:
            domain = (c.get("domain") or "").lstrip(".")
            if not domain or "chatgpt.com" in domain or "openai.com" in domain:
                out[c["name"]] = str(c["value"])
    return out


async def content_generator(r):
    async for chunk in r.aiter_content():
        yield chunk


async def chatgpt_reverse_proxy(request: Request, path: str):
    try:
        origin_host = request.url.netloc
        if request.url.is_secure:
            petrol = "https"
        else:
            petrol = "http"
        if "x-forwarded-proto" in request.headers:
            petrol = request.headers["x-forwarded-proto"]
        if "cf-visitor" in request.headers:
            cf_visitor = json.loads(request.headers["cf-visitor"])
            petrol = cf_visitor.get("scheme", petrol)

        params = dict(request.query_params)
        request_cookies = dict(request.cookies)

        # headers = {
        #     key: value for key, value in request.headers.items()
        #     if (key.lower() not in ["host", "origin", "referer", "priority",
        #                             "oai-device-id"] and key.lower() not in headers_reject_list)
        # }
        headers = {
            key: value for key, value in request.headers.items()
            if (key.lower() in headers_accept_list)
        }

        base_url = random.choice(chatgpt_base_url_list) if chatgpt_base_url_list else "https://chatgpt.com"
        if "assets/" in path:
            base_url = "https://cdn.oaistatic.com"
        if "file-" in path and "backend-api" not in path:
            base_url = "https://files.oaiusercontent.com"
        if "v1/" in path:
            base_url = "https://ab.chatgpt.com"
        if "sandbox" in path:
            base_url = "https://web-sandbox.oaiusercontent.com"
            path = path.replace("sandbox/", "")

        chosen = await _select_instance()
        if chosen is None:
            raise HTTPException(
                status_code=503,
                detail="No instance in pool. Add one via POST /instances first.",
            )
        instance_id = chosen["instance_id"]
        pool = get_pool()
        cookies_raw = await pool.get_cookies(instance_id)
        instance_cookies = _flatten_cookies(cookies_raw)
        request_cookies = {**instance_cookies, **request_cookies}

        fp_raw = chosen.get("fingerprint") or "{}"
        fp = json.loads(fp_raw) if isinstance(fp_raw, str) else (fp_raw or {})
        proxy_url = chosen.get("proxy_url", "") or None
        user_agent = fp.get("ua") or fp.get("user-agent") or ""
        impersonate = "chrome120"
        session_id = instance_id

        # Backend uses cookie-based auth; never forward client's bearer token upstream.
        headers.pop("authorization", None)
        if user_agent:
            headers["user-agent"] = user_agent

        headers.update({
            "accept-language": "en-US,en;q=0.9",
            "host": base_url.replace("https://", "").replace("http://", ""),
            "origin": base_url,
            "referer": f"{base_url}/"
        })
        if "v1/initialize" in path:
            headers.update({"user-agent": request.headers.get("user-agent")})
            if "statsig-api-key" not in headers:
                headers.update({
                    "statsig-sdk-type": "js-client",
                    "statsig-api-key": "client-tnE5GCU2F2cTxRiMbvTczMDT1jpwIigZHsZSdqiy4u",
                    "statsig-sdk-version": "5.1.0",
                    "statsig-client-time": int(time.time() * 1000),
                })

        data = await request.body()

        proxy_url = proxy_url.replace("{}", session_id) if proxy_url else None
        client = Client(proxy=proxy_url, impersonate=impersonate)
        try:
            background = BackgroundTask(client.close)
            r = await client.request(request.method, f"{base_url}/{path}", params=params, headers=headers,
                                     cookies=request_cookies, data=data, stream=True, allow_redirects=False)
            if r.status_code == 307 or r.status_code == 302 or r.status_code == 301:
                return Response(status_code=307,
                                headers={"Location": r.headers.get("Location")
                                .replace("ab.chatgpt.com", origin_host)
                                .replace("chatgpt.com", origin_host)
                                .replace("cdn.oaistatic.com", origin_host)
                                .replace("https", petrol)}, background=background)
            elif 'stream' in r.headers.get("content-type", ""):
                logger.info(f"Request instance: {instance_id}")
                logger.info(f"Request proxy: {proxy_url}")
                logger.info(f"Request UA: {user_agent}")
                logger.info(f"Request impersonate: {impersonate}")
                conv_key = r.cookies.get("conv_key", "")
                response = StreamingResponse(content_generator(r), media_type=r.headers.get("content-type", ""),
                                  background=background)
                response.set_cookie("conv_key", value=conv_key)
                return response
            elif 'image' in r.headers.get("content-type", "") or "audio" in r.headers.get("content-type", "") or "video" in r.headers.get("content-type", ""):
                rheaders = dict(r.headers)
                response = Response(content=await r.acontent(), headers=rheaders,
                                        status_code=r.status_code, background=background)
                return response
            else:
                if path.endswith("backend-api/conversation") or path.endswith("backend-alt/conversation") or "/register-websocket" in path:
                    response = Response(content=(await r.acontent()), media_type=r.headers.get("content-type"),
                                        status_code=r.status_code, background=background)
                else:
                    content = await r.atext()
                    if "public-api/" in path:
                        content = (content
                                   .replace("https://ab.chatgpt.com", f"{petrol}://{origin_host}")
                                   .replace("https://cdn.oaistatic.com", f"{petrol}://{origin_host}")
                                   .replace("webrtc.chatgpt.com", voice_host if voice_host else "webrtc.chatgpt.com")
                                   .replace("files.oaiusercontent.com", file_host if file_host else "files.oaiusercontent.com")
                                   .replace("chatgpt.com/ces", f"{origin_host}/ces")
                                   )
                    else:
                        content = (content
                                   .replace("https://ab.chatgpt.com", f"{petrol}://{origin_host}")
                                   .replace("https://cdn.oaistatic.com", f"{petrol}://{origin_host}")
                                   .replace("webrtc.chatgpt.com", voice_host if voice_host else "webrtc.chatgpt.com")
                                   .replace("files.oaiusercontent.com", file_host if file_host else "files.oaiusercontent.com")
                                   .replace("web-sandbox.oaiusercontent.com", f"{origin_host}/sandbox")
                                   .replace("https://chatgpt.com", f"{petrol}://{origin_host}")
                                   .replace("chatgpt.com/ces", f"{origin_host}/ces")
                                   )
                    if base_url == "https://web-sandbox.oaiusercontent.com":
                        content = content.replace("/assets", "/sandbox/assets")
                    rheaders = dict(r.headers)
                    content_type = rheaders.get("content-type", "")
                    cache_control = rheaders.get("cache-control", "")
                    expires = rheaders.get("expires", "")
                    content_disposition = rheaders.get("content-disposition", "")
                    rheaders = {
                        "cache-control": cache_control,
                        "content-type": content_type,
                        "expires": expires,
                        "content-disposition": content_disposition
                    }
                    response = Response(content=content, headers=rheaders,
                                        status_code=r.status_code, background=background)
                return response
        except Exception as e:
            await client.close()
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
