import json
import random
import uuid
from typing import Optional

from fastapi import HTTPException

from api.models import model_proxy
from chatgpt.authorization import get_req_token
from chatgpt.chatFormat import api_messages_to_chat, stream_response, format_not_stream_response, head_process_response
from chatgpt.Driver import get_driver
from chatgpt.InstancePool import get_pool, InstanceLease

from utils.Logger import logger
from utils.configs import (
    history_disabled,
    upload_by_url,
    async_models,
    task_ttl_seconds,
    heartbeat_interval,
)


class ChatService:
    def __init__(self, origin_token: Optional[str] = None):
        self.req_token = get_req_token(origin_token)
        self.lease: Optional[InstanceLease] = None
        self.task_id: Optional[str] = None
        self.async_mode = False
        self._driver = get_driver()
        self._pool = get_pool()

    async def set_dynamic_data(self, data: dict):
        self.data = data
        await self.set_model()

        self.async_mode = self._is_async(data)

        # 限流: 读 instance:<id>:limits HASH（acquire 之后再校验，因为限流是按 instance 维度）
        self.parent_message_id = data.get('parent_message_id')
        self.conversation_id = data.get('conversation_id')
        self.history_disabled = data.get('history_disabled', history_disabled)
        self.api_messages = data.get("messages", [])
        self.prompt_tokens = 0
        self.max_tokens = data.get("max_tokens", 2147483647)
        if not isinstance(self.max_tokens, int):
            self.max_tokens = 2147483647

        sticky_caller = self.req_token if self.req_token else None
        self.lease = await self._pool.acquire(
            model_slug=self.req_model, sticky_caller=sticky_caller
        )
        if self.lease is None:
            raise HTTPException(
                status_code=503,
                detail="No available browser instance for the requested model",
            )

        clears_at = await self._pool.get_limit(self.lease.instance_id, self.req_model)
        if clears_at:
            import time
            if clears_at > int(time.time()):
                await self._pool.release(self.lease, outcome="rate_limited")
                self.lease = None
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate-limit on instance for model {self.req_model}, clears at {clears_at}",
                )

        logger.info(
            f"ChatService: lease instance_id={self.lease.instance_id} "
            f"req_model={self.req_model} async_mode={self.async_mode}"
        )

    def _is_async(self, data: dict) -> bool:
        model = data.get("model", "")
        if data.get("mode") == "async":
            return True
        return any(model.startswith(prefix) for prefix in async_models)

    async def set_model(self):
        self.origin_model = self.data.get("model", "gpt-3.5-turbo-0125")
        self.resp_model = model_proxy.get(self.origin_model, self.origin_model)
        if "gizmo" in self.origin_model or "g-" in self.origin_model:
            self.gizmo_id = "g-" + self.origin_model.split("g-")[-1]
        else:
            self.gizmo_id = None

        m = self.origin_model
        if "gpt-5.5-pro" in m:
            self.req_model = "gpt-5.5-pro"
        elif "o3-mini-high" in m:
            self.req_model = "o3-mini-high"
        elif "o3-mini-medium" in m:
            self.req_model = "o3-mini-medium"
        elif "o3-mini-low" in m:
            self.req_model = "o3-mini-low"
        elif "o3-mini" in m:
            self.req_model = "o3-mini"
        elif "o3" in m:
            self.req_model = "o3"
        elif "o1-preview" in m:
            self.req_model = "o1-preview"
        elif "o1-pro" in m:
            self.req_model = "o1-pro"
        elif "o1-mini" in m:
            self.req_model = "o1-mini"
        elif "o1" in m:
            self.req_model = "o1"
        elif "gpt-4.5o" in m:
            self.req_model = "gpt-4.5o"
        elif "gpt-4o-canmore" in m:
            self.req_model = "gpt-4o-canmore"
        elif "gpt-4o-mini" in m:
            self.req_model = "gpt-4o-mini"
        elif "gpt-4o" in m:
            self.req_model = "gpt-4o"
        elif "gpt-4-mobile" in m:
            self.req_model = "gpt-4-mobile"
        elif "gpt-4" in m:
            self.req_model = "gpt-4"
        elif "gpt-3.5" in m:
            self.req_model = "text-davinci-002-render-sha"
        elif "auto" in m:
            self.req_model = "auto"
        else:
            self.req_model = "gpt-4o"

    async def prepare_send_conversation(self):
        try:
            chat_messages, self.prompt_tokens = await api_messages_to_chat(
                self, self.api_messages, upload_by_url
            )
        except Exception as e:
            logger.error(f"Failed to format messages: {str(e)}")
            raise HTTPException(status_code=400, detail="Failed to format messages.")

        if self.gizmo_id:
            conversation_mode = {"kind": "gizmo_interaction", "gizmo_id": self.gizmo_id}
        else:
            conversation_mode = {"kind": "primary_assistant"}

        chat_request = {
            "action": "next",
            "client_contextual_info": {
                "is_dark_mode": False,
                "time_since_loaded": random.randint(50, 500),
                "page_height": random.randint(500, 1000),
                "page_width": random.randint(1000, 2000),
                "pixel_ratio": 1.5,
                "screen_height": random.randint(800, 1200),
                "screen_width": random.randint(1200, 2200),
            },
            "conversation_mode": conversation_mode,
            "conversation_origin": None,
            "force_paragen": False,
            "force_paragen_model_slug": "",
            "force_rate_limit": False,
            "force_use_sse": True,
            "history_and_training_disabled": self.history_disabled,
            "messages": chat_messages,
            "model": self.req_model,
            "paragen_cot_summary_display_override": "allow",
            "paragen_stream_type_override": None,
            "parent_message_id": self.parent_message_id if self.parent_message_id else f"{uuid.uuid4()}",
            "reset_rate_limits": False,
            "suggestions": [],
            "supported_encodings": [],
            "system_hints": [],
            "timezone": "America/Los_Angeles",
            "timezone_offset_min": -480,
            "variant_purpose": "comparison_implicit",
            "websocket_request_id": f"{uuid.uuid4()}",
        }
        if self.conversation_id:
            chat_request['conversation_id'] = self.conversation_id

        self.chat_request = chat_request
        return chat_request

    async def send_conversation(self):
        payload = {
            "op": "chat",
            "instance_id": self.lease.instance_id,
            "model": self.req_model,
            "request": self.chat_request,
        }
        self.task_id = await self._driver.dispatch(payload, ttl=task_ttl_seconds)

        if self.async_mode:
            return {
                "id": self.task_id,
                "object": "chat.completion.task",
                "status": "queued",
                "model": self.resp_model,
            }

        stream = self.data.get("stream", False)
        chunks_iter = self._driver.iter_chunks(
            self.task_id, heartbeat_every=heartbeat_interval
        )
        res, start = await head_process_response(chunks_iter)
        if not start:
            raise HTTPException(
                status_code=403,
                detail="No content from worker; instance may be cooling.",
            )
        if stream:
            return stream_response(self, res, self.resp_model, self.max_tokens)
        return await format_not_stream_response(
            stream_response(self, res, self.resp_model, self.max_tokens),
            self.prompt_tokens, self.max_tokens, self.resp_model,
        )

    async def upload_file(self, file_content, mime_type):
        if not file_content or not mime_type:
            return None
        # Worker 端处理上传; gateway 不再直连 chatgpt.com 上传通道。
        # 简化版: 文件以 base64 inline 进 messages，让 worker 在浏览器内 fetch /backend-api/files。
        # 这里先返回 None 让 chatFormat 走文本回退路径，避免阻断主链路。
        # 后续在 worker 落地 upload op 后再启用。
        logger.info("upload_file via worker not yet wired; falling back to inline text")
        return None

    async def check_upload(self, file_id):
        return True

    async def close_client(self):
        if self.lease is not None:
            try:
                await self._pool.release(self.lease, outcome="ok")
            except Exception as e:
                logger.error(f"release lease failed: {e}")
            finally:
                self.lease = None

    async def dispatch_only(self) -> str:
        """Async-mode 路由用: dispatch 完不读 chunks, 返回 task_id."""
        if self.task_id is None:
            payload = {
                "op": "chat",
                "instance_id": self.lease.instance_id,
                "model": self.req_model,
                "request": self.chat_request,
            }
            self.task_id = await self._driver.dispatch(payload, ttl=task_ttl_seconds)
        return self.task_id
