import asyncio
import json
import os
from typing import Optional


STEALTH_INIT_SCRIPT = r"""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
window.chrome = window.chrome || { runtime: {} };
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters)
    );
}
Object.defineProperty(navigator, 'plugins', {
    get: () => [{name:'Chrome PDF Plugin'},{name:'Chrome PDF Viewer'},{name:'Native Client'}],
});
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
"""


class BrowserSlot:
    """One Playwright BrowserContext bound 1:1 to an instance_id."""

    def __init__(self, *, instance_id: str, cookies: dict, fingerprint: dict,
                 proxy_url: str = "", headless: bool = False):
        self.instance_id = instance_id
        self.cookies = cookies
        self.fingerprint = fingerprint or {}
        self.proxy_url = proxy_url
        self.headless = headless
        self._pw = None
        self._browser = None
        self._ctx = None
        self._page = None
        self._abort_controllers: dict[str, str] = {}
        self._chunk_callbacks: dict[str, callable] = {}
        self._lock = asyncio.Lock()

    async def start(self):
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise RuntimeError(
                "playwright not installed; run `pip install playwright && playwright install chromium`"
            ) from e

        self._pw = await async_playwright().start()

        ua = self.fingerprint.get("ua") or self.fingerprint.get("user-agent")
        viewport = self.fingerprint.get("viewport") or {"width": 1440, "height": 900}
        timezone = self.fingerprint.get("timezone", "America/Los_Angeles")
        locale = self.fingerprint.get("accept_language", "en-US")

        launch_args = ["--disable-blink-features=AutomationControlled"]
        proxy = None
        if self.proxy_url:
            proxy = {"server": self.proxy_url}

        user_data_dir = os.path.join(
            os.getenv("BROWSER_PROFILES", "/tmp/browser-profiles"),
            self.instance_id,
        )
        os.makedirs(user_data_dir, exist_ok=True)

        self._ctx = await self._pw.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=self.headless,
            args=launch_args,
            user_agent=ua,
            viewport=viewport,
            timezone_id=timezone,
            locale=locale,
            proxy=proxy,
        )
        await self._ctx.add_init_script(STEALTH_INIT_SCRIPT)
        if self.cookies:
            playwright_cookies = self._cookies_to_playwright(self.cookies)
            if playwright_cookies:
                await self._ctx.add_cookies(playwright_cookies)

        self._page = await self._ctx.new_page()
        await self._page.goto("https://chatgpt.com", wait_until="domcontentloaded")

        # JS bridge: page calls window.__chunkBridge with task_id + base64 chunk
        async def _on_chunk(source, task_id: str, b64: str):
            cb = self._chunk_callbacks.get(task_id)
            if cb:
                import base64
                try:
                    cb(base64.b64decode(b64))
                except Exception:
                    pass

        await self._ctx.expose_binding("__chunkBridge", _on_chunk)

        async def _on_done(source, task_id: str):
            cb = self._chunk_callbacks.get(task_id)
            if cb:
                cb(None)  # sentinel

        await self._ctx.expose_binding("__chunkDone", _on_done)

    @staticmethod
    def _cookies_to_playwright(cookies: dict) -> list:
        if isinstance(cookies, list):
            return cookies
        if not isinstance(cookies, dict):
            return []
        out = []
        for name, value in cookies.items():
            out.append({
                "name": name, "value": str(value),
                "domain": ".chatgpt.com", "path": "/",
                "secure": True, "httpOnly": False, "sameSite": "Lax",
            })
        return out

    async def run_chat(self, task_id: str, request_body: dict, on_chunk: callable):
        """Execute a /backend-api/conversation call inside the page; pipe chunks via callback."""
        async with self._lock:
            self._chunk_callbacks[task_id] = on_chunk
            try:
                await self._page.evaluate(
                    """async ([taskId, body]) => {
                        window.__abortControllers = window.__abortControllers || {};
                        const ctrl = new AbortController();
                        window.__abortControllers[taskId] = ctrl;
                        try {
                            const resp = await fetch('/backend-api/conversation', {
                                method: 'POST',
                                headers: {
                                    'content-type': 'application/json',
                                    'accept': 'text/event-stream',
                                },
                                body: JSON.stringify(body),
                                signal: ctrl.signal,
                                credentials: 'include',
                            });
                            const reader = resp.body.getReader();
                            while (true) {
                                const { value, done } = await reader.read();
                                if (done) break;
                                const b64 = btoa(String.fromCharCode.apply(null, value));
                                await window.__chunkBridge(taskId, b64);
                            }
                        } catch (e) {
                            // aborted or network error; surface via __chunkDone
                        } finally {
                            delete window.__abortControllers[taskId];
                            await window.__chunkDone(taskId);
                        }
                    }""",
                    [task_id, request_body],
                )
            finally:
                self._chunk_callbacks.pop(task_id, None)

    async def abort(self, task_id: str):
        try:
            await self._page.evaluate(
                """(tid) => {
                    if (window.__abortControllers && window.__abortControllers[tid]) {
                        window.__abortControllers[tid].abort();
                    }
                }""",
                task_id,
            )
        except Exception:
            pass

    async def ping(self) -> bool:
        try:
            r = await self._page.evaluate(
                "fetch('/api/auth/session').then(r => r.status)"
            )
            return r == 200
        except Exception:
            return False

    async def export_cookies(self) -> list:
        if self._ctx is None:
            return []
        return await self._ctx.cookies()

    async def close(self):
        try:
            if self._ctx is not None:
                await self._ctx.close()
        finally:
            if self._pw is not None:
                await self._pw.stop()
            self._ctx = None
            self._page = None
            self._pw = None
