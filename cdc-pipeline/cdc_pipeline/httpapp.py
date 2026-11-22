"""零依赖 HTTP 微框架：JSON REST + 静态页面，带优雅停机。"""
from __future__ import annotations

import json
import logging
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

from .common import rows_to_dicts

Handler = Callable[[dict, "Request"], Any]
log = logging.getLogger("cdc.http")

_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
}


class ApiError(Exception):
    def __init__(self, status: int, message: str, detail: Any = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.detail = detail


class Request:
    def __init__(self, handler: BaseHTTPRequestHandler, body: bytes, url_params: dict):
        self._h = handler
        self.body_bytes = body
        self.url_params = url_params
        self.query = _parse_query(handler.path)

    def json(self) -> dict:
        if not self.body_bytes:
            return {}
        try:
            data = json.loads(self.body_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ApiError(400, f"请求体不是合法 JSON: {exc.msg}")
        if not isinstance(data, dict):
            raise ApiError(400, "请求体必须是 JSON 对象")
        return data


def _parse_query(path: str) -> dict:
    from urllib.parse import parse_qs, urlsplit

    qs = parse_qs(urlsplit(path).query)
    return {k: v[-1] for k, v in qs.items()}


class Router:
    def __init__(self):
        self.routes: list[tuple[re.Pattern, str, Handler]] = []

    def add(self, method: str, pattern: str, handler: Handler):
        regex = re.compile("^" + re.sub(r"{(\w+)}", r"(?P<\1>[^/]+)", pattern) + "$")
        self.routes.append((regex, method, handler))

    def match(self, method: str, path: str):
        for regex, m, handler in self.routes:
            match = regex.match(path)
            if match and m == method:
                return handler, match.groupdict()
        return None, {}


class App(BaseHTTPRequestHandler):
    router: Router
    static_dir: Path
    server_version = "CDC/1.0"

    def log_message(self, fmt, *args):  # 安静一些，交给 logging
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, status: int, payload: Any):
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_file(self, path: Path):
        if not path.is_file():
            self._send_json(404, {"error": "not found"})
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", _CONTENT_TYPES.get(path.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle(self, method: str):
        from urllib.parse import urlsplit

        path = urlsplit(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        handler, params = self.router.match(method, path)
        if handler is None:
            if method == "GET" and self.static_dir is not None:
                rel = "index.html" if path in ("/", "") else path.lstrip("/")
                candidate = (self.static_dir / rel).resolve()
                if self.static_dir.resolve() in candidate.parents or candidate == self.static_dir.resolve():
                    self._send_file(candidate)
                    return
            self._send_json(404, {"error": f"无此路由: {method} {path}"})
            return
        try:
            request = Request(self, body, params)
            result = handler(request, params)
        except ApiError as exc:
            self._send_json(exc.status, {"error": exc.message, "detail": exc.detail})
        except Exception as exc:  # noqa: BLE001 - 服务边界兜底
            log.exception("处理 %s %s 失败", method, path)
            self._send_json(500, {"error": f"服务器内部错误: {exc}"})
        else:
            if result is None:
                self._send_json(200, {"ok": True})
            elif isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], int):
                self._send_json(result[0], result[1])
            else:
                self._send_json(200, result)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")

    def do_DELETE(self):
        self._handle("DELETE")


def make_server(host: str, port: int, router: Router, static_dir: Path | None) -> ThreadingHTTPServer:
    App.router = router
    App.static_dir = static_dir
    httpd = ThreadingHTTPServer((host, port), App)
    httpd.daemon_threads = True
    return httpd


def json_list(rows) -> list[dict]:
    return rows_to_dicts(rows)
