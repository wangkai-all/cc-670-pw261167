"""目标仓 HTTP 客户端：提交变更、幂等查询。"""
from __future__ import annotations

import json
import urllib.error
import urllib.request


class TargetClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(self.base_url + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json; charset=utf-8")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {"error": body}
            raise TargetError(exc.code, parsed.get("error", body), parsed) from exc

    def health(self):
        return self._request("GET", "/api/health")

    def get_commit(self, source_id: str, txn_id: str):
        return self._request("GET", f"/api/commits/ref/{source_id}/{txn_id}")

    def submit_commit(self, source_id: str, txn_id: str, commit_seq, changes: list[dict]):
        return self._request(
            "POST",
            "/api/commits",
            {"source_id": source_id, "txn_id": txn_id, "commit_seq": commit_seq, "changes": changes},
        )


class TargetError(Exception):
    def __init__(self, status: int, message: str, detail: dict):
        super().__init__(message)
        self.status = status
        self.message = message
        self.detail = detail
