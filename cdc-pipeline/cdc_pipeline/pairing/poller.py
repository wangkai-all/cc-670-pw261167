"""后台轮询：按保存的消费位置从接收服务拉取新投递，重启后不跳过未完成事务。"""
from __future__ import annotations

import logging
import threading
import urllib.error
import urllib.request
import json

from .store import PairingStore

log = logging.getLogger("cdc.pairing.poller")


class ReceiverPoller:
    def __init__(self, receiver_url: str, store: PairingStore, interval: float = 1.0):
        self.receiver_url = receiver_url.rstrip("/")
        self.store = store
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        self._thread = threading.Thread(target=self._loop, name="receiver-poller", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def run_once(self) -> dict:
        """同步拉取一轮，也供手动按钮与测试调用。"""
        after = self.store.get_checkpoint()
        try:
            with urllib.request.urlopen(
                f"{self.receiver_url}/api/stream/pending?after={after}", timeout=10
            ) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError) as exc:
            log.warning("拉取接收服务失败: %s", exc)
            return {"ok": False, "error": str(exc), "ingested": []}

        ingested = []
        for delivery in payload.get("deliveries", []):
            result = self.store.ingest_delivery(delivery)
            ingested.append(result)
            if not result["duplicate"]:
                self._ack(delivery["attempt_id"])
                log.info(
                    "消费投递 attempt=%s fragment=%s 事件=%s 冲突=%s",
                    delivery["attempt_id"], delivery["fragment_id"],
                    result["events_processed"], result["conflicts"],
                )
        return {"ok": True, "checkpoint": self.store.get_checkpoint(), "ingested": ingested}

    def _ack(self, attempt_id: int):
        req = urllib.request.Request(
            f"{self.receiver_url}/api/stream/ack/{attempt_id}", data=b"", method="POST"
        )
        try:
            urllib.request.urlopen(req, timeout=10).read()
        except (urllib.error.URLError, TimeoutError) as exc:
            # ACK 失败无大碍：配对侧已有 attempt 去重，下一轮拉到也只会被跳过
            log.warning("ACK attempt=%s 失败: %s", attempt_id, exc)

    def _loop(self):
        while not self._stop.wait(self.interval):
            try:
                self.run_once()
            except Exception:  # noqa: BLE001 - 轮询线程不能因单轮错误退出
                log.exception("轮询轮次异常")
