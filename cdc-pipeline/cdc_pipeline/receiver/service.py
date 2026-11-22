"""接收服务入口：8071 端口，提供导入/重排/重投/拉取 API 与管理页面。"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from ..common import DB
from ..httpapp import ApiError, Router, make_server
from . import generator
from .store import ReceiverStore

log = logging.getLogger("cdc.receiver")


def build_router(store: ReceiverStore) -> Router:
    r = Router()

    r.add("GET", "/api/health", lambda req, ctx: {"service": "receiver", "ok": True})

    r.add("GET", "/api/fragments", lambda req, ctx: store.list_fragments())

    r.add("GET", "/api/fragments/{fid}", lambda req, ctx: store.get_fragment(ctx["fid"]))

    def import_frag(req, ctx):
        body = req.json()
        content = body.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ApiError(400, "content 必须是非空字符串（JSON 数组或 NDJSON）")
        return store.import_fragment(content, body.get("fragment_id"), body.get("note"))

    r.add("POST", "/api/fragments", import_frag)

    def redeliver(req, ctx):
        body = req.json()
        return store.redeliver(ctx["fid"], bool(body.get("tampered", False)))

    r.add("POST", "/api/fragments/{fid}/redeliver", redeliver)

    def reorder(req, ctx):
        body = req.json()
        direction = body.get("direction", "")
        if direction not in ("up", "down"):
            raise ApiError(400, "direction 必须是 up 或 down")
        return store.reorder(int(ctx["attempt_id"]), direction)

    r.add("POST", "/api/deliveries/{attempt_id}/reorder", reorder)

    # 配对服务消费接口
    def fetch(req, ctx):
        after = int(req.query.get("after", "0"))
        return {"deliveries": store.fetch_pending(after)}

    r.add("GET", "/api/stream/pending", fetch)
    r.add("POST", "/api/stream/ack/{attempt_id}", lambda req, ctx: store.mark_consumed(int(ctx["attempt_id"])))

    # 一键生成项目自身日志（生成文件 + 自动导入为片段）
    def load_demo(req, ctx):
        base = Path(os.environ.get("CDC_DATA_DIR", "data")) / "generated_logs"
        paths = generator.write_demo_dir(base)
        created = []
        for frag in generator.build_fragments():
            try:
                created.append(
                    store.import_fragment(generator.to_jsonl(frag["events"]), frag["fragment_id"], frag["note"])
                )
            except ApiError as exc:
                if exc.status != 409:
                    raise
        return {"generated_files": [str(p) for p in paths], "imported": [c["fragment_id"] for c in created]}

    r.add("POST", "/api/demo/load", load_demo)
    return r


def main():
    parser = argparse.ArgumentParser(description="CDC 接收服务")
    parser.add_argument("--host", default=os.environ.get("CDC_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("RECEIVER_PORT", "8071")))
    parser.add_argument("--data-dir", default=os.environ.get("CDC_DATA_DIR", "data"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    data = Path(args.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    db = DB(str(data / "receiver.db"))
    store = ReceiverStore(db)
    static = Path(__file__).parent.parent / "web"
    httpd = make_server(args.host, args.port, build_router(store), static)
    log.info("接收服务启动: http://%s:%s  数据: %s", args.host, args.port, db.path)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        db.close()


if __name__ == "__main__":
    main()
