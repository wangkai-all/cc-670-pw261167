"""配对服务入口：8072 端口。消费接收流、配对、异常管理、按提交序号发布。"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from ..common import DB
from ..httpapp import ApiError, Router, make_server
from .poller import ReceiverPoller
from .store import PairingStore
from .target_client import TargetClient, TargetError

log = logging.getLogger("cdc.pairing")


def build_router(store: PairingStore, poller: ReceiverPoller, target: TargetClient) -> Router:
    r = Router()

    r.add("GET", "/api/health", lambda req, ctx: {
        "service": "pairing", "ok": True, "checkpoint": store.get_checkpoint()
    })

    # 手动触发拉取 / 重新核验
    r.add("POST", "/api/poll", lambda req, ctx: poller.run_once())
    r.add("POST", "/api/reverify", lambda req, ctx: store.reverify_all())

    # 视图
    r.add("GET", "/api/pending", lambda req, ctx: store.pending_pairs())
    r.add("GET", "/api/ready", lambda req, ctx: store.ready_changes())
    r.add("GET", "/api/exceptions", lambda req, ctx: store.exceptions())
    r.add("GET", "/api/conflicts", lambda req, ctx: store.conflicts(
        req.query.get("resolved") == "1"))
    r.add("GET", "/api/txns", lambda req, ctx: store.txns_overview())
    r.add("GET", "/api/deliveries", lambda req, ctx: store.delivery_log())
    r.add("GET", "/api/publish-log", lambda req, ctx: store.publish_log())

    # 预览下一笔可发布事务将产生的完整变更（主键迁移展示为撤旧加新）
    def preview(req, ctx):
        nxt = store.next_publishable()
        if nxt is None:
            return {"next": None, "changes": []}
        changes = store.build_changes(nxt["source_id"], nxt["txn_id"])
        return {"next": nxt, "changes": changes}
    r.add("GET", "/api/publish/preview", preview)

    def publish(req, ctx):
        nxt = store.next_publishable()
        if nxt is None:
            raise ApiError(409, "没有可发布的完整事务（异常未补齐或已全部发布）")
        src, txn = nxt["source_id"], nxt["txn_id"]
        changes = store.build_changes(src, txn)

        # 幂等：目标仓若已有该来源事务，直接对齐状态不重复执行
        existing = None
        try:
            existing = target.get_commit(src, txn)
        except TargetError as exc:
            if exc.status != 404:
                raise ApiError(502, f"目标仓查询失败: {exc.message}")
        if existing is not None:
            with store.db.write() as conn:
                store.mark_published(conn, src, txn, existing["commit_id"], len(changes))
            return {"txn_id": txn, "target_commit": existing["commit_id"], "idempotent": True, "changes": changes}

        try:
            result = target.submit_commit(src, txn, nxt["commit_seq"], changes)
        except TargetError as exc:
            raise ApiError(exc.status, f"目标仓拒绝提交: {exc.message}", exc.detail)

        with store.db.write() as conn:
            store.mark_published(conn, src, txn, result["commit_id"], len(changes))
        return {
            "txn_id": txn, "target_commit": result["commit_id"],
            "idempotent": result.get("idempotent", False), "changes": changes,
        }

    r.add("POST", "/api/publish", publish)
    return r


def main():
    parser = argparse.ArgumentParser(description="CDC 配对服务")
    parser.add_argument("--host", default=os.environ.get("CDC_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PAIRING_PORT", "8072")))
    parser.add_argument("--data-dir", default=os.environ.get("CDC_DATA_DIR", "data"))
    parser.add_argument("--receiver-url", default=os.environ.get("RECEIVER_URL", "http://127.0.0.1:8071"))
    parser.add_argument("--target-url", default=os.environ.get("TARGET_URL", "http://127.0.0.1:8073"))
    parser.add_argument("--poll-interval", type=float, default=float(os.environ.get("POLL_INTERVAL", "1")))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    data = Path(args.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    db = DB(str(data / "pairing.db"))
    store = PairingStore(db)
    target = TargetClient(args.target_url)
    poller = ReceiverPoller(args.receiver_url, store, args.poll_interval)
    static = Path(__file__).parent.parent / "web"
    httpd = make_server(args.host, args.port, build_router(store, poller, target), static)
    poller.start()
    log.info("配对服务启动: http://%s:%s 消费位置=%s 接收端=%s 目标端=%s",
             args.host, args.port, store.get_checkpoint(), args.receiver_url, args.target_url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        poller.stop()
        httpd.server_close()
        db.close()


if __name__ == "__main__":
    main()
