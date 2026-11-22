#!/usr/bin/env python3
"""端到端测试：启动三个独立服务，按网页同款 API 走完整业务流程并核对目标仓。

覆盖：
  1. 跨批前后像配对（T2），配对键为来源事务+语句标识
  2. 同事务多次更新保留真实先后（T1）
  3. 主键迁移发布为撤旧加新、关联表同事务可见（T3）
  4. 缺前像收到 COMMIT → 整笔异常；迟到前像补齐 → 重新核验恢复（T4）
  5. 回滚事务永不发布（T5）
  6. 已处理片段原内容重传不重复执行；同标识篡改重传报 CONTENT_MISMATCH 差异
  7. 投递顺序调整（未消费可移、已消费拒绝）
  8. 按提交序号发布、异常阻塞后续序号
  9. 配对状态与消费位置持久化，重启后不跳过未完成事务
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PORTS = {"receiver": 8071, "pairing": 8072, "target": 8073}
BASE = {name: f"http://127.0.0.1:{port}" for name, port in PORTS.items()}

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}  {detail}")


def http(method: str, url: str, payload=None, expect_error=False):
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = json.loads(exc.read().decode() or "{}")
        if expect_error:
            return exc.code, body
        raise AssertionError(f"{method} {url} 意外失败 {exc.code}: {body}")


def wait_health():
    deadline = time.time() + 20
    while time.time() < deadline:
        ok = True
        for name in ("receiver", "pairing", "target"):
            try:
                urllib.request.urlopen(f"{BASE[name]}/api/health", timeout=2).read()
            except Exception:
                ok = False
        if ok:
            return
        time.sleep(0.3)
    raise RuntimeError("服务启动超时")


def start_services(data_dir: Path, poll_interval: str = "3600") -> dict[str, subprocess.Popen]:
    env = dict(os.environ, CDC_DATA_DIR=str(data_dir), PYTHONPATH=str(ROOT))
    common = [sys.executable, "-m"]
    procs = {
        "target": subprocess.Popen(common + ["cdc_pipeline.target.service", "--port", "8073",
                                             "--data-dir", str(data_dir)], cwd=ROOT, env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
        "receiver": subprocess.Popen(common + ["cdc_pipeline.receiver.service", "--port", "8071",
                                               "--data-dir", str(data_dir)], cwd=ROOT, env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
        "pairing": subprocess.Popen(common + ["cdc_pipeline.pairing.service", "--port", "8072",
                                              "--data-dir", str(data_dir),
                                              "--receiver-url", BASE["receiver"],
                                              "--target-url", BASE["target"],
                                              "--poll-interval", poll_interval], cwd=ROOT, env=env,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
    }
    wait_health()
    return procs


def stop_services(procs):
    for p in procs.values():
        p.send_signal(signal.SIGINT)
    for p in procs.values():
        try:
            p.wait(timeout=8)
        except subprocess.TimeoutExpired:
            p.kill()


def poll():
    return http("POST", f"{BASE['pairing']}/api/poll", {})[1]


def import_fragment(fid: str, events: list[dict], note=""):
    return http("POST", f"{BASE['receiver']}/api/fragments",
                {"fragment_id": fid, "note": note,
                 "content": "\n".join(json.dumps(e, ensure_ascii=False) for e in events)})[1]


def main():
    data_dir = Path(tempfile.mkdtemp(prefix="cdc-e2e-"))
    print(f"数据目录: {data_dir}")
    procs = start_services(data_dir)
    try:
        run_flow()
        # ---- 重启恢复 ----
        print("\n[9] 服务重启：配对状态与消费位置应保留，未完成事务不跳过")
        stop_services(procs)
        procs = start_services(data_dir)
        health = http("GET", f"{BASE['pairing']}/api/health")[1]
        check("重启后消费位置持久化", health["checkpoint"] >= 10, f"checkpoint={health.get('checkpoint')}")
        txns = {t["txn_id"]: t for t in http("GET", f"{BASE['pairing']}/api/txns")[1]}
        check("重启后 T6 仍为 EXCEPTION（未跳过未完成事务）", txns["T6"]["status"] == "EXCEPTION")
        check("重启后已发布的 T4 保持 PUBLISHED", txns["T4"]["status"] == "PUBLISHED")
        check("重启后 T1/T2/T3 仍为 PUBLISHED",
              all(txns[t]["status"] == "PUBLISHED" for t in ("T1", "T2", "T3")))
        check("重启后 T5 仍为 ROLLED_BACK", txns["T5"]["status"] == "ROLLED_BACK")
        # 重启后再次轮询：已确认的投递不会被重复执行
        before = http("GET", f"{BASE['pairing']}/api/deliveries")[1]
        poll()
        after = http("GET", f"{BASE['pairing']}/api/deliveries")[1]
        check("重启轮询不产生重复投递记录", len(before) == len(after))
    finally:
        stop_services(procs)
        shutil.rmtree(data_dir, ignore_errors=True)

    print(f"\n结果: {PASS} 通过 / {FAIL} 失败")
    sys.exit(1 if FAIL else 0)


def run_flow():
    from cdc_pipeline.receiver.generator import build_fragments
    frags = {f["fragment_id"]: f for f in build_fragments()}

    # ---- 1. 分阶段导入（模拟网页逐批导入），先到 F07 ----
    print("[1] 导入 F00..F07（F08 迟到前像、F09 回滚稍后再投）")
    stage1 = ["F00-base-inserts", "F01-t1-complete", "F02-t2-before", "F03-t2-after",
              "F04-t2-end", "F05-t3-pk-change", "F06-t3-related-end", "F07-t4-missing-before"]
    for fid in stage1:
        f = frags[fid]
        import_fragment(fid, f["events"], f["note"])

    # ---- 2. 顺序调整：未消费可移动；消费后拒绝 ----
    print("[2] 投递顺序调整")
    deliveries = http("GET", f"{BASE['receiver']}/api/fragments")[1]
    first_attempt = deliveries[0]["deliveries"][0]["attempt_id"]
    st, _ = http("POST", f"{BASE['receiver']}/api/deliveries/{first_attempt}/reorder",
                 {"direction": "down"}, expect_error=True)
    check("未消费投递允许调整顺序", st == 200, f"status={st}")
    poll()
    st, body = http("POST", f"{BASE['receiver']}/api/deliveries/{first_attempt}/reorder",
                    {"direction": "up"}, expect_error=True)
    check("已消费投递调整被拒绝", st == 409, body.get("error"))

    # ---- 3. 配对视图 ----
    print("[3] 配对结果核查")
    pending = {p["stmt_id"] + "|" + p["txn_id"]: p for p in http("GET", f"{BASE['pairing']}/api/pending")[1]}
    check("T4 缺前像处于 WAITING_BEFORE",
          pending.get("S1|T4", {}).get("status") == "WAITING_BEFORE", str(pending.get("S1|T4")))
    txns = {t["txn_id"]: t for t in http("GET", f"{BASE['pairing']}/api/txns")[1]}
    check("T4 COMMIT 后整笔进入异常区", txns["T4"]["status"] == "EXCEPTION")
    check("T2 跨批前后像已配平为 COMPLETE", txns["T2"]["status"] == "COMPLETE")
    check("T3 主键迁移事务 COMPLETE", txns["T3"]["status"] == "COMPLETE")
    check("T1 事务 COMPLETE", txns["T1"]["status"] == "COMPLETE")
    check("T0 基础事务 COMPLETE", txns["T0"]["status"] == "COMPLETE")
    check("严格按提交序号：下一笔是 T0(seq=0)",
          http("GET", f"{BASE['pairing']}/api/publish/preview")[1]["next"]["txn_id"] == "T0")
    conflicts = http("GET", f"{BASE['pairing']}/api/conflicts")[1]
    check("T4 产生 INCOMPLETE_TXN 冲突记录", any(c["kind"] == "INCOMPLETE_TXN" and c["txn_id"] == "T4"
                                                  and c["resolved"] == 0 for c in conflicts))

    # T1 同事务两次更新的先后顺序
    ready = [p for p in http("GET", f"{BASE['pairing']}/api/ready")[1] if p["txn_id"] == "T1"]
    seqs = [p["seq"] for p in ready]
    check("同事务多次更新按 seq 保留真实先后", seqs == sorted(seqs) and len(seqs) == 3, str(seqs))
    check("第二次更新体现真实中间态(amount=120,status=PAID)",
          any(p["before_row"] == {"id": 100, "cust": "张三", "amount": 120, "status": "PAID"} for p in ready))

    # ---- 4. 异常阻塞发布 ----
    print("[4] 异常未解决时的发布边界")
    # T0..T3 应可连续发布；T4 异常不会出现在发布队列
    published = []
    for _ in range(4):
        r = http("POST", f"{BASE['pairing']}/api/publish", {})[1]
        published.append(r["txn_id"])
    check("按提交序号发布 T0,T1,T2,T3", published == ["T0", "T1", "T2", "T3"], str(published))
    st, body = http("POST", f"{BASE['pairing']}/api/publish", {}, expect_error=True)
    check("T4 异常时没有可发布事务（不跳过未完成事务）", st == 409, body.get("error"))

    # ---- 5. 目标仓核对 ----
    print("[5] 目标仓实际内容核对")
    rows = http("GET", f"{BASE['target']}/api/rows")[1]
    orders = {tuple(r["key_values"]): r["data"] for r in rows if r["table"] == "orders"}
    items = {tuple(r["key_values"]): r["data"] for r in rows if r["table"] == "order_items"}
    check("T1 新增订单 100 最终状态 DONE/amount=120",
          orders.get((100,)) == {"id": 100, "cust": "张三", "amount": 120, "status": "DONE"},
          str(orders.get((100,))))
    check("T2 跨批更新订单 200 为 amount=88/DONE",
          orders.get((200,)) == {"id": 200, "cust": "李四", "amount": 88, "status": "DONE"},
          str(orders.get((200,))))
    check("T3 主键迁移：旧键 300 已删除", (300,) not in orders)
    check("T3 主键迁移：新键 301 已存在",
          orders.get((301,)) == {"id": 301, "cust": "王五", "amount": 70, "status": "PAID"},
          str(orders.get((301,))))
    check("T3 关联表 order_items 同事务可见",
          items.get((301, 1)) == {"order_id": 301, "line": 1, "item": "键盘", "qty": 2},
          str(items.get((301, 1))))
    check("T4 未发布：订单 400 保持旧值 amount=60/PAID",
          orders.get((400,)) == {"id": 400, "cust": "赵六", "amount": 60, "status": "PAID"})

    audit = http("GET", f"{BASE['target']}/api/audit")[1]
    t3_audit = [a for a in audit if a["commit_id"] == 4]  # T3 是第 4 个提交
    ops = [(a["table_name"], a["op"], tuple(a["key_values"])) for a in t3_audit]
    check("T3 审计留痕：撤旧 D(300) + 加新 I(301) + 关联 I 同提交原子可见",
          ("orders", "D", (300,)) in ops and ("orders", "I", (301,)) in ops
          and ("order_items", "I", (301, 1)) in ops, str(ops))
    commits = http("GET", f"{BASE['target']}/api/commits")[1]
    check("目标仓提交数=4（T5 回滚/T4 异常均未入仓）", len(commits) == 4, str([c["txn_id"] for c in commits]))

    # ---- 6. 回滚事务 ----
    print("[6] 回滚事务 T5")
    f = frags["F09-t5-rollback"]
    import_fragment(f["fragment_id"], f["events"], f["note"])
    poll()
    txns = {t["txn_id"]: t for t in http("GET", f"{BASE['pairing']}/api/txns")[1]}
    check("T5 标记 ROLLED_BACK", txns["T5"]["status"] == "ROLLED_BACK")
    st, _ = http("POST", f"{BASE['pairing']}/api/publish", {}, expect_error=True)
    check("回滚事务不可发布", st == 409)
    rows = http("GET", f"{BASE['target']}/api/rows")[1]
    check("回滚事务的订单 500 未入仓", not any(r["table"] == "orders" and r["key_values"] == [500] for r in rows))

    # ---- 7. 重传幂等与篡改差异 ----
    print("[7] 旧片段重传")
    # 原内容重传：接收端生成新投递，配对端按“事件+槽位”幂等，不产生冲突、不新增配对
    conflicts_before = len(http("GET", f"{BASE['pairing']}/api/conflicts")[1])
    pairs_before = {p["pair_id"] for p in http("GET", f"{BASE['pairing']}/api/ready")[1]}
    r = http("POST", f"{BASE['receiver']}/api/fragments/F01-t1-complete/redeliver",
             {"tampered": False})[1]
    poll_result = poll()
    dup = [d for d in poll_result["ingested"] if d["attempt_id"] == r["attempt_id"]]
    # 投递记录存在（留痕），但其中每个事件半像都命中幂等：无冲突、无新配对
    check("已处理片段原内容重传：事件级幂等不重复执行",
          dup and dup[0]["conflicts"] == [] and dup[0]["events_processed"] == 4, str(dup))
    pairs_after = {p["pair_id"] for p in http("GET", f"{BASE['pairing']}/api/ready")[1]}
    check("重传未产生新配对记录", pairs_before == pairs_after)
    check("重传未产生新冲突",
          len(http("GET", f"{BASE['pairing']}/api/conflicts")[1]) == conflicts_before)
    target_before = http("GET", f"{BASE['target']}/api/commits")[1]
    check("重传不改变目标仓", len(target_before) == 4)

    # 篡改重传：同 event_id 内容改变 → CONTENT_MISMATCH 并指出差异
    http("POST", f"{BASE['receiver']}/api/fragments/F01-t1-complete/redeliver", {"tampered": True})
    poll()
    conflicts = http("GET", f"{BASE['pairing']}/api/conflicts")[1]
    mm = [c for c in conflicts if c["kind"] == "CONTENT_MISMATCH"]
    check("同标识内容改变被拒绝并记录 CONTENT_MISMATCH", len(mm) >= 1)
    if mm:
        diff_fields = {d["field"] for d in mm[0]["detail"]["diffs"]}
        row_diff = next((d for d in mm[0]["detail"]["diffs"] if d["field"] == "row"), None)
        check("冲突明细指出具体差异（row 内被加入篡改标记）",
              "row" in diff_fields and row_diff is not None
              and any("_REDELIVERED_TAMPER" in json.dumps(v) for v in row_diff.values()),
              str(diff_fields))
    # 被篡改副本不得影响既有配对
    ready_t1 = [p for p in http("GET", f"{BASE['pairing']}/api/ready")[1] if p["txn_id"] == "T1"]
    check("篡改副本未污染既有配对（订单 100 数据不变）",
          all(p["after_row"] is None or "_REDELIVERED_TAMPER" not in json.dumps(p["after_row"])
              for p in ready_t1))

    # ---- 8. 迟到前像补齐 → 重新核验 → 发布 → 再核对 ----
    print("[8] T4 迟到前像补齐与重新核验")
    f = frags["F08-t4-before-late"]
    import_fragment(f["fragment_id"], f["events"], f["note"])
    poll()
    txns = {t["txn_id"]: t for t in http("GET", f"{BASE['pairing']}/api/txns")[1]}
    check("迟到半像到达后自动重算，T4 恢复 COMPLETE", txns["T4"]["status"] == "COMPLETE")
    # 显式重新核验接口也应可用且无副作用
    rv = http("POST", f"{BASE['pairing']}/api/reverify", {})[1]
    check("重新核验接口可调用", "recovered" in rv)
    conflicts = http("GET", f"{BASE['pairing']}/api/conflicts")[1]
    check("原 INCOMPLETE_TXN 冲突已核销",
          all(c["resolved"] == 1 for c in conflicts if c["kind"] == "INCOMPLETE_TXN" and c["txn_id"] == "T4"))
    preview = http("GET", f"{BASE['pairing']}/api/publish/preview")[1]
    check("补齐后下一笔可发布为 T4", preview["next"]["txn_id"] == "T4")
    pub = http("POST", f"{BASE['pairing']}/api/publish", {})[1]
    check("T4 发布成功", pub["txn_id"] == "T4")

    # 目标仓最终核对
    rows = http("GET", f"{BASE['target']}/api/rows")[1]
    orders = {tuple(r["key_values"]): r["data"] for r in rows if r["table"] == "orders"}
    check("T4 补齐发布后订单 400 更新为 amount=90/DONE",
          orders.get((400,)) == {"id": 400, "cust": "赵六", "amount": 90, "status": "DONE"},
          str(orders.get((400,))))
    commits = http("GET", f"{BASE['target']}/api/commits")[1]
    check("目标仓提交数=5，提交序号保持 0..4",
          [c["commit_seq"] for c in commits] == [0, 1, 2, 3, 4])

    # 发布端幂等：直接向目标仓重复提交同一来源事务
    st, body = http("POST", f"{BASE['target']}/api/commits", {
        "source_id": "SRC1", "txn_id": "T1", "commit_seq": 1,
        "changes": [{"op": "I", "table": "orders", "key_columns": ["id"],
                     "before": None, "after": {"id": 100, "x": 1}}]
    })
    check("目标仓对来源事务幂等，重复提交不重复执行", body.get("idempotent") is True)
    rows = http("GET", f"{BASE['target']}/api/rows")[1]
    orders = {tuple(r["key_values"]): r["data"] for r in rows if r["table"] == "orders"}
    check("幂等返回后行内容未被假数据覆盖", orders[(100,)]["status"] == "DONE")

    # ---- 8.5 再造一笔异常事务 T6，用于验证重启后仍停留在异常区、不被跳过 ----
    print("[8.5] 构造重启时仍未完成的事务 T6")
    t6_after = [{
        "event_id": "E601", "source_id": "SRC1", "txn_id": "T6", "stmt_id": "S1", "seq": 1,
        "table": "orders", "op": "U", "key_columns": ["id"],
        "after": {"row": {"id": 400, "cust": "赵六", "amount": 99, "status": "X"}},
    }, {
        "event_id": "E602", "source_id": "SRC1", "txn_id": "T6", "kind": "TXN_END",
        "txn_status": "COMMIT", "commit_seq": 5,
    }]
    import_fragment("F10-t6-missing-before", t6_after, "T6 缺前像（重启恢复验证用）")
    poll()
    txns = {t["txn_id"]: t for t in http("GET", f"{BASE['pairing']}/api/txns")[1]}
    check("T6 重启前已进入异常区", txns["T6"]["status"] == "EXCEPTION")


if __name__ == "__main__":
    main()
