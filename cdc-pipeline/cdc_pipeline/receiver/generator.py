"""项目自身的变更日志生成器：构造覆盖跨批前后像、主键变更、缺前像、回滚的演示片段。

事件协议（JSON Lines / JSON 数组均可）：
  普通事件: {event_id, source_id, txn_id, stmt_id, seq, table, op, key_columns, before?, after?}
  事务边界: {event_id, source_id, txn_id, kind: "TXN_END", txn_status: "COMMIT|ROLLBACK", commit_seq?}
其中 op=I 只带 after，op=D 只带 before，op=U 必须 before/after 成对（可跨批到达）。
"""
from __future__ import annotations

import json
from pathlib import Path

SRC = "SRC1"


def ev(event_id, txn_id, stmt_id, seq, table, op, key_columns, before=None, after=None):
    e = {
        "event_id": event_id,
        "source_id": SRC,
        "txn_id": txn_id,
        "stmt_id": stmt_id,
        "seq": seq,
        "table": table,
        "op": op,
        "key_columns": key_columns,
    }
    if before is not None:
        e["before"] = {"row": before}
    if after is not None:
        e["after"] = {"row": after}
    return e


def end(event_id, txn_id, commit_seq=None, status="COMMIT"):
    e = {"event_id": event_id, "source_id": SRC, "txn_id": txn_id, "kind": "TXN_END", "txn_status": status}
    if commit_seq is not None:
        e["commit_seq"] = commit_seq
    return e


def build_fragments() -> list[dict]:
    """返回 [{fragment_id, note, events:[...]}]，顺序即建议投递顺序（页面可调整）。"""
    fragments: list[dict] = []

    # T0：基础数据（后续事务更新/迁移/删除的目标行必须先存在），commit_seq=0
    t0 = [
        ev("E001", "T0", "S1", 1, "orders", "I", ["id"],
           after={"id": 200, "cust": "李四", "amount": 50, "status": "PAID"}),
        ev("E002", "T0", "S2", 2, "orders", "I", ["id"],
           after={"id": 300, "cust": "王五", "amount": 70, "status": "PAID"}),
        ev("E003", "T0", "S3", 3, "orders", "I", ["id"],
           after={"id": 400, "cust": "赵六", "amount": 60, "status": "PAID"}),
        end("E004", "T0", commit_seq=0),
    ]
    fragments.append({"fragment_id": "F00-base-inserts", "note": "T0 基础订单 200/300/400", "events": t0})

    # T1：基础新增 + 同事务普通更新（两次更新保留真实先后），commit_seq=1
    t1 = [
        ev("E101", "T1", "S1", 1, "orders", "I", ["id"],
           after={"id": 100, "cust": "张三", "amount": 100, "status": "NEW"}),
        ev("E102", "T1", "S2", 2, "orders", "U", ["id"],
           before={"id": 100, "cust": "张三", "amount": 100, "status": "NEW"},
           after={"id": 100, "cust": "张三", "amount": 120, "status": "PAID"}),
        ev("E103", "T1", "S3", 3, "orders", "U", ["id"],
           before={"id": 100, "cust": "张三", "amount": 120, "status": "PAID"},
           after={"id": 100, "cust": "张三", "amount": 120, "status": "DONE"}),
        end("E104", "T1", commit_seq=1),
    ]
    fragments.append({"fragment_id": "F01-t1-complete", "note": "T1 完整事务：新增+两次更新", "events": t1})

    # T2：跨批到达的更新 —— 前像一批、后像一批、事务结束又一批
    t2_before = [
        ev("E201", "T2", "S1", 1, "orders", "U", ["id"],
           before={"id": 200, "cust": "李四", "amount": 50, "status": "PAID"}),
    ]
    t2_after = [
        ev("E201", "T2", "S1", 1, "orders", "U", ["id"],
           after={"id": 200, "cust": "李四", "amount": 88, "status": "DONE"}),
    ]
    t2_end = [end("E202", "T2", commit_seq=2)]
    fragments.append({"fragment_id": "F02-t2-before", "note": "T2 只有更新前像（后像隔批）", "events": t2_before})
    fragments.append({"fragment_id": "F03-t2-after", "note": "T2 补到更新后像", "events": t2_after})
    fragments.append({"fragment_id": "F04-t2-end", "note": "T2 事务结束", "events": t2_end})

    # T3：主键迁移 + 关联表同事务，commit_seq=3（也拆成跨批）
    t3_a = [
        ev("E301", "T3", "S1", 1, "orders", "U", ["id"],
           before={"id": 300, "cust": "王五", "amount": 70, "status": "PAID"},
           after={"id": 301, "cust": "王五", "amount": 70, "status": "PAID"}),
    ]
    t3_b = [
        ev("E302", "T3", "S2", 2, "order_items", "I", ["order_id", "line"],
           after={"order_id": 301, "line": 1, "item": "键盘", "qty": 2}),
        end("E303", "T3", commit_seq=3),
    ]
    fragments.append({"fragment_id": "F05-t3-pk-change", "note": "T3 主键 300→301（发布应表现为撤旧加新）", "events": t3_a})
    fragments.append({"fragment_id": "F06-t3-related-end", "note": "T3 关联表 order_items 同事务+结束", "events": t3_b})

    # T4：COMMIT 时仍缺前像 → 整笔异常；之后补齐可重新核验，commit_seq=4
    t4_after = [
        ev("E401", "T4", "S1", 1, "orders", "U", ["id"],
           after={"id": 400, "cust": "赵六", "amount": 90, "status": "DONE"}),
        end("E402", "T4", commit_seq=4),
    ]
    t4_before_late = [
        ev("E401", "T4", "S1", 1, "orders", "U", ["id"],
           before={"id": 400, "cust": "赵六", "amount": 60, "status": "PAID"}),
    ]
    fragments.append({"fragment_id": "F07-t4-missing-before", "note": "T4 缺前像就收到事务结束 → 异常区", "events": t4_after})
    fragments.append({"fragment_id": "F08-t4-before-late", "note": "T4 迟到的前像（补齐后重新核验）", "events": t4_before_late})

    # T5：回滚事务，任何状态都不得发布
    t5 = [
        ev("E501", "T5", "S1", 1, "orders", "I", ["id"],
           after={"id": 500, "cust": "孙七", "amount": 1, "status": "NEW"}),
        end("E502", "T5", status="ROLLBACK"),
    ]
    fragments.append({"fragment_id": "F09-t5-rollback", "note": "T5 回滚（永远不发布）", "events": t5})

    return fragments


def to_jsonl(events: list[dict]) -> str:
    return "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"


def write_demo_dir(out_dir: str | Path) -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for frag in build_fragments():
        p = out / f"{frag['fragment_id']}.jsonl"
        p.write_text(to_jsonl(frag["events"]), encoding="utf-8")
        paths.append(p)
    return paths
