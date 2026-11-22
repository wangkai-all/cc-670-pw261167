#!/usr/bin/env python3
"""进程内单元测试：配对状态机与目标仓事务语义（不起 HTTP 服务）。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cdc_pipeline.common import DB
from cdc_pipeline.httpapp import ApiError
from cdc_pipeline.pairing.store import PairingStore
from cdc_pipeline.target.store import TargetStore

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}  {detail}")


def ev(eid, txn, stmt, seq=1, table="t", op="U", before=None, after=None, src="S"):
    e = {"event_id": eid, "source_id": src, "txn_id": txn, "stmt_id": stmt,
         "seq": seq, "table": table, "op": op, "key_columns": ["id"]}
    if before is not None:
        e["before"] = {"row": before}
    if after is not None:
        e["after"] = {"row": after}
    return e


def end(eid, txn, seq=None, status="COMMIT", src="S"):
    e = {"event_id": eid, "source_id": src, "txn_id": txn, "kind": "TXN_END", "txn_status": status}
    if seq is not None:
        e["commit_seq"] = seq
    return e


def delivery(attempt, events, fragment=None):
    return {"attempt_id": attempt, "fragment_id": fragment or f"F{attempt}",
            "sort_key": attempt, "tampered": False, "events": events}


def main():
    tmp = Path(tempfile.mkdtemp(prefix="cdc-unit-"))
    db = DB(str(tmp / "pairing.db"))
    st = PairingStore(db)

    print("[1] 配对键是来源事务+语句标识，不是“按主键找最近一条”")
    # 两个不同事务更新同一主键 id=1，各自的半像不得串配
    st.ingest_delivery(delivery(1, [ev("a1", "TA", "S1", before={"id": 1, "v": 0})]))
    st.ingest_delivery(delivery(2, [ev("b1", "TB", "S1", before={"id": 1, "v": 9})]))
    st.ingest_delivery(delivery(3, [ev("b2", "TB", "S1", after={"id": 1, "v": 99})]))
    st.ingest_delivery(delivery(4, [ev("a2", "TA", "S1", after={"id": 1, "v": 1})]))
    st.ingest_delivery(delivery(41, [end("a9", "TA", seq=1), end("b9", "TB", seq=2)]))
    all_pairs = {(p["txn_id"], p["stmt_id"]): p
                 for p in (st.pending_pairs() + st.ready_changes())}
    check("TA 配到自己的前像(v=0→1)，未串到 TB",
          all_pairs[("TA", "S1")]["before_row"]["v"] == 0 and all_pairs[("TA", "S1")]["after_row"]["v"] == 1,
          str(all_pairs.get(("TA", "S1"))))
    check("TB 配到自己的前像(v=9→99)",
          all_pairs[("TB", "S1")]["before_row"]["v"] == 9 and all_pairs[("TB", "S1")]["after_row"]["v"] == 99)

    print("[2] 缺半像不得擅自当新增/删除；COMMIT 后整笔异常，补齐后恢复")
    st.ingest_delivery(delivery(5, [ev("c1", "TC", "S1", after={"id": 7, "v": 1}),
                                    end("c2", "TC", seq=1)]))
    txns = {t["txn_id"]: t for t in st.txns_overview()}
    check("TC 进入 EXCEPTION", txns["TC"]["status"] == "EXCEPTION")
    check("缺前像的 U 没有被当成 INSERT/DELETE",
          all(p["op"] == "U" for p in st.exceptions()[0]["pairs"]))
    st.ingest_delivery(delivery(6, [ev("c3", "TC", "S1", before={"id": 7, "v": 0})]))
    txns = {t["txn_id"]: t for t in st.txns_overview()}
    check("迟到前像补齐后自动恢复 COMPLETE", txns["TC"]["status"] == "COMPLETE")

    print("[3] 同 event_id 重传：同内容幂等，改内容报差异冲突")
    r1 = st.ingest_delivery(delivery(7, [ev("d1", "TD", "S1", before={"id": 2, "v": 0},
                                            after={"id": 2, "v": 1}), end("d2", "TD", seq=2)]))
    r2 = st.ingest_delivery(delivery(8, [ev("d1", "TD", "S1", before={"id": 2, "v": 0},
                                            after={"id": 2, "v": 1})]))
    check("同内容重传无冲突且幂等", r2["conflicts"] == [])
    r3 = st.ingest_delivery(delivery(9, [ev("d1", "TD", "S1", before={"id": 2, "v": 0},
                                            after={"id": 2, "v": 666})]))
    check("内容改变产生 CONTENT_MISMATCH",
          any(c["kind"] == "CONTENT_MISMATCH" for cid in r3["conflicts"]
              for c in st.conflicts() if c["conflict_id"] == cid))
    pair = next(p for p in st.ready_changes() if p["txn_id"] == "TD")
    check("被篡改副本未覆盖首次内容(v 仍为 1)", pair["after_row"]["v"] == 1)

    print("[4] 同事务重复投递同一半像被拒绝")
    st.ingest_delivery(delivery(10, [ev("e1", "TE", "S1", before={"id": 3, "v": 0})]))
    r = st.ingest_delivery(delivery(11, [ev("e2", "TE", "S1", before={"id": 3, "v": 5})]))
    check("重复前像产生 DUPLICATE_IMAGE",
          any(c["kind"] == "DUPLICATE_IMAGE" for cid in r["conflicts"]
              for c in st.conflicts() if c["conflict_id"] == cid))

    print("[5] 回滚事务永不发布")
    st.ingest_delivery(delivery(12, [ev("f1", "TF", "S1", op="I", after={"id": 9, "v": 1}),
                                     end("f2", "TF", status="ROLLBACK")]))
    txns = {t["txn_id"]: t for t in st.txns_overview()}
    check("TF 状态 ROLLED_BACK", txns["TF"]["status"] == "ROLLED_BACK")
    check("回滚事务不可构建发布变更", _expect_api_error(lambda: st.build_changes("S", "TF")))

    print("[6] 主键迁移展开为撤旧加新")
    st.ingest_delivery(delivery(13, [
        ev("g1", "TG", "S1", before={"id": 300, "v": 1}, after={"id": 301, "v": 1}),
        end("g2", "TG", seq=3),
    ]))
    changes = st.build_changes("S", "TG")
    check("变更展开为 D(300)+I(301)",
          [(c["op"], c["before"] and c["before"]["id"], c["after"] and c["after"]["id"])
           for c in changes] == [("D", 300, None), ("I", None, 301)], str(changes))
    check("非主键更新不展开",
          len(st.build_changes("S", "TD")) == 1 and st.build_changes("S", "TD")[0]["op"] == "U")

    print("[7] 目标仓：整事务原子性与来源事务幂等")
    tdb = DB(str(tmp / "target.db"))
    target = TargetStore(tdb)
    # 先铺基础行 300，迁移事务才能撤旧
    target.submit_commit("S", "TBASE", 0, [{
        "op": "I", "table": "t", "key_columns": ["id"], "before": None,
        "after": {"id": 300, "v": 1},
    }])
    ok = target.submit_commit("S", "TG", 3, changes)
    check("主键迁移提交成功（撤旧加新）", ok["commit_id"] > 0)
    again = target.submit_commit("S", "TG", 3, changes)
    check("同来源事务重复提交幂等", again["idempotent"] is True)
    bad = [
        {"op": "I", "table": "t", "key_columns": ["id"], "before": None,
         "after": {"id": 100, "v": 1}},
        {"op": "I", "table": "t", "key_columns": ["id"], "before": None,
         "after": {"id": 100, "v": 2}},  # 同键重复插入，应整笔回滚
    ]
    check("冲突变更整事务回滚抛错", _expect_api_error(lambda: target.submit_commit("S", "TX", 9, bad)))
    check("回滚后无残留提交", target.get_commit_by_ref("S", "TX") is None)
    rows = target.all_rows()
    check("回滚后无残留行（仅剩迁移后的 301）",
          [r["key_values"] for r in rows] == [[301]], str([r["key_values"] for r in rows]))

    print("[8] 畸形事件进入冲突而不是崩溃")
    r = st.ingest_delivery(delivery(14, [{"event_id": "bad", "source_id": "S", "txn_id": "TZ"}]))
    check("缺 stmt/table/op 的事件记录 MALFORMED_EVENT",
          any(c["kind"] == "MALFORMED_EVENT" for cid in r["conflicts"]
              for c in st.conflicts() if c["conflict_id"] == cid))

    db.close()
    tdb.close()
    print(f"\n结果: {PASS} 通过 / {FAIL} 失败")
    sys.exit(1 if FAIL else 0)


def _expect_api_error(fn):
    try:
        fn()
    except ApiError:
        return True
    return False


if __name__ == "__main__":
    main()
