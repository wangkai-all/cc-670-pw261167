#!/usr/bin/env python3
"""手工分步演示（与页面操作完全同款的 API 调用）。"""
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cdc_pipeline.receiver.generator import build_fragments

B = {"r": "http://127.0.0.1:8071", "p": "http://127.0.0.1:8072", "t": "http://127.0.0.1:8073"}


def call(svc, method, path, payload=None):
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(B[svc] + path, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def import_frag(fid, frags):
    f = frags[fid]
    return call("r", "POST", "/api/fragments", {
        "fragment_id": fid, "note": f["note"],
        "content": "\n".join(json.dumps(e, ensure_ascii=False) for e in f["events"]),
    })


def main():
    frags = {f["fragment_id"]: f for f in build_fragments()}

    print("=== 阶段一：导入 F00..F07（F08 迟到前像、F09 回滚暂不投），手动拉取 ===")
    for fid in ["F00-base-inserts", "F01-t1-complete", "F02-t2-before", "F03-t2-after",
                "F04-t2-end", "F05-t3-pk-change", "F06-t3-related-end", "F07-t4-missing-before"]:
        import_frag(fid, frags)
    call("p", "POST", "/api/poll", {})
    for t in call("p", "GET", "/api/txns")[1]:
        print(f"  {t['txn_id']}: {t['status']}  seq={t['commit_seq']}")
    print("  异常区:", [x["txn_id"] for x in call("p", "GET", "/api/exceptions")[1]])
    print("  待配对:", [(p["txn_id"], p["stmt_id"], p["status"]) for p in call("p", "GET", "/api/pending")[1]])
    print("  冲突:", [(c["kind"], c["txn_id"]) for c in call("p", "GET", "/api/conflicts")[1]])

    print("\n=== 阶段二：按提交序号发布 T0..T3；T4 异常挡住发布 ===")
    for _ in range(4):
        st, r = call("p", "POST", "/api/publish", {})
        print(f"  发布 -> {st} {r.get('txn_id')} 目标提交#{r.get('target_commit')} 变更{len(r.get('changes', []))}条")
    st, r = call("p", "POST", "/api/publish", {})
    print(f"  再发布被拒 -> {st} {r.get('error')}")

    print("\n=== 阶段三：主键迁移在目标仓的实际表现（T3 撤旧加新 + 关联表同事务）===")
    for row in call("t", "GET", "/api/rows")[1]:
        print(f"  {row['table']:<12} 键={row['key_values']} 数据={row['data']}")

    print("\n=== 阶段四：F01 原内容重传（幂等）与篡改重传（CONTENT_MISMATCH 差异）===")
    call("r", "POST", "/api/fragments/F01-t1-complete/redeliver", {"tampered": False})
    call("p", "POST", "/api/poll", {})
    print("  原内容重传后冲突数:", len(call("p", "GET", "/api/conflicts")[1]))
    call("r", "POST", "/api/fragments/F01-t1-complete/redeliver", {"tampered": True})
    call("p", "POST", "/api/poll", {})
    mm = [c for c in call("p", "GET", "/api/conflicts")[1] if c["kind"] == "CONTENT_MISMATCH"]
    print("  篡改重传冲突:", [(c["kind"], c["event_id"]) for c in mm])
    print("  差异明细:", json.dumps(mm[0]["detail"], ensure_ascii=False))

    print("\n=== 阶段五：迟到前像 F08 到达 → 重新核验 → T4 恢复并发布 ===")
    import_frag("F08-t4-before-late", frags)
    call("p", "POST", "/api/poll", {})
    print("  补齐后 T4:", [t for t in call("p", "GET", "/api/txns")[1] if t["txn_id"] == "T4"][0]["status"])
    print("  重新核验:", call("p", "POST", "/api/reverify", {})[1])
    st, r = call("p", "POST", "/api/publish", {})
    print(f"  T4 发布 -> {st} {r.get('txn_id')} 变更{len(r.get('changes', []))}条")

    print("\n=== 阶段六：F09 回滚事务永不发布 ===")
    import_frag("F09-t5-rollback", frags)
    call("p", "POST", "/api/poll", {})
    print("  T5:", [t for t in call("p", "GET", "/api/txns")[1] if t["txn_id"] == "T5"][0]["status"])
    st, r = call("p", "POST", "/api/publish", {})
    print(f"  发布被拒 -> {st} {r.get('error')}")

    print("\n=== 最终核对：目标仓行 / 提交 / 审计 ===")
    print("  当前行:")
    for row in call("t", "GET", "/api/rows")[1]:
        print(f"    {row['table']:<12} 键={row['key_values']} 数据={row['data']}")
    print("  提交记录:", [(c["txn_id"], c["commit_seq"], c["change_count"]) for c in call("t", "GET", "/api/commits")[1]])
    print("  审计留痕:")
    for a in call("t", "GET", "/api/audit")[1]:
        print(f"    提交#{a['commit_id']} {a['table_name']:<12} {a['op']} 键={a['key_values']}")


if __name__ == "__main__":
    main()
