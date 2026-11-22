"""配对服务存储：事件配对、事务状态机、冲突/异常、发布。"""
from __future__ import annotations

import json
import time

from ..common import DB, canonical, content_hash, key_tuple, row_to_dict
from ..httpapp import ApiError

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT
);
-- 事件半像按“事件标识 + 槽位(before/after/full)”存储：
-- 同一 event_id 的前像/后像允许跨批到达，但同一槽位重复或内容不同都会被识别。
CREATE TABLE IF NOT EXISTS event_images (
    image_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    txn_id          TEXT NOT NULL,
    stmt_id         TEXT NOT NULL,
    slot            TEXT NOT NULL,
    seq             INTEGER,
    table_name      TEXT,
    op              TEXT,
    key_columns     TEXT,
    row_data        TEXT,
    payload_hash    TEXT NOT NULL,
    first_seen_at   REAL NOT NULL,
    UNIQUE(source_id, txn_id, event_id, slot)
);
CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id      INTEGER PRIMARY KEY,
    fragment_id     TEXT NOT NULL,
    sort_key        INTEGER NOT NULL,
    tampered        INTEGER NOT NULL,
    received_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pairs (
    pair_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    txn_id          TEXT NOT NULL,
    stmt_id         TEXT NOT NULL,
    seq             INTEGER,
    table_name      TEXT NOT NULL,
    op              TEXT NOT NULL,
    key_columns     TEXT NOT NULL,
    before_row      TEXT,
    after_row       TEXT,
    before_event_id TEXT,
    after_event_id  TEXT,
    status          TEXT NOT NULL,
    updated_at      REAL NOT NULL,
    UNIQUE(source_id, txn_id, stmt_id)
);
CREATE TABLE IF NOT EXISTS txns (
    source_id       TEXT NOT NULL,
    txn_id          TEXT NOT NULL,
    txn_status      TEXT NOT NULL,
    commit_seq      INTEGER,
    end_event_id    TEXT,
    status          TEXT NOT NULL,
    exception_reason TEXT,
    published_commit INTEGER,
    updated_at      REAL NOT NULL,
    PRIMARY KEY (source_id, txn_id)
);
CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      REAL NOT NULL,
    kind            TEXT NOT NULL,
    source_id       TEXT,
    txn_id          TEXT,
    event_id        TEXT,
    attempt_id      INTEGER,
    fragment_id     TEXT,
    message         TEXT NOT NULL,
    detail          TEXT,
    resolved        INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS publish_log (
    publish_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    txn_id          TEXT NOT NULL,
    commit_seq      INTEGER,
    target_commit   INTEGER NOT NULL,
    change_count    INTEGER NOT NULL,
    published_at    REAL NOT NULL,
    UNIQUE(source_id, txn_id)
);
"""

REQUIRED_FIELDS = ("event_id", "source_id", "txn_id")


class PairingStore:
    def __init__(self, db: DB):
        self.db = db
        db.conn.executescript(SCHEMA)

    # ---------------- 消费位置 ----------------
    def get_checkpoint(self) -> int:
        row = self.db.query_one("SELECT v FROM meta WHERE k='last_attempt_id'")
        return int(row["v"]) if row else 0

    def _set_checkpoint(self, conn, attempt_id: int):
        conn.execute(
            "INSERT INTO meta(k,v) VALUES('last_attempt_id',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (str(attempt_id),),
        )

    # ---------------- 接收一个投递批次 ----------------
    def ingest_delivery(self, delivery: dict) -> dict:
        attempt_id = int(delivery["attempt_id"])
        fragment_id = delivery["fragment_id"]
        sort_key = int(delivery["sort_key"])
        tampered = bool(delivery.get("tampered"))
        now = time.time()

        duplicate = self.db.query_one(
            "SELECT 1 FROM delivery_attempts WHERE attempt_id=?", (attempt_id,)
        )
        if duplicate:
            # 重传的已处理片段：不重复执行
            return {"attempt_id": attempt_id, "duplicate": True, "events_processed": 0, "conflicts": []}

        conflict_ids: list[int] = []
        with self.db.write() as conn:
            conn.execute(
                "INSERT INTO delivery_attempts(attempt_id,fragment_id,sort_key,tampered,received_at)"
                " VALUES(?,?,?,?,?)",
                (attempt_id, fragment_id, sort_key, 1 if tampered else 0, now),
            )
            for raw in delivery.get("events", []):
                cid = self._ingest_event(conn, raw, attempt_id, fragment_id)
                if cid is not None:
                    conflict_ids.append(cid)
            # 事件落库后重算受影响事务
            affected = self._affected_txns(conn, delivery.get("events", []))
            for key in affected:
                self._recompute_txn(conn, *key)
            self._set_checkpoint(conn, attempt_id)
        return {
            "attempt_id": attempt_id,
            "duplicate": False,
            "events_processed": len(delivery.get("events", [])),
            "conflicts": conflict_ids,
        }

    def _affected_txns(self, conn, events):
        keys = set()
        for e in events:
            if all(k in e for k in REQUIRED_FIELDS):
                keys.add((e["source_id"], e["txn_id"]))
        return keys

    # ---------------- 单事件处理 ----------------
    def _ingest_event(self, conn, ev: dict, attempt_id: int, fragment_id: str) -> int | None:
        now = time.time()
        for f in REQUIRED_FIELDS:
            if not ev.get(f):
                return self._conflict(
                    conn, "MALFORMED_EVENT", None, None, ev.get("event_id"), attempt_id, fragment_id,
                    f"事件缺少必填字段 {f}", ev,
                )

        eid, src, txn = ev["event_id"], ev["source_id"], ev["txn_id"]

        if ev.get("kind") == "TXN_END":
            return self._ingest_txn_end(conn, ev, attempt_id, fragment_id, now)

        stmt = ev.get("stmt_id")
        table = ev.get("table")
        op = ev.get("op")
        if not stmt or not table or op not in ("I", "U", "D"):
            return self._conflict(
                conn, "MALFORMED_EVENT", src, txn, eid, attempt_id, fragment_id,
                "数据事件缺少 stmt_id/table 或 op 非 I/U/D", ev,
            )
        key_columns = ev.get("key_columns") or []
        before_row = (ev.get("before") or {}).get("row")
        after_row = (ev.get("after") or {}).get("row")

        # 同一事件里的每个半像槽位分别去重 / 比对
        slots = []
        if before_row is not None:
            slots.append(("before", before_row))
        if after_row is not None:
            slots.append(("after", after_row))
        if not slots:
            return self._conflict(
                conn, "MALFORMED_EVENT", src, txn, eid, attempt_id, fragment_id,
                "数据事件必须至少携带 before 或 after 之一", ev,
            )
        conflict_id = None
        for slot, row in slots:
            cid = self._ingest_image(
                conn, ev, slot, row, eid, src, txn, stmt, table, op, key_columns,
                attempt_id, fragment_id, now,
            )
            conflict_id = conflict_id or cid
        return conflict_id

    def _ingest_image(self, conn, ev, slot, row, eid, src, txn, stmt, table, op,
                      key_columns, attempt_id, fragment_id, now) -> int | None:
        payload = {
            "event_id": eid, "slot": slot, "seq": ev.get("seq"), "table": table,
            "op": op, "key_columns": key_columns, "row": row,
        }
        phash = content_hash(payload)
        existing = conn.execute(
            "SELECT * FROM event_images WHERE source_id=? AND txn_id=? AND event_id=? AND slot=?",
            (src, txn, eid, slot),
        ).fetchone()
        if existing is not None:
            if existing["payload_hash"] != phash:
                # 同事件同槽位内容改变：指出差异，拒绝该副本
                diff = _json_diff(
                    {"row": json.loads(existing["row_data"]), "seq": existing["seq"],
                     "table": existing["table_name"], "op": existing["op"],
                     "key_columns": json.loads(existing["key_columns"])},
                    {"row": row, "seq": ev.get("seq"), "table": table, "op": op,
                     "key_columns": key_columns},
                )
                return self._conflict(
                    conn, "CONTENT_MISMATCH", src, txn, eid, attempt_id, fragment_id,
                    f"事件 {eid} 的 {slot} 半像重传内容与首次投递不一致", diff,
                )
            return None  # 幂等：同事件同槽位同内容，直接跳过

        conn.execute(
            "INSERT INTO event_images(event_id,source_id,txn_id,stmt_id,slot,seq,table_name,op,"
            "key_columns,row_data,payload_hash,first_seen_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (eid, src, txn, stmt, slot, ev.get("seq"), table, op, json.dumps(key_columns),
             json.dumps(row, ensure_ascii=False), phash, now),
        )

        # 配对键 = 来源事务 + 语句标识（绝不只按主键找最近一条）
        pair = conn.execute(
            "SELECT * FROM pairs WHERE source_id=? AND txn_id=? AND stmt_id=?", (src, txn, stmt)
        ).fetchone()
        if pair is None:
            conn.execute(
                "INSERT INTO pairs(source_id,txn_id,stmt_id,seq,table_name,op,key_columns,"
                "before_row,after_row,before_event_id,after_event_id,status,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (src, txn, stmt, ev.get("seq"), table, op, json.dumps(key_columns),
                 json.dumps(row, ensure_ascii=False) if slot == "before" else None,
                 json.dumps(row, ensure_ascii=False) if slot == "after" else None,
                 eid if slot == "before" else None,
                 eid if slot == "after" else None,
                 "WAITING", now),
            )
            return None

        if pair["table_name"] != table or pair["op"] != op:
            return self._conflict(
                conn, "PAIR_MISMATCH", src, txn, eid, attempt_id, fragment_id,
                f"语句 {stmt} 的 table/op 与既有配对不一致",
                {"existing": {"table": pair["table_name"], "op": pair["op"]},
                 "incoming": {"table": table, "op": op}},
            )
        cols = set(json.loads(pair["key_columns"])) | set(key_columns)
        conn.execute("UPDATE pairs SET key_columns=?, updated_at=? WHERE pair_id=?",
                     (json.dumps(sorted(cols)), now, pair["pair_id"]))
        if slot == "before":
            if pair["before_row"] is not None:
                return self._conflict(
                    conn, "DUPLICATE_IMAGE", src, txn, eid, attempt_id, fragment_id,
                    f"语句 {stmt} 的前像已存在，不能重复投递前像",
                    {"existing": json.loads(pair["before_row"]), "incoming": row},
                )
            conn.execute(
                "UPDATE pairs SET before_row=?, before_event_id=?, seq=COALESCE(seq,?), updated_at=?"
                " WHERE pair_id=?",
                (json.dumps(row, ensure_ascii=False), eid, ev.get("seq"), now, pair["pair_id"]),
            )
        else:
            if pair["after_row"] is not None:
                return self._conflict(
                    conn, "DUPLICATE_IMAGE", src, txn, eid, attempt_id, fragment_id,
                    f"语句 {stmt} 的后像已存在，不能重复投递后像",
                    {"existing": json.loads(pair["after_row"]), "incoming": row},
                )
            conn.execute(
                "UPDATE pairs SET after_row=?, after_event_id=?, seq=COALESCE(seq,?), updated_at=?"
                " WHERE pair_id=?",
                (json.dumps(row, ensure_ascii=False), eid, ev.get("seq"), now, pair["pair_id"]),
            )
        return None

    def _ingest_txn_end(self, conn, ev, attempt_id, fragment_id, now) -> int | None:
        src, txn = ev["source_id"], ev["txn_id"]
        status = ev.get("txn_status", "COMMIT")
        if status not in ("COMMIT", "ROLLBACK"):
            return self._conflict(
                conn, "MALFORMED_EVENT", src, txn, ev["event_id"], attempt_id, fragment_id,
                f"txn_status 非法: {status}", ev,
            )
        existing = conn.execute("SELECT * FROM txns WHERE source_id=? AND txn_id=?", (src, txn)).fetchone()
        if existing is not None:
            if existing["txn_status"] != status or existing["commit_seq"] != ev.get("commit_seq"):
                return self._conflict(
                    conn, "CONTENT_MISMATCH", src, txn, ev["event_id"], attempt_id, fragment_id,
                    f"事务 {txn} 的结束标记重传内容不一致",
                    {"existing": {"txn_status": existing["txn_status"], "commit_seq": existing["commit_seq"]},
                     "incoming": {"txn_status": status, "commit_seq": ev.get("commit_seq")}},
                )
            return None
        conn.execute(
            "INSERT INTO txns(source_id,txn_id,txn_status,commit_seq,end_event_id,status,"
            "exception_reason,published_commit,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (src, txn, status, ev.get("commit_seq"), ev["event_id"], "OPEN", None, None, now),
        )
        return None

    # ---------------- 事务状态重算 ----------------
    def _recompute_txn(self, conn, src: str, txn: str):
        txn_row = conn.execute("SELECT * FROM txns WHERE source_id=? AND txn_id=?", (src, txn)).fetchone()
        now = time.time()
        pairs = conn.execute(
            "SELECT * FROM pairs WHERE source_id=? AND txn_id=? ORDER BY COALESCE(seq, pair_id)", (src, txn)
        ).fetchall()

        for p in pairs:
            conn.execute("UPDATE pairs SET status=?, updated_at=? WHERE pair_id=?",
                         (_pair_status(p), now, p["pair_id"]))

        if txn_row is None:
            return  # 还没收到事务结束，保持开放等待
        if txn_row["txn_status"] == "ROLLBACK":
            conn.execute("UPDATE txns SET status='ROLLED_BACK', exception_reason=NULL, updated_at=?"
                         " WHERE source_id=? AND txn_id=?", (now, src, txn))
            return
        if txn_row["published_commit"] is not None:
            return  # 已发布不动

        incomplete = [
            {"stmt_id": p["stmt_id"], "table": p["table_name"], "op": p["op"], "status": _pair_status(p)}
            for p in pairs if _pair_status(p) != "READY"
        ]
        if incomplete:
            # 收到明确事务结束后仍缺失：整笔进入异常区，不擅自当新增/删除
            conn.execute(
                "UPDATE txns SET status='EXCEPTION', exception_reason=?, updated_at=? WHERE source_id=? AND txn_id=?",
                ("存在未配平的语句: " + canonical(incomplete), now, src, txn),
            )
            self._conflict(
                conn, "INCOMPLETE_TXN", src, txn, txn_row["end_event_id"], None, None,
                f"事务 {txn} 已 COMMIT 但存在缺失半像，整笔进入异常区", incomplete,
            )
        else:
            prev_exception = txn_row["status"] == "EXCEPTION"
            conn.execute(
                "UPDATE txns SET status='COMPLETE', exception_reason=NULL, updated_at=? WHERE source_id=? AND txn_id=?",
                (now, src, txn),
            )
            if prev_exception:
                # 迟到半像补齐后恢复：旧的“事务不完整”冲突同步核销
                conn.execute(
                    "UPDATE conflicts SET resolved=1 WHERE kind='INCOMPLETE_TXN' AND source_id=? AND txn_id=?",
                    (src, txn),
                )

    # ---------------- 重新核验（迟到半像补齐后）----------------
    def reverify_all(self) -> dict:
        rows = self.db.query("SELECT source_id, txn_id FROM txns WHERE status='EXCEPTION'")
        recovered = []
        with self.db.write() as conn:
            for r in rows:
                before = conn.execute(
                    "SELECT status, exception_reason FROM txns WHERE source_id=? AND txn_id=?",
                    (r["source_id"], r["txn_id"]),
                ).fetchone()
                self._recompute_txn(conn, r["source_id"], r["txn_id"])
                after = conn.execute(
                    "SELECT status FROM txns WHERE source_id=? AND txn_id=?",
                    (r["source_id"], r["txn_id"]),
                ).fetchone()
                if after["status"] == "COMPLETE" and before["status"] == "EXCEPTION":
                    recovered.append({"txn_id": r["txn_id"], "source_id": r["source_id"]})
                    conn.execute(
                        "UPDATE conflicts SET resolved=1 WHERE kind='INCOMPLETE_TXN' AND txn_id=? AND source_id=?",
                        (r["txn_id"], r["source_id"]),
                    )
        return {"recovered": recovered}

    # ---------------- 查询视图 ----------------
    def pending_pairs(self) -> list[dict]:
        rows = self.db.query(
            """SELECT p.*, t.status AS txn_state, t.commit_seq
               FROM pairs p LEFT JOIN txns t ON t.source_id=p.source_id AND t.txn_id=p.txn_id
               WHERE p.status <> 'READY'
               ORDER BY p.source_id, p.txn_id, COALESCE(p.seq,p.pair_id)"""
        )
        return [_pair_dict(r) for r in rows]

    def ready_changes(self) -> list[dict]:
        rows = self.db.query(
            """SELECT p.*, t.status AS txn_state, t.commit_seq
               FROM pairs p JOIN txns t ON t.source_id=p.source_id AND t.txn_id=p.txn_id
               WHERE t.status='COMPLETE'
               ORDER BY t.commit_seq, p.source_id, p.txn_id, COALESCE(p.seq,p.pair_id)"""
        )
        return [_pair_dict(r) for r in rows]

    def exceptions(self) -> list[dict]:
        out = []
        for r in self.db.query("SELECT * FROM txns WHERE status='EXCEPTION' ORDER BY source_id, txn_id"):
            d = dict(r)
            d["pairs"] = [
                _pair_dict(p) for p in self.db.query(
                    "SELECT p.*, t.status AS txn_state, t.commit_seq FROM pairs p "
                    "JOIN txns t ON t.source_id=p.source_id AND t.txn_id=p.txn_id "
                    "WHERE p.source_id=? AND p.txn_id=? ORDER BY COALESCE(p.seq,p.pair_id)",
                    (r["source_id"], r["txn_id"]),
                )
            ]
            out.append(d)
        return out

    def conflicts(self, include_resolved: bool = False) -> list[dict]:
        sql = "SELECT * FROM conflicts"
        if not include_resolved:
            sql += " WHERE resolved=0"
        sql += " ORDER BY conflict_id DESC"
        return [_conflict_dict(r) for r in self.db.query(sql)]

    def txns_overview(self) -> list[dict]:
        return [dict(r) for r in self.db.query(
            "SELECT source_id,txn_id,txn_status,commit_seq,status,exception_reason,published_commit,updated_at "
            "FROM txns ORDER BY COALESCE(commit_seq, 999999), source_id, txn_id"
        )]

    def delivery_log(self) -> list[dict]:
        return [dict(r) for r in self.db.query(
            "SELECT * FROM delivery_attempts ORDER BY attempt_id"
        )]

    # ---------------- 发布 ----------------
    def next_publishable(self) -> dict | None:
        row = self.db.query_one(
            """SELECT * FROM txns WHERE status='COMPLETE'
               ORDER BY COALESCE(commit_seq, 999999999), source_id, txn_id LIMIT 1"""
        )
        return row_to_dict(row) if row else None

    def build_changes(self, src: str, txn: str) -> list[dict]:
        txn_row = self.db.query_one("SELECT * FROM txns WHERE source_id=? AND txn_id=?", (src, txn))
        if txn_row is None or txn_row["status"] != "COMPLETE":
            raise ApiError(409, f"事务 {src}/{txn} 当前不可发布")
        changes = []
        for p in self.db.query(
            "SELECT * FROM pairs WHERE source_id=? AND txn_id=? ORDER BY COALESCE(seq,pair_id)", (src, txn)
        ):
            key_columns = json.loads(p["key_columns"])
            before = json.loads(p["before_row"]) if p["before_row"] else None
            after = json.loads(p["after_row"]) if p["after_row"] else None
            op = p["op"]
            # 主键迁移：更新且主键值变化 → 撤旧加新
            if op == "U" and key_tuple(before, key_columns) != key_tuple(after, key_columns):
                changes.append({"op": "D", "table": p["table_name"], "key_columns": key_columns,
                                "before": before, "after": None, "stmt_id": p["stmt_id"], "key_moved": True})
                changes.append({"op": "I", "table": p["table_name"], "key_columns": key_columns,
                                "before": None, "after": after, "stmt_id": p["stmt_id"], "key_moved": True})
            else:
                changes.append({"op": op, "table": p["table_name"], "key_columns": key_columns,
                                "before": before, "after": after, "stmt_id": p["stmt_id"], "key_moved": False})
        return changes

    def mark_published(self, conn, src: str, txn: str, target_commit: int, change_count: int):
        now = time.time()
        conn.execute(
            "INSERT INTO publish_log(source_id,txn_id,commit_seq,target_commit,change_count,published_at)"
            " VALUES(?,?,(SELECT commit_seq FROM txns WHERE source_id=? AND txn_id=?),?,?,?)",
            (src, txn, src, txn, target_commit, change_count, now),
        )
        conn.execute(
            "UPDATE txns SET status='PUBLISHED', published_commit=?, updated_at=? WHERE source_id=? AND txn_id=?",
            (target_commit, now, src, txn),
        )

    def publish_log(self) -> list[dict]:
        return [dict(r) for r in self.db.query("SELECT * FROM publish_log ORDER BY publish_id")]

    def _conflict(self, conn, kind, src, txn, eid, attempt_id, fragment_id, message, detail) -> int:
        cur = conn.execute(
            "INSERT INTO conflicts(created_at,kind,source_id,txn_id,event_id,attempt_id,fragment_id,"
            "message,detail) VALUES(?,?,?,?,?,?,?,?,?)",
            (time.time(), kind, src, txn, eid, attempt_id, fragment_id, message,
             json.dumps(detail, ensure_ascii=False, default=str)),
        )
        return cur.lastrowid


# ---------------- 辅助函数 ----------------
def _pair_status(p) -> str:
    op = p["op"]
    has_before = p["before_row"] is not None
    has_after = p["after_row"] is not None
    if op == "I":
        return "READY" if has_after else "WAITING_AFTER"
    if op == "D":
        return "READY" if has_before else "WAITING_BEFORE"
    if has_before and has_after:
        return "READY"
    if has_before:
        return "WAITING_AFTER"
    if has_after:
        return "WAITING_BEFORE"
    return "WAITING"


def _pair_dict(r) -> dict:
    d = dict(r)
    d["key_columns"] = json.loads(d["key_columns"]) if d["key_columns"] else []
    d["before_row"] = json.loads(d["before_row"]) if d["before_row"] else None
    d["after_row"] = json.loads(d["after_row"]) if d["after_row"] else None
    return d


def _conflict_dict(r) -> dict:
    d = dict(r)
    try:
        d["detail"] = json.loads(d["detail"]) if d["detail"] else None
    except json.JSONDecodeError:
        pass
    return d


def _json_diff(a: dict, b: dict) -> dict:
    keys = sorted(set(a) | set(b))
    diffs = []
    for k in keys:
        if a.get(k) != b.get(k):
            diffs.append({"field": k, "first_seen": a.get(k), "redelivered": b.get(k)})
    return {"diffs": diffs}
