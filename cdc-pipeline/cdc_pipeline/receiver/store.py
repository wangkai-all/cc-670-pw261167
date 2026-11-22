"""接收服务：接收网页导入的日志片段，保存投递副本并支持重排/重投。"""
from __future__ import annotations

from ..common import DB, content_hash, parse_fragment, row_to_dict
from ..httpapp import ApiError

SCHEMA = """
CREATE TABLE IF NOT EXISTS fragments (
    fragment_id      TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,
    note             TEXT,
    created_at       REAL NOT NULL,
    event_count      INTEGER NOT NULL,
    raw_content      TEXT NOT NULL,
    content_hash     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    attempt_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    fragment_id      TEXT NOT NULL REFERENCES fragments(fragment_id),
    sort_key         INTEGER NOT NULL,
    tampered         INTEGER NOT NULL DEFAULT 0,
    delivered_at     REAL NOT NULL,
    consumed         INTEGER NOT NULL DEFAULT 0,
    consumed_at      REAL
);
CREATE INDEX IF NOT EXISTS idx_deliveries_order ON deliveries(sort_key, attempt_id);
"""


class ReceiverStore:
    def __init__(self, db: DB):
        self.db = db
        db.conn.executescript(SCHEMA)

    # ---------- 导入 ----------
    def import_fragment(self, content: str, fragment_id: str | None, note: str | None) -> dict:
        import time
        import uuid

        try:
            events = parse_fragment(content)
        except ValueError as exc:
            raise ApiError(400, str(exc))
        if not events:
            raise ApiError(400, "片段为空，至少需要一个事件")
        sources = {e.get("source_id") for e in events if e.get("source_id")}
        if len(sources) > 1:
            raise ApiError(400, "一个片段内的 source_id 必须一致")
        source_id = next(iter(sources)) if sources else "UNKNOWN"
        fid = fragment_id or f"F-{uuid.uuid4().hex[:8]}"
        now = time.time()
        with self.db.write() as conn:
            exists = conn.execute("SELECT 1 FROM fragments WHERE fragment_id=?", (fid,)).fetchone()
            if exists:
                raise ApiError(409, f"片段 {fid} 已存在，请换一个编号")
            max_sort = conn.execute("SELECT COALESCE(MAX(sort_key),0) FROM deliveries").fetchone()[0]
            conn.execute(
                "INSERT INTO fragments(fragment_id,source_id,note,created_at,event_count,raw_content,content_hash)"
                " VALUES(?,?,?,?,?,?,?)",
                (fid, source_id, note, now, len(events), content, content_hash(events)),
            )
            conn.execute(
                "INSERT INTO deliveries(fragment_id,sort_key,tampered,delivered_at) VALUES(?,?,0,?)",
                (fid, max_sort + 1, now),
            )
        return self.get_fragment(fid)

    # ---------- 查询 ----------
    def list_fragments(self) -> list[dict]:
        rows = self.db.query(
            """
            SELECT f.fragment_id, f.source_id, f.note, f.created_at, f.event_count,
                   f.content_hash,
                   (SELECT MIN(sort_key) FROM deliveries d WHERE d.fragment_id=f.fragment_id) AS first_sort,
                   (SELECT COUNT(*) FROM deliveries d WHERE d.fragment_id=f.fragment_id) AS delivery_count,
                   (SELECT COUNT(*) FROM deliveries d WHERE d.fragment_id=f.fragment_id AND d.consumed=1)
                       AS consumed_count
            FROM fragments f ORDER BY first_sort, f.fragment_id
            """
        )
        out = []
        for r in rows:
            d = dict(r)
            d["deliveries"] = rows_to_dicts(
                self.db.query(
                    "SELECT attempt_id,fragment_id,sort_key,tampered,delivered_at,consumed,consumed_at "
                    "FROM deliveries WHERE fragment_id=? ORDER BY sort_key, attempt_id",
                    (d["fragment_id"],),
                )
            )
            out.append(d)
        return out

    def get_fragment(self, fragment_id: str) -> dict:
        row = self.db.query_one("SELECT * FROM fragments WHERE fragment_id=?", (fragment_id,))
        if row is None:
            raise ApiError(404, f"片段 {fragment_id} 不存在")
        d = row_to_dict(row)
        d["events"] = parse_fragment(d.pop("raw_content"))
        d["deliveries"] = rows_to_dicts(
            self.db.query(
                "SELECT attempt_id,fragment_id,sort_key,tampered,delivered_at,consumed,consumed_at "
                "FROM deliveries WHERE fragment_id=? ORDER BY sort_key, attempt_id",
                (fragment_id,),
            )
        )
        return d

    # ---------- 重投（可篡改内容用于演示同标识内容改变）----------
    def redeliver(self, fragment_id: str, tampered: bool) -> dict:
        import time

        with self.db.write() as conn:
            frag = conn.execute("SELECT * FROM fragments WHERE fragment_id=?", (fragment_id,)).fetchone()
            if frag is None:
                raise ApiError(404, f"片段 {fragment_id} 不存在")
            max_sort = conn.execute("SELECT COALESCE(MAX(sort_key),0) FROM deliveries").fetchone()[0]
            cur = conn.execute(
                "INSERT INTO deliveries(fragment_id,sort_key,tampered,delivered_at) VALUES(?,?,?,?)",
                (fragment_id, max_sort + 1, 1 if tampered else 0, time.time()),
            )
            attempt_id = cur.lastrowid
        return self._attempt(attempt_id)

    # ---------- 顺序调整 ----------
    def reorder(self, attempt_id: int, direction: str) -> dict:
        with self.db.write() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE attempt_id=?", (attempt_id,)).fetchone()
            if row is None:
                raise ApiError(404, f"投递 {attempt_id} 不存在")
            if row["consumed"]:
                raise ApiError(409, "该投递已被配对服务消费，不能再调整顺序")
            order = [
                dict(r)
                for r in conn.execute("SELECT * FROM deliveries ORDER BY sort_key, attempt_id").fetchall()
            ]
            idx = next(i for i, r in enumerate(order) if r["attempt_id"] == attempt_id)
            swap_idx = idx - 1 if direction == "up" else idx + 1
            if swap_idx < 0 or swap_idx >= len(order):
                raise ApiError(400, "已经位于端点，无法继续移动")
            if order[swap_idx]["consumed"]:
                raise ApiError(409, "相邻投递已被消费，顺序不能越过消费位置")
            a, b = order[idx]["sort_key"], order[swap_idx]["sort_key"]
            conn.execute("UPDATE deliveries SET sort_key=? WHERE attempt_id=?", (b, attempt_id))
            conn.execute("UPDATE deliveries SET sort_key=? WHERE attempt_id=?", (a, order[swap_idx]["attempt_id"]))
        return self._attempt(attempt_id)

    def _attempt(self, attempt_id: int) -> dict:
        row = self.db.query_one(
            "SELECT attempt_id,fragment_id,sort_key,tampered,delivered_at,consumed,consumed_at "
            "FROM deliveries WHERE attempt_id=?",
            (attempt_id,),
        )
        return row_to_dict(row)

    # ---------- 配对服务拉取 / 确认 ----------
    def fetch_pending(self, after_attempt_id: int) -> list[dict]:
        rows = self.db.query(
            "SELECT attempt_id,fragment_id,sort_key,tampered FROM deliveries "
            "WHERE attempt_id>? ORDER BY sort_key, attempt_id",
            (after_attempt_id,),
        )
        out = []
        for r in rows:
            frag = self.db.query_one(
                "SELECT raw_content FROM fragments WHERE fragment_id=?", (r["fragment_id"],)
            )
            events = parse_fragment(frag["raw_content"])
            if r["tampered"]:
                # 演示：重投时把事件标成“内容已改变”，由配对服务比对出差异
                for ev in events:
                    ev = _tamper(ev)
            out.append(
                {
                    "attempt_id": r["attempt_id"],
                    "fragment_id": r["fragment_id"],
                    "sort_key": r["sort_key"],
                    "tampered": bool(r["tampered"]),
                    "events": events,
                }
            )
        return out

    def mark_consumed(self, attempt_id: int):
        import time

        with self.db.write() as conn:
            conn.execute(
                "UPDATE deliveries SET consumed=1, consumed_at=? WHERE attempt_id=? AND consumed=0",
                (time.time(), attempt_id),
            )


def _tamper(ev: dict) -> dict:
    """重投但同 event_id 内容改变：对行数据做可见的字段级扰动。"""
    for slot in ("before", "after"):
        image = ev.get(slot)
        if isinstance(image, dict) and isinstance(image.get("row"), dict):
            image["row"] = dict(image["row"])
            image["row"]["_REDELIVERED_TAMPER"] = "重传内容与首次投递不一致"
    ev["_redelivered_tampered"] = True
    return ev


def rows_to_dicts(rows):
    return [dict(r) for r in rows]
