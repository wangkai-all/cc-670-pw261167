"""共享工具：规范化 JSON、哈希、主键计算、SQLite 封装。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable


def canonical(obj: Any) -> str:
    """确定性 JSON 序列化：键排序、无多余空白，用于内容指纹与去重。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


def key_tuple(row: dict, key_columns: Iterable[str]) -> tuple:
    return tuple(row.get(k) for k in key_columns)


def row_key_hash(table: str, row: dict, key_columns: Iterable[str]) -> str:
    return content_hash({"table": table, "key": list(key_tuple(row, key_columns))})


def parse_fragment(content: str) -> list[dict]:
    """片段支持 JSON 数组或 NDJSON（每行一个事件）。"""
    text = content.strip()
    if not text:
        return []
    if text[0] in "[{":
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = None
        if data is not None:
            if isinstance(data, list):
                return data
            return [data]
    events: list[dict] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"第 {line_no} 行不是合法 JSON: {exc.msg}") from exc
    return events


class DB:
    """带全局锁的 SQLite 封装；WAL 模式，写操作串行化保证恢复一致性。"""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")

    @contextmanager
    def write(self):
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")

    def query(self, sql: str, params: tuple | list = ()) -> list[sqlite3.Row]:
        if isinstance(params, str):
            params = (params,)
        with self.lock:
            return list(self.conn.execute(sql, params).fetchall())

    def query_one(self, sql: str, params: tuple | list = ()) -> sqlite3.Row | None:
        if isinstance(params, str):
            params = (params,)
        with self.lock:
            return self.conn.execute(sql, params).fetchone()

    def close(self):
        with self.lock:
            self.conn.close()


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict]:
    return [row_to_dict(r) for r in rows]


def row_to_dict(row: sqlite3.Row) -> dict:
    """sqlite3.Row 转 dict（避免 dict(row) 的键展开兼容性问题）。"""
    return {k: row[k] for k in row.keys()}
