"""SQLite 持久层：WAL 模式，短事务，所有关键状态变更在事务内完成。"""
from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any, Iterable

from . import config

_local = threading.local()

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    published_at    TEXT,
    row_count       INTEGER NOT NULL DEFAULT 0,
    current_path    TEXT,
    -- rewriting：撤回正在重写本批，禁止下载；published：可下载
    status          TEXT NOT NULL DEFAULT 'published'
                    CHECK (status IN ('published', 'rewriting', 'failed'))
);

CREATE TABLE IF NOT EXISTS batch_subjects (
    batch_id    INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    subject_id  TEXT NOT NULL,
    PRIMARY KEY (batch_id, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_subjects_subject ON batch_subjects(subject_id);

CREATE TABLE IF NOT EXISTS imports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'staging'
                    CHECK (status IN ('staging', 'published', 'rejected')),
    path            TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    batch_id        INTEGER REFERENCES batches(id) ON DELETE SET NULL,
    note            TEXT
);
CREATE INDEX IF NOT EXISTS idx_imports_status ON imports(status);

CREATE TABLE IF NOT EXISTS withdrawals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id      TEXT NOT NULL UNIQUE,
    requested_at    TEXT NOT NULL,
    -- accepted：已受理（此后永久拒收）；processing：清理中；completed：全部完成
    status          TEXT NOT NULL
                    CHECK (status IN ('accepted', 'processing', 'completed', 'failed')),
    batches_total   INTEGER NOT NULL DEFAULT 0,
    batches_done    INTEGER NOT NULL DEFAULT 0,
    files_removed   INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS withdrawal_batches (
    withdrawal_id   INTEGER NOT NULL REFERENCES withdrawals(id) ON DELETE CASCADE,
    batch_id        INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'rewriting', 'done')),
    PRIMARY KEY (withdrawal_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_wb_state ON withdrawal_batches(state);

-- 日统计作业：受理即冻结来源批次与已撤回者，后续导入不参与本次统计
CREATE TABLE IF NOT EXISTS daily_stats_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    start_date      TEXT NOT NULL,   -- YYYY-MM-DD（含）
    end_date        TEXT NOT NULL,   -- YYYY-MM-DD（含）
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    -- pending：已受理待计算；processing：计算中；failed：冲突/错误，无任何结果供数；
    -- ready：结果与来源依据已原子发布；invalidated：供数资格被撤回永久停供
    status          TEXT NOT NULL
                    CHECK (status IN ('pending', 'processing', 'failed', 'ready', 'invalidated')),
    source_row_count INTEGER NOT NULL DEFAULT 0,
    result_path     TEXT,            -- ready 时的结果 Parquet（失效后删除并清空）
    error_reason    TEXT
);
CREATE INDEX IF NOT EXISTS idx_dsj_status ON daily_stats_jobs(status);

-- 冻结的来源批次（受理时刻已发布批次的快照），即"来源依据"
CREATE TABLE IF NOT EXISTS daily_stats_sources (
    job_id          INTEGER NOT NULL REFERENCES daily_stats_jobs(id) ON DELETE CASCADE,
    batch_id        INTEGER NOT NULL,
    frozen_path     TEXT NOT NULL,   -- 受理时刻批次 current_path（如 v1.parquet）
    file_size       INTEGER NOT NULL,
    mtime_ns        INTEGER NOT NULL,
    PRIMARY KEY (job_id, batch_id)
);

-- 受理时刻已受理撤回的受试者：本次统计排除，且永久不得进入结果
CREATE TABLE IF NOT EXISTS daily_stats_excluded (
    job_id          INTEGER NOT NULL REFERENCES daily_stats_jobs(id) ON DELETE CASCADE,
    subject_id      TEXT NOT NULL,
    PRIMARY KEY (job_id, subject_id)
);

-- 作业覆盖的受试者（基于冻结来源文件构建），用于撤回时精确判定受影响作业
CREATE TABLE IF NOT EXISTS daily_stats_job_subjects (
    job_id          INTEGER NOT NULL REFERENCES daily_stats_jobs(id) ON DELETE CASCADE,
    subject_id      TEXT NOT NULL,
    PRIMARY KEY (job_id, subject_id)
);
CREATE INDEX IF NOT EXISTS idx_dsjs_subject ON daily_stats_job_subjects(subject_id);

-- 按日统计结果（与作业同事务发布，失败不留半份）
CREATE TABLE IF NOT EXISTS daily_stats_results (
    job_id          INTEGER NOT NULL REFERENCES daily_stats_jobs(id) ON DELETE CASCADE,
    stat_date       TEXT NOT NULL,
    person_count    INTEGER NOT NULL,
    total_steps     INTEGER NOT NULL,
    ge_8000_count   INTEGER NOT NULL,
    PRIMARY KEY (job_id, stat_date)
);

-- 同人同日异值冲突（整次失败的明细：哪些受试者/日期/值来自哪些批次）
CREATE TABLE IF NOT EXISTS daily_stats_conflicts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL REFERENCES daily_stats_jobs(id) ON DELETE CASCADE,
    subject_id      TEXT NOT NULL,
    stat_date       TEXT NOT NULL,
    detail          TEXT NOT NULL    -- JSON: [{steps, batch_ids:[...]}, ...]
);

-- 失效审计：作业被哪次撤回永久停供（结果文件删除，记录保留用于"失效原因"查询）
CREATE TABLE IF NOT EXISTS daily_stats_invalidations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL,
    subject_id      TEXT NOT NULL,
    invalidated_at  TEXT NOT NULL,
    reason          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dsi_job ON daily_stats_invalidations(job_id);
"""


def get_conn() -> sqlite3.Connection:
    """每个线程一个连接（worker 线程与请求线程各自独立）。"""
    conn = getattr(_local, "conn", None)
    if conn is None:
        config.ensure_dirs()
        conn = sqlite3.connect(config.DB_PATH, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        _local.conn = conn
    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    # 两个实例可能同时首次打开数据库：journal_mode=WAL 的切换需要排他锁，
    # 撞锁时退避重试，避免启动失败。
    for attempt in range(30):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError:
            if attempt == 29:
                raise
            time.sleep(0.2)
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")


def init_db() -> None:
    conn = get_conn()
    # DDL 也可能与另一实例的初始化撞锁，重试
    for attempt in range(30):
        try:
            conn.executescript(SCHEMA)
            return
        except sqlite3.OperationalError:
            if attempt == 29:
                raise
            time.sleep(0.2)


def query(sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    return get_conn().execute(sql, tuple(params)).fetchall()


def query_one(sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    return get_conn().execute(sql, tuple(params)).fetchone()
