"""业务服务层：导入发布、撤回受理与清理、日统计作业、崩溃恢复。

锁约定：
- PUBLISH_LOCK 串行化"导入发布/撤回受理/统计受理/统计发布复核"，保证受理时刻的全序关系；
- WITHDRAW_LOCK 保证同一时刻只有一个实例在执行撤回清理；
- STATS_LOCK 保证同一时刻只有一个实例在执行日统计计算（慢计算不占用 PUBLISH_LOCK，
  因此统计期间导入与撤回受理均可继续）；
- 下载只依赖 SQLite 中批次/作业的 status，无需互斥锁。
"""
from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from pathlib import Path

import pyarrow as pa

from . import config, db
from .locks import file_lock
from .parquet_store import (
    atomic_write_parquet,
    distinct_subjects,
    list_parquet_files,
    parquet_contains_subject,
    read_rows_in_range,
    rewrite_without_subject,
    subjects_in_date_range,
    write_stats_result,
)


class ServiceError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- 导入发布

def _subject_is_withdrawn(conn, subject_ids: list[str]) -> str | None:
    placeholders = ",".join("?" * len(subject_ids))
    row = conn.execute(
        f"SELECT subject_id FROM withdrawals WHERE subject_id IN ({placeholders}) LIMIT 1",
        subject_ids,
    ).fetchone()
    return row[0] if row else None


def publish_import(table: pa.Table) -> dict:
    """在 PUBLISH_LOCK 下原子发布一批数据，返回批次信息。"""
    subjects = distinct_subjects(table)
    conn = db.get_conn()

    with file_lock(config.PUBLISH_LOCK):
        # 受理后永久拒收：含已撤回受试者的整批一律拒绝
        withdrawn = _subject_is_withdrawn(conn, subjects)
        if withdrawn is not None:
            raise ServiceError(
                f"整批拒绝：受试者 {withdrawn} 已撤回，任何包含该人的批次均不得导入",
                status_code=409,
            )

        now = utcnow()
        # 1. 先落暂存 Parquet（CSV 从不落盘，落盘的只有 Parquet）
        staging_name = f"import-{uuid.uuid4().hex}.parquet"
        staging_path = config.STAGING_DIR / staging_name
        atomic_write_parquet(table, staging_path)
        row_count = table.num_rows

        try:
            conn.execute("BEGIN IMMEDIATE")
            # 2. 登记暂存导入
            cur = conn.execute(
                "INSERT INTO imports (created_at, status, path, row_count, note) "
                "VALUES (?, 'staging', ?, ?, NULL)",
                (now, str(staging_path), row_count),
            )
            import_id = cur.lastrowid
            # 3. 建立批次
            cur = conn.execute(
                "INSERT INTO batches (created_at, row_count, status) VALUES (?, ?, 'published')",
                (now, row_count),
            )
            batch_id = cur.lastrowid
            # 4. 原子移入批次目录（版本号从 v1 起；先建目录再 rename）
            batch_dir = config.BATCHES_DIR / f"batch-{batch_id:08d}"
            batch_dir.mkdir(parents=True, exist_ok=True)
            final_path = batch_dir / "v1.parquet"
            os.replace(staging_path, final_path)
            # 5. 回填路径与受试者索引
            conn.execute(
                "UPDATE batches SET current_path=?, published_at=? WHERE id=?",
                (str(final_path), now, batch_id),
            )
            conn.execute(
                "UPDATE imports SET status='published', batch_id=? WHERE id=?",
                (batch_id, import_id),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO batch_subjects (batch_id, subject_id) VALUES (?, ?)",
                [(batch_id, s) for s in subjects],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            # 暂存文件可能已被 rename 走；清理仍存在的残留
            if staging_path.exists():
                staging_path.unlink()
            raise

    return {
        "batch_id": batch_id,
        "import_id": import_id,
        "row_count": row_count,
        "subjects": subjects,
        "path": str(final_path),
    }


# ---------------------------------------------------------------- 撤回受理

def accept_withdrawal(subject_id: str) -> dict:
    """受理撤回：在 PUBLISH_LOCK 下登记，与导入发布构成全序。

    重复撤回返回既有任务（接续原任务，不会新建）。
    """
    conn = db.get_conn()
    with file_lock(config.PUBLISH_LOCK):
        existing = conn.execute(
            "SELECT * FROM withdrawals WHERE subject_id=?", (subject_id,)
        ).fetchone()
        if existing is not None:
            return _withdrawal_dict(existing)

        now = utcnow()
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "INSERT INTO withdrawals (subject_id, requested_at, status, updated_at) "
            "VALUES (?, ?, 'accepted', ?)",
            (subject_id, now, now),
        )
        withdrawal_id = cur.lastrowid
        batch_rows = conn.execute(
            "SELECT batch_id FROM batch_subjects WHERE subject_id=? ORDER BY batch_id",
            (subject_id,),
        ).fetchall()
        conn.executemany(
            "INSERT INTO withdrawal_batches (withdrawal_id, batch_id, state) VALUES (?, ?, 'pending')",
            [(withdrawal_id, r[0]) for r in batch_rows],
        )
        conn.execute(
            "UPDATE withdrawals SET batches_total=? WHERE id=?",
            (len(batch_rows), withdrawal_id),
        )
        # 与受理同一事务：相关统计作业立即丧失供数资格（结果文件在提交后删除）
        deleted_paths = _invalidate_stats_for_subject(conn, subject_id, now)
        conn.execute("COMMIT")

    # 事务提交后才删除结果文件：永久停供，且回滚不会误删
    for sp in deleted_paths:
        _delete_quietly(Path(sp))

    row = conn.execute("SELECT * FROM withdrawals WHERE id=?", (withdrawal_id,)).fetchone()
    return _withdrawal_dict(row)


def _withdrawal_dict(row) -> dict:
    return {
        "id": row["id"],
        "subject_id": row["subject_id"],
        "requested_at": row["requested_at"],
        "status": row["status"],
        "batches_total": row["batches_total"],
        "batches_done": row["batches_done"],
        "files_removed": row["files_removed"],
        "last_error": row["last_error"],
        "updated_at": row["updated_at"],
    }


def get_withdrawal(subject_id: str) -> dict | None:
    row = db.query_one("SELECT * FROM withdrawals WHERE subject_id=?", (subject_id,))
    return _withdrawal_dict(row) if row else None


def list_withdrawals() -> list[dict]:
    return [_withdrawal_dict(r) for r in db.query("SELECT * FROM withdrawals ORDER BY id")]


# ---------------------------------------------------------------- 后台清理

def _next_version_path(batch_dir: Path, current_path: str | None) -> Path:
    version = 1
    if current_path:
        stem = Path(current_path).stem
        if stem.startswith("v") and stem[1:].isdigit():
            version = int(stem[1:]) + 1
    return batch_dir / f"v{version}.parquet"


def process_due_withdrawal() -> bool:
    """处理一条未完成的撤回任务。返回是否处理了任务。"""
    conn = db.get_conn()
    with file_lock(config.WITHDRAW_LOCK, timeout=5):
        w = conn.execute(
            "SELECT * FROM withdrawals WHERE status != 'completed' ORDER BY id LIMIT 1"
        ).fetchone()
        if w is None:
            return False

        withdrawal_id = w["id"]
        subject_id = w["subject_id"]
        conn.execute(
            "UPDATE withdrawals SET status='processing', updated_at=? WHERE id=?",
            (utcnow(), withdrawal_id),
        )
        try:
            # 1. 逐批重写（每批独立事务，崩溃后可从 rewriting 状态接续）
            wb_rows = conn.execute(
                "SELECT batch_id, state FROM withdrawal_batches "
                "WHERE withdrawal_id=? AND state != 'done' ORDER BY batch_id",
                (withdrawal_id,),
            ).fetchall()
            for wb in wb_rows:
                batch_id = wb["batch_id"]
                _rewrite_batch(withdrawal_id, batch_id, subject_id)

            # 2. 全卷清扫：旧版本与暂存件中含该人的文件全部清除
            removed = _final_sweep(subject_id)

            # 3. 完成
            conn.execute(
                "UPDATE withdrawals SET status='completed', files_removed=?, "
                "last_error=NULL, updated_at=? WHERE id=?",
                (removed, utcnow(), withdrawal_id),
            )
        except Exception as exc:
            conn.execute(
                "UPDATE withdrawals SET status='failed', last_error=?, updated_at=? WHERE id=?",
                (repr(exc), utcnow(), withdrawal_id),
            )
            raise
        return True


def _rewrite_batch(withdrawal_id: int, batch_id: int, subject_id: str) -> None:
    conn = db.get_conn()
    batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    if batch is None:
        # 批次已不存在，直接标记完成
        conn.execute(
            "UPDATE withdrawal_batches SET state='done' WHERE withdrawal_id=? AND batch_id=?",
            (withdrawal_id, batch_id),
        )
        return

    # 状态翻转与下载封锁在同一事务
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE batches SET status='rewriting' WHERE id=?", (batch_id,))
    conn.execute(
        "UPDATE withdrawal_batches SET state='rewriting' "
        "WHERE withdrawal_id=? AND batch_id=?",
        (withdrawal_id, batch_id),
    )
    current_path = conn.execute(
        "SELECT current_path FROM batches WHERE id=?", (batch_id,)
    ).fetchone()[0]
    conn.execute("COMMIT")

    batch_dir = config.BATCHES_DIR / f"batch-{batch_id:08d}"
    new_path = _next_version_path(batch_dir, current_path)
    old_path = Path(current_path) if current_path else None

    # 幂等：崩溃重启后可能 new_path 已存在，重算一遍结果相同
    kept = rewrite_without_subject(old_path, new_path, subject_id)

    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE batches SET current_path=?, row_count=?, status='published' WHERE id=?",
        (str(new_path), kept, batch_id),
    )
    conn.execute(
        "DELETE FROM batch_subjects WHERE batch_id=? AND subject_id=?",
        (batch_id, subject_id),
    )
    conn.execute(
        "UPDATE withdrawal_batches SET state='done' WHERE withdrawal_id=? AND batch_id=?",
        (withdrawal_id, batch_id),
    )
    # 直接以 done 行数回填，崩溃接续重复执行结果也一致
    conn.execute(
        "UPDATE withdrawals SET "
        "batches_done=(SELECT COUNT(*) FROM withdrawal_batches "
        "WHERE withdrawal_id=? AND state='done'), updated_at=? WHERE id=?",
        (withdrawal_id, utcnow(), withdrawal_id),
    )
    conn.execute("COMMIT")

    # 旧版本立即删除（崩溃残留由最终清扫兜底）
    if old_path and old_path.exists() and old_path != new_path:
        old_path.unlink()


def _final_sweep(subject_id: str) -> int:
    """扫描共享卷全部 Parquet，清除含该人的旧件/暂存件，并做最终校验。"""
    conn = db.get_conn()
    valid_paths = {
        r[0] for r in conn.execute(
            "SELECT current_path FROM batches WHERE current_path IS NOT NULL"
        ).fetchall()
    }
    removed = 0

    # 2a. 批次目录下不属于任何批次当前版本的文件 = 历史旧版本，删除
    for p in list_parquet_files():
        sp = str(p)
        if sp.startswith(str(config.BATCHES_DIR)) and sp not in valid_paths:
            p.unlink()
            removed += 1

    # 2b. 暂存件：含该人的整份删除，并把对应导入标记为 rejected
    for p in list_parquet_files():
        sp = str(p)
        if not sp.startswith(str(config.STAGING_DIR)):
            continue
        if parquet_contains_subject(p, subject_id):
            p.unlink()
            removed += 1
            conn.execute(
                "UPDATE imports SET status='rejected', "
                "note='withdrawal sweep: contained withdrawn subject' WHERE path=?",
                (sp,),
            )

    # 2c. 最终校验：任何现存 Parquet 都不得再含该人，否则任务失败不得报完成
    for p in list_parquet_files():
        if parquet_contains_subject(p, subject_id):
            raise ServiceError(
                f"撤回清扫后仍发现含受试者 {subject_id} 的文件：{p}", status_code=500
            )
    return removed


# ---------------------------------------------------------------- 日统计作业

def _delete_quietly(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _job_covers_subject(conn, job, subject_id: str) -> bool:
    """判定作业受理时冻结的数据是否覆盖某受试者（无关撤回不得误伤）。

    覆盖表在受理时刻于 PUBLISH_LOCK 内构建，故这里只需纯数据库查询。
    """
    return conn.execute(
        "SELECT 1 FROM daily_stats_job_subjects WHERE job_id=? AND subject_id=? LIMIT 1",
        (job["id"], subject_id),
    ).fetchone() is not None


def _invalidate_stats_for_subject(conn, subject_id: str, now: str) -> list[str]:
    """在 PUBLISH_LOCK 事务内调用：撤回相关的统计作业立即丧失供数资格。

    返回待删除的结果文件路径（事务提交后由调用方删除，避免回滚后误删）。
    """
    deleted_paths: list[str] = []
    jobs = conn.execute(
        "SELECT * FROM daily_stats_jobs WHERE status IN ('pending','processing','ready') ORDER BY id"
    ).fetchall()
    reason = f"受试者 {subject_id} 撤回：相关已存结果永久停供，在途统计不得发布，须重新生成"
    for job in jobs:
        if not _job_covers_subject(conn, job, subject_id):
            continue  # 无关撤回不影响
        old_path = job["result_path"]
        cur = conn.execute(
            "UPDATE daily_stats_jobs SET status='invalidated', result_path=NULL, "
            "error_reason=?, updated_at=? WHERE id=? AND status IN ('pending','processing','ready')",
            (reason, now, job["id"]),
        )
        if cur.rowcount > 0:
            # 已存结果立即永久停供：同一事务删除结果行，不留半份
            conn.execute(
                "DELETE FROM daily_stats_results WHERE job_id=?", (job["id"],)
            )
            conn.execute(
                "INSERT INTO daily_stats_invalidations (job_id, subject_id, invalidated_at, reason) "
                "VALUES (?, ?, ?, ?)",
                (job["id"], subject_id, now, reason),
            )
            if old_path:
                deleted_paths.append(old_path)
    return deleted_paths


def create_daily_stats(start_s: str, end_s: str) -> dict:
    """受理日统计：PUBLISH_LOCK 下冻结已发布批次与已撤回者名单。"""
    try:
        start = dt.date.fromisoformat(start_s)
        end = dt.date.fromisoformat(end_s)
    except (TypeError, ValueError) as exc:
        raise ServiceError("日期格式必须为 YYYY-MM-DD", status_code=422) from exc
    if start > end:
        raise ServiceError("起始日期不得晚于结束日期", status_code=422)

    conn = db.get_conn()
    job_id: int | None = None
    with file_lock(config.PUBLISH_LOCK):
        # 存在重写中批次则暂不受理
        rewriting = conn.execute(
            "SELECT id FROM batches WHERE status='rewriting' ORDER BY id LIMIT 1"
        ).fetchone()
        if rewriting:
            raise ServiceError(
                f"批次 {rewriting[0]} 正在撤回重写，暂不受理统计，请稍后重试",
                status_code=409,
            )
        # 已受理但尚未完成的撤回也会重写批次：该窗口内冻结的文件不稳定，
        # 且含已撤回者数据，暂不受理（撤回通常很快完成，前端可重试）
        open_w = conn.execute(
            "SELECT subject_id FROM withdrawals WHERE status != 'completed' LIMIT 1"
        ).fetchone()
        if open_w:
            raise ServiceError(
                f"受试者 {open_w[0]} 的撤回正在处理中，暂不受理统计，请稍后重试",
                status_code=409,
            )
        now = utcnow()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "INSERT INTO daily_stats_jobs (start_date, end_date, created_at, updated_at, status) "
                "VALUES (?, ?, ?, ?, 'pending')",
                (start.isoformat(), end.isoformat(), now, now),
            )
            job_id = cur.lastrowid
            # 冻结受理时刻"已发布"的批次；受理之后的导入不在快照内，不加入本次统计
            batches = conn.execute(
                "SELECT id, current_path FROM batches "
                "WHERE status='published' AND current_path IS NOT NULL ORDER BY id"
            ).fetchall()
            covered: set[str] = set()
            for b in batches:
                # DB 的 current_path 理论上指向当前版本；若文件已不在（极端竞态/残留），
                # 受理阶段直接拒绝，绝不把不存在的路径冻结进来源依据
                p = Path(b["current_path"])
                if not p.exists():
                    raise ServiceError(
                        f"批次 {b['id']} 数据文件缺失（{p}），暂不受理统计",
                        status_code=409,
                    )
                st = p.stat()
                conn.execute(
                    "INSERT INTO daily_stats_sources "
                    "(job_id, batch_id, frozen_path, file_size, mtime_ns) VALUES (?, ?, ?, ?, ?)",
                    (job_id, b["id"], str(p), st.st_size, st.st_mtime_ns),
                )
                # 受理时刻即固化覆盖受试者（基于当前真实文件）：
                # 已撤回者的数据已在批次重写中剔除，天然不会进入覆盖表；
                # 此后撤回失效判定为纯数据库查询，即使文件随后被重写删除也不会漏判
                covered |= subjects_in_date_range(p, start, end)
            conn.executemany(
                "INSERT OR IGNORE INTO daily_stats_job_subjects (job_id, subject_id) VALUES (?, ?)",
                [(job_id, s) for s in sorted(covered)],
            )
            # 排除所有已受理撤回者
            withdrawn = [
                r[0]
                for r in conn.execute("SELECT subject_id FROM withdrawals ORDER BY subject_id").fetchall()
            ]
            conn.executemany(
                "INSERT OR IGNORE INTO daily_stats_excluded (job_id, subject_id) VALUES (?, ?)",
                [(job_id, s) for s in withdrawn],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return _daily_stats_job_dict(_get_job_row(job_id))


def _resolve_source_path(conn, job_id: int, frozen_path: Path) -> Path | None:
    """定位来源文件：冻结版本可能已被撤回重写删除，回退到批次当前版本。

    回退是安全的：重写只会剔除已撤回受试者，而该受试者本就不允许进入本作业结果。
    """
    if frozen_path.exists():
        return frozen_path
    row = conn.execute(
        "SELECT b.current_path FROM daily_stats_sources s "
        "JOIN batches b ON b.id = s.batch_id "
        "WHERE s.job_id=? AND s.frozen_path=?",
        (job_id, str(frozen_path)),
    ).fetchone()
    if row and row[0] and Path(row[0]).exists():
        return Path(row[0])
    return None


def process_due_daily_stats() -> bool:
    """worker 入口：处理一条待计算的统计作业，返回是否处理了作业。"""
    conn = db.get_conn()
    try:
        with file_lock(config.STATS_LOCK, timeout=5):
            job = conn.execute(
                "SELECT * FROM daily_stats_jobs "
                "WHERE status IN ('pending','processing') ORDER BY id LIMIT 1"
            ).fetchone()
            if job is None:
                return False
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE daily_stats_jobs SET status='processing', updated_at=? "
                "WHERE id=? AND status IN ('pending','processing')",
                (utcnow(), job["id"]),
            )
            conn.execute("COMMIT")
            if cur.rowcount == 0:
                return False  # 已被撤回失效
            try:
                _compute_daily_stats(job["id"])
            except Exception as exc:
                # 守卫：计算期间已被撤回失效的作业不得被异常处理改回 failed
                conn.execute(
                    "UPDATE daily_stats_jobs SET status='failed', error_reason=?, updated_at=? "
                    "WHERE id=? AND status IN ('pending','processing')",
                    (repr(exc), utcnow(), job["id"]),
                )
                raise
            return True
    except TimeoutError:
        return False  # 另一实例正在统计


def _compute_daily_stats(job_id: int) -> None:
    """慢计算：读取冻结来源 → 冲突检测 → 聚合 → 锁内复核后原子发布。"""
    conn = db.get_conn()
    job = conn.execute("SELECT * FROM daily_stats_jobs WHERE id=?", (job_id,)).fetchone()
    if job is None or job["status"] not in ("pending", "processing"):
        return
    start = dt.date.fromisoformat(job["start_date"])
    end = dt.date.fromisoformat(job["end_date"])

    # 覆盖受试者表已在受理时刻构建（撤回精确失效的依据），这里不再延迟构建
    # 读取全部冻结来源；排除受理时刻已撤回者
    excluded = {
        r[0]
        for r in conn.execute(
            "SELECT subject_id FROM daily_stats_excluded WHERE job_id=?", (job_id,)
        ).fetchall()
    }
    # subject -> date(iso) -> {(steps, batch_id)}
    observations: dict[str, dict[str, set[tuple[int, int]]]] = {}
    source_row_count = 0
    for src in conn.execute(
        "SELECT * FROM daily_stats_sources WHERE job_id=? ORDER BY batch_id", (job_id,)
    ).fetchall():
        frozen_path = Path(src["frozen_path"])
        p = _resolve_source_path(conn, job_id, frozen_path)
        if p is None:
            raise ServiceError(
                f"来源批次 {src['batch_id']} 的文件已不可用，统计失败", status_code=500
            )
        if str(p) == src["frozen_path"]:
            st = p.stat()
            # 冻结签名复核：受理后原文件被改动即失败（替换文件属撤回重写，豁免）
            if st.st_size != src["file_size"] or st.st_mtime_ns != src["mtime_ns"]:
                raise ServiceError(
                    f"来源批次 {src['batch_id']} 的冻结文件在受理后被改动，统计失败",
                    status_code=500,
                )
        for subject, row_date, steps_val in read_rows_in_range(p, start, end):
            if subject in excluded:
                continue
            source_row_count += 1
            observations.setdefault(subject, {}).setdefault(
                row_date.isoformat(), set()
            ).add((steps_val, src["batch_id"]))

    # 3. 同人同日异值 → 整次失败，逐组列出值与来源批次
    conflicts: list[dict] = []
    for subject in sorted(observations):
        for day in sorted(observations[subject]):
            entries = observations[subject][day]
            distinct_steps = {e[0] for e in entries}
            if len(distinct_steps) > 1:
                per_value: dict[int, set[int]] = {}
                for steps_val, batch_id in entries:
                    per_value.setdefault(steps_val, set()).add(batch_id)
                conflicts.append(
                    {
                        "subject_id": subject,
                        "stat_date": day,
                        "detail": [
                            {"steps": s, "batch_ids": sorted(bids)}
                            for s, bids in sorted(per_value.items())
                        ],
                    }
                )
    if conflicts:
        now = utcnow()
        conn.execute("BEGIN IMMEDIATE")
        # 状态守卫：计算期间若已被撤回失效，绝不能把状态改回 failed
        fresh = conn.execute(
            "SELECT status FROM daily_stats_jobs WHERE id=?", (job_id,)
        ).fetchone()
        if fresh is None or fresh["status"] not in ("pending", "processing"):
            conn.execute("ROLLBACK")
            return
        conn.executemany(
            "INSERT INTO daily_stats_conflicts (job_id, subject_id, stat_date, detail) "
            "VALUES (?, ?, ?, ?)",
            [
                (job_id, c["subject_id"], c["stat_date"], json.dumps(c["detail"], ensure_ascii=False))
                for c in conflicts
            ],
        )
        conn.execute(
            "UPDATE daily_stats_jobs SET status='failed', error_reason=?, "
            "source_row_count=?, updated_at=? WHERE id=? AND status IN ('pending','processing')",
            (
                f"发现 {len(conflicts)} 处同人同日异值冲突，整次统计失败，冲突明细列出了来源批次",
                source_row_count,
                now,
                job_id,
            ),
        )
        conn.execute("COMMIT")  # 失败状态与冲突明细一起落库，无任何结果行/结果文件
        return

    # 4. 去重后按日聚合：每人每天只计一次（同值去重；异值已在上面判失败）
    daily: dict[str, dict] = {}
    for subject, by_date in observations.items():
        for day, entries in by_date.items():
            steps_val = next(iter(entries))[0]
            bucket = daily.setdefault(day, {"persons": set(), "total": 0, "ge": set()})
            bucket["persons"].add(subject)
            bucket["total"] += steps_val
            if steps_val >= 8000:
                bucket["ge"].add(subject)
    result_rows = [
        {
            "stat_date": dt.date.fromisoformat(day),
            "person_count": len(bucket["persons"]),
            "total_steps": bucket["total"],
            "ge_8000_count": len(bucket["ge"]),
        }
        for day, bucket in sorted(daily.items())
    ]

    # 5. 先原子写结果文件（未提交前任何接口都不供数），再在 PUBLISH_LOCK 下复核发布
    result_path = config.STATS_DIR / f"stats-{job_id:08d}.parquet"
    write_stats_result(result_rows, result_path)
    now = utcnow()
    artifact_to_delete: Path | None = None
    with file_lock(config.PUBLISH_LOCK):
        conn.execute("BEGIN IMMEDIATE")
        fresh = conn.execute(
            "SELECT status, result_path FROM daily_stats_jobs WHERE id=?", (job_id,)
        ).fetchone()
        if fresh is None or fresh["status"] not in ("pending", "processing"):
            conn.execute("ROLLBACK")
            artifact_to_delete = result_path
        else:
            # 5a. 统计期间撤回受理：在途统计不得发布
            hit = conn.execute(
                "SELECT js.subject_id FROM daily_stats_job_subjects js "
                "JOIN withdrawals w ON w.subject_id = js.subject_id "
                "WHERE js.job_id=? LIMIT 1",
                (job_id,),
            ).fetchone()
            fail_reason = None
            if hit:
                reason = (
                    f"受试者 {hit[0]} 在统计期间撤回：在途结果不予发布，须重新生成"
                )
                conn.execute(
                    "UPDATE daily_stats_jobs SET status='invalidated', result_path=NULL, "
                    "error_reason=?, updated_at=? WHERE id=?",
                    (reason, now, job_id),
                )
                conn.execute(
                    "DELETE FROM daily_stats_results WHERE job_id=?", (job_id,)
                )
                conn.execute(
                    "INSERT INTO daily_stats_invalidations (job_id, subject_id, invalidated_at, reason) "
                    "VALUES (?, ?, ?, ?)",
                    (job_id, hit[0], now, reason),
                )
                artifact_to_delete = result_path
            else:
                # 5b. 来源冻结复核（存在性 + 原文件签名）
                for src in conn.execute(
                    "SELECT * FROM daily_stats_sources WHERE job_id=? ORDER BY batch_id", (job_id,)
                ).fetchall():
                    p = _resolve_source_path(conn, job_id, Path(src["frozen_path"]))
                    if p is None:
                        fail_reason = f"来源批次 {src['batch_id']} 文件不可用，发布复核失败"
                        break
                    if str(p) == src["frozen_path"]:
                        st = p.stat()
                        if st.st_size != src["file_size"] or st.st_mtime_ns != src["mtime_ns"]:
                            fail_reason = f"来源批次 {src['batch_id']} 冻结文件被改动，发布复核失败"
                            break
            if artifact_to_delete is None and fail_reason is None:
                # 5c. 结果、来源依据与供数资格在同一事务一起生效
                conn.executemany(
                    "INSERT INTO daily_stats_results "
                    "(job_id, stat_date, person_count, total_steps, ge_8000_count) "
                    "VALUES (?, ?, ?, ?, ?)",
                    [
                        (
                            job_id,
                            r["stat_date"].isoformat(),
                            r["person_count"],
                            r["total_steps"],
                            r["ge_8000_count"],
                        )
                        for r in result_rows
                    ],
                )
                conn.execute(
                    "UPDATE daily_stats_jobs SET status='ready', result_path=?, "
                    "source_row_count=?, error_reason=NULL, updated_at=? WHERE id=?",
                    (str(result_path), source_row_count, now, job_id),
                )
                conn.execute("COMMIT")
            elif fail_reason is not None:
                conn.execute(
                    "UPDATE daily_stats_jobs SET status='failed', error_reason=?, "
                    "source_row_count=?, updated_at=? WHERE id=?",
                    (fail_reason, source_row_count, now, job_id),
                )
                conn.execute("COMMIT")
                artifact_to_delete = result_path
            else:
                conn.execute("COMMIT")
    if artifact_to_delete is not None:
        _delete_quietly(artifact_to_delete)


def _get_job_row(job_id: int):
    return db.query_one("SELECT * FROM daily_stats_jobs WHERE id=?", (job_id,))


def _daily_stats_job_dict(row) -> dict:
    return {
        "id": row["id"],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "source_row_count": row["source_row_count"],
        "error_reason": row["error_reason"],
    }


def list_daily_stats_jobs() -> list[dict]:
    return [
        _daily_stats_job_dict(r)
        for r in db.query("SELECT * FROM daily_stats_jobs ORDER BY id DESC")
    ]


def get_daily_stats_job(job_id: int) -> dict | None:
    row = _get_job_row(job_id)
    if row is None:
        return None
    detail = _daily_stats_job_dict(row)
    detail["sources"] = [
        {
            "batch_id": r["batch_id"],
            "frozen_path": r["frozen_path"],
            "file_size": r["file_size"],
            "mtime_ns": r["mtime_ns"],
        }
        for r in db.query(
            "SELECT * FROM daily_stats_sources WHERE job_id=? ORDER BY batch_id", (job_id,)
        )
    ]
    detail["excluded_subjects"] = [
        r[0]
        for r in db.query(
            "SELECT subject_id FROM daily_stats_excluded WHERE job_id=? ORDER BY subject_id",
            (job_id,),
        )
    ]
    detail["conflicts"] = [
        {
            "subject_id": r["subject_id"],
            "stat_date": r["stat_date"],
            "detail": json.loads(r["detail"]),
        }
        for r in db.query(
            "SELECT * FROM daily_stats_conflicts WHERE job_id=? ORDER BY id", (job_id,)
        )
    ]
    detail["invalidations"] = [
        {
            "subject_id": r["subject_id"],
            "invalidated_at": r["invalidated_at"],
            "reason": r["reason"],
        }
        for r in db.query(
            "SELECT * FROM daily_stats_invalidations WHERE job_id=? ORDER BY id", (job_id,)
        )
    ]
    return detail


def get_daily_stats_results(job_id: int) -> list[dict]:
    row = _get_job_row(job_id)
    if row is None:
        raise ServiceError(f"统计作业 {job_id} 不存在", status_code=404)
    if row["status"] == "invalidated":
        raise ServiceError(
            f"统计作业 {job_id} 已永久停供：{row['error_reason'] or '受试者撤回'}",
            status_code=410,
        )
    if row["status"] == "failed":
        raise ServiceError(
            f"统计作业 {job_id} 失败，无结果：{row['error_reason'] or '详见冲突明细'}",
            status_code=422,
        )
    if row["status"] != "ready":
        raise ServiceError(
            f"统计作业 {job_id} 尚在 {row['status']}，结果未发布", status_code=409
        )
    if not row["result_path"] or not Path(row["result_path"]).exists():
        raise ServiceError(
            f"统计作业 {job_id} 的结果文件缺失", status_code=500
        )
    return [
        {
            "stat_date": r["stat_date"],
            "person_count": r["person_count"],
            "total_steps": r["total_steps"],
            "ge_8000_count": r["ge_8000_count"],
        }
        for r in db.query(
            "SELECT * FROM daily_stats_results WHERE job_id=? ORDER BY stat_date", (job_id,)
        )
    ]


def get_stats_download(job_id: int):
    row = _get_job_row(job_id)
    if row is None:
        raise ServiceError(f"统计作业 {job_id} 不存在", status_code=404)
    if row["status"] == "invalidated":
        raise ServiceError(
            f"统计作业 {job_id} 已永久停供：{row['error_reason'] or '受试者撤回'}",
            status_code=410,
        )
    if row["status"] != "ready":
        raise ServiceError(
            f"统计作业 {job_id} 当前状态 {row['status']}，无结果可下载", status_code=409
        )
    path = Path(row["result_path"]) if row["result_path"] else None
    if not path or not path.exists():
        raise ServiceError(f"统计作业 {job_id} 的结果文件缺失", status_code=500)
    return row, path


# ---------------------------------------------------------------- 下载与列表

def get_downloadable_batch(batch_id: int):
    row = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if row is None:
        raise ServiceError(f"批次 {batch_id} 不存在", status_code=404)
    if row["status"] != "published":
        raise ServiceError(
            f"批次 {batch_id} 正在撤回重写，暂时禁止下载", status_code=409
        )
    path = Path(row["current_path"])
    if not path.exists():
        # 绝不允许把丢失/损坏的文件当成功返回
        raise ServiceError(f"批次 {batch_id} 的数据文件缺失", status_code=500)
    return row, path


def list_batches() -> list[dict]:
    rows = db.query(
        "SELECT b.*, "
        "(SELECT GROUP_CONCAT(subject_id, ',') FROM batch_subjects WHERE batch_id=b.id) AS subjects "
        "FROM batches b ORDER BY b.id"
    )
    return [
        {
            "id": r["id"],
            "created_at": r["created_at"],
            "row_count": r["row_count"],
            "status": r["status"],
            "subjects": (r["subjects"] or "").split(",") if r["subjects"] else [],
        }
        for r in rows
    ]


# ---------------------------------------------------------------- 崩溃恢复

def recover_on_startup() -> dict:
    """进程启动时接续未完成状态，绝不误报完成。"""
    config.ensure_dirs()
    db.init_db()
    conn = db.get_conn()
    cleaned_tmp = 0
    rejected_imports = 0
    deleted_orphans = 0

    # 1. 清理写入中途崩溃留下的临时文件
    for root in (config.BATCHES_DIR, config.STAGING_DIR, config.STATS_DIR):
        if not root.exists():
            continue
        for p in root.rglob(".tmp.*"):
            if p.is_file():
                p.unlink()
                cleaned_tmp += 1

    # 2. 暂存导入：文件丢失或没有关联批次（发布途中崩溃）→ 作废并清理
    for imp in conn.execute("SELECT id, path FROM imports WHERE status='staging'").fetchall():
        p = Path(imp["path"])
        if not p.exists():
            conn.execute(
                "UPDATE imports SET status='rejected', note=? WHERE id=?",
                ("startup recovery: staging file lost", imp["id"]),
            )
            rejected_imports += 1
        else:
            linked = conn.execute(
                "SELECT 1 FROM imports WHERE id=? AND batch_id IS NOT NULL", (imp["id"],)
            ).fetchone()
            if linked is None:
                p.unlink()
                conn.execute(
                    "UPDATE imports SET status='rejected', note=? WHERE id=?",
                    ("startup recovery: unfinished publish", imp["id"]),
                )
                rejected_imports += 1

    # 3. 批次状态恢复：rewriting 留给 worker 接续；文件丢失的 published 标记 failed
    for b in conn.execute("SELECT id, status, current_path FROM batches").fetchall():
        if b["status"] == "published" and (not b["current_path"] or not Path(b["current_path"]).exists()):
            conn.execute("UPDATE batches SET status='failed' WHERE id=?", (b["id"],))

    # 4. 清理批次目录中无任何 DB 引用的孤儿 Parquet
    valid_paths = {
        r[0] for r in conn.execute(
            "SELECT current_path FROM batches WHERE current_path IS NOT NULL"
        ).fetchall()
    }
    if config.BATCHES_DIR.exists():
        for p in config.BATCHES_DIR.rglob("*.parquet"):
            if str(p) not in valid_paths:
                # 注意：rewriting 中的新版本（vN 已写但事务未提交）也会被清，
                # worker 接续时会基于当前版本幂等重写，安全。
                p.unlink()
                deleted_orphans += 1

    # 5. 日统计作业恢复
    stats = _recover_daily_stats()

    return {
        "cleaned_tmp_files": cleaned_tmp,
        "rejected_imports": rejected_imports,
        "deleted_orphan_parquets": deleted_orphans,
        "daily_stats": stats,
    }


def _recover_daily_stats() -> dict:
    """统计作业的崩溃恢复：失效兜底、孤儿结果清理、processing 接续。"""
    conn = db.get_conn()
    invalidated = 0
    failed = 0
    deleted_orphan_results = 0
    now = utcnow()

    # 5a. 失效兜底：若本实例停机期间他实例受理了撤回，这里补齐供数资格回收
    with file_lock(config.PUBLISH_LOCK):
        conn.execute("BEGIN IMMEDIATE")
        deleted_paths: list[str] = []
        for w in conn.execute("SELECT subject_id FROM withdrawals").fetchall():
            jobs = conn.execute(
                "SELECT * FROM daily_stats_jobs WHERE status IN ('pending','processing','ready')"
            ).fetchall()
            for job in jobs:
                if _job_covers_subject(conn, job, w["subject_id"]):
                    reason = (
                        f"受试者 {w['subject_id']} 撤回：重启恢复时补齐失效，"
                        "已存结果永久停供，须重新生成"
                    )
                    old_path = job["result_path"]
                    conn.execute(
                        "UPDATE daily_stats_jobs SET status='invalidated', result_path=NULL, "
                        "error_reason=?, updated_at=? WHERE id=? AND status IN "
                        "('pending','processing','ready')",
                        (reason, now, job["id"]),
                    )
                    conn.execute(
                        "DELETE FROM daily_stats_results WHERE job_id=?", (job["id"],)
                    )
                    conn.execute(
                        "INSERT INTO daily_stats_invalidations "
                        "(job_id, subject_id, invalidated_at, reason) VALUES (?, ?, ?, ?)",
                        (job["id"], w["subject_id"], now, reason),
                    )
                    if old_path:
                        deleted_paths.append(old_path)
                    invalidated += 1
        conn.execute("COMMIT")
    for sp in deleted_paths:
        _delete_quietly(Path(sp))

    # 5b. ready 作业结果文件丢失：不得供数，转 failed 待重新生成
    for job in conn.execute(
        "SELECT id, result_path FROM daily_stats_jobs WHERE status='ready'"
    ).fetchall():
        if not job["result_path"] or not Path(job["result_path"]).exists():
            conn.execute(
                "UPDATE daily_stats_jobs SET status='failed', "
                "error_reason='启动恢复：结果文件缺失，须重新生成', updated_at=? WHERE id=?",
                (now, job["id"]),
            )
            failed += 1

    # 5c. processing 作业保持 processing（worker 幂等接续，覆盖表/结果均无半份可见）

    # 5d. 清理 stats/ 下无 ready 作业引用的孤儿 Parquet（含崩溃前已写未提交的结果）
    valid_result_paths = {
        r[0]
        for r in conn.execute(
            "SELECT result_path FROM daily_stats_jobs "
            "WHERE status='ready' AND result_path IS NOT NULL"
        ).fetchall()
    }
    if config.STATS_DIR.exists():
        for p in config.STATS_DIR.glob("*.parquet"):
            if str(p) not in valid_result_paths:
                p.unlink()
                deleted_orphan_results += 1

    return {
        "invalidated_jobs": invalidated,
        "failed_jobs": failed,
        "deleted_orphan_results": deleted_orphan_results,
    }
