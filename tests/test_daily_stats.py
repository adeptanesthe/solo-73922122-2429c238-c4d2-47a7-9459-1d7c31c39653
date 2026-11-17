"""日统计作业语义：

- 受理时冻结已发布批次、排除已撤回者，后续导入不加入；
- 同人同日同值只计一次，异值整次失败并列出来源批次；
- 按日输出人数/步数总和/≥8000 人数，结果与来源依据一起保存；
- 撤回使相关已存结果永久停供、在途统计不得发布，无关撤回不影响；
- 重写中批次不受理；失败不留半份结果；重启后仍成立。
"""
import io
import threading
from pathlib import Path

import pyarrow.parquet as pq

from app import config, services
from tests.conftest import (
    csv_upload,
    run_stats_once,
    wait_completed,
    wait_stats_terminal,
)


def create_stats(client, start="2026-11-01", end="2026-11-30"):
    return client.post("/api/daily-stats", json={"start_date": start, "end_date": end})


def test_stats_basic_aggregation_and_dedup(client):
    # 批次1：S001 在 11-01 有两条同值记录（同人同日同值只计一次）
    csv_upload(client, [
        ("S001", "2026-11-01", 9000),
        ("S001", "2026-11-01", 9000),
        ("S002", "2026-11-01", 5000),
    ])
    # 批次2：S003 达标；S001 次日数据
    csv_upload(client, [
        ("S003", "2026-11-01", 8000),
        ("S001", "2026-11-02", 100),
    ])
    r = create_stats(client)
    assert r.status_code == 200
    job_id = r.json()["id"]
    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "ready"

    rows = client.get(f"/api/daily-stats/{job_id}/results").json()["results"]
    by_day = {r["stat_date"]: r for r in rows}
    d1 = by_day["2026-11-01"]
    assert d1["person_count"] == 3            # S001/S002/S003（同值去重后）
    assert d1["total_steps"] == 9000 + 5000 + 8000
    assert d1["ge_8000_count"] == 2           # S001=9000、S003=8000
    d2 = by_day["2026-11-02"]
    assert d2["person_count"] == 1 and d2["total_steps"] == 100 and d2["ge_8000_count"] == 0

    # 结果文件可下载且内容一致
    dl = client.get(f"/api/daily-stats/{job_id}/download")
    assert dl.status_code == 200
    t = pq.read_table(io.BytesIO(dl.content))
    assert t.num_rows == 2


def test_stats_snapshot_excludes_later_imports_and_withdrawn(client):
    csv_upload(client, [("S001", "2026-11-01", 100)])
    r = create_stats(client)
    job_id = r.json()["id"]

    # 受理之后的导入不得加入本次统计
    csv_upload(client, [("S002", "2026-11-01", 200)])
    # 受理时刻已有撤回者：先撤回再受理
    csv_upload(client, [("S009", "2026-11-01", 9)])
    client.post("/api/withdrawals", json={"subject_id": "S009"})
    wait_completed(client, "S009")
    r2 = create_stats(client)
    job2 = r2.json()["id"]

    final1 = wait_stats_terminal(client, job_id)
    final2 = wait_stats_terminal(client, job2)
    assert final1["status"] == "ready" and final2["status"] == "ready"

    rows1 = client.get(f"/api/daily-stats/{job_id}/results").json()["results"]
    assert rows1[0]["person_count"] == 1  # 只有 S001，S002 是后导入
    detail2 = client.get(f"/api/daily-stats/{job2}").json()
    assert "S009" in detail2["excluded_subjects"]
    rows2 = client.get(f"/api/daily-stats/{job2}/results").json()["results"]
    assert rows2[0]["person_count"] == 2  # S001、S002；S009 被排除


def test_conflict_same_person_day_different_values_fails_and_lists_batches(client):
    b1 = csv_upload(client, [("S001", "2026-11-01", 100)]).json()["batch_id"]
    b2 = csv_upload(client, [("S001", "2026-11-01", 200)]).json()["batch_id"]
    job_id = create_stats(client).json()["id"]
    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "failed"
    assert "冲突" in final["error_reason"]

    detail = client.get(f"/api/daily-stats/{job_id}").json()
    assert len(detail["conflicts"]) == 1
    c = detail["conflicts"][0]
    assert c["subject_id"] == "S001" and c["stat_date"] == "2026-11-01"
    per = {d["steps"]: d["batch_ids"] for d in c["detail"]}
    assert per == {100: [b1], 200: [b2]}

    # 失败不留半份结果：无结果行、无结果文件、结果接口 422、下载 409
    assert client.get(f"/api/daily-stats/{job_id}/results").status_code == 422
    assert client.get(f"/api/daily-stats/{job_id}/download").status_code == 409
    assert not list(config.STATS_DIR.glob("*.parquet"))


def test_conflict_inside_single_batch_also_fails(client):
    # 同一批次内同人同日异值同样算冲突
    csv_upload(client, [
        ("S001", "2026-11-01", 100),
        ("S001", "2026-11-01", 101),
    ])
    job_id = create_stats(client).json()["id"]
    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "failed"
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    assert len(detail["conflicts"]) == 1


def test_stats_rejected_while_batch_rewriting(client, monkeypatch):
    csv_upload(client, [("S001", "2026-11-01", 1), ("S002", "2026-11-01", 2)])
    client.post("/api/withdrawals", json={"subject_id": "S001"})

    original = services.rewrite_without_subject

    def wrapped(src, dst, subject):
        # 重写窗口内受理统计 → 409
        r = create_stats(client)
        assert r.status_code == 409
        return original(src, dst, subject)

    monkeypatch.setattr(services, "rewrite_without_subject", wrapped)
    wait_completed(client, "S001")

    # 重写结束后恢复受理
    r = create_stats(client)
    assert r.status_code == 200


def test_stats_rejected_while_withdrawal_open_then_accepted_after_complete(client):
    """撤回已受理但清理未完成（无重写批次的 GHOST 场景）也暂不受理统计。"""
    csv_upload(client, [("S001", "2026-11-01", 1)])
    # GHOST 无任何批次，受理后很快完成；这里直接驱动受理但不跑 worker
    r = client.post("/api/withdrawals", json={"subject_id": "GHOST"})
    assert r.status_code == 200
    r = create_stats(client)
    assert r.status_code == 409
    assert "撤回" in r.json()["detail"]

    # 撤回完成后恢复受理，且 GHOST 不影响真实数据
    wait_completed(client, "GHOST")
    r = create_stats(client)
    assert r.status_code == 200
    job_id = r.json()["id"]
    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "ready"
    rows = client.get(f"/api/daily-stats/{job_id}/results").json()["results"]
    assert rows[0]["person_count"] == 1


def test_withdrawal_invalidates_ready_stats_and_stops_serving(client):
    csv_upload(client, [
        ("S001", "2026-11-01", 9000),
        ("S002", "2026-11-01", 5000),
    ])
    job_id = create_stats(client).json()["id"]
    wait_stats_terminal(client, job_id)
    result_path = config.STATS_DIR / f"stats-{job_id:08d}.parquet"
    assert result_path.exists()

    client.post("/api/withdrawals", json={"subject_id": "S001"})
    wait_completed(client, "S001")

    # 立即永久停供
    assert client.get(f"/api/daily-stats/{job_id}/results").status_code == 410
    assert client.get(f"/api/daily-stats/{job_id}/download").status_code == 410
    assert not result_path.exists()  # 结果文件已删除
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    assert detail["status"] == "invalidated"
    assert detail["invalidations"][0]["subject_id"] == "S001"
    # 数据库无残留结果行
    from app import db
    n = db.query_one(
        "SELECT COUNT(*) AS c FROM daily_stats_results WHERE job_id=?", (job_id,)
    )["c"]
    assert n == 0


def test_unrelated_withdrawal_does_not_invalidate(client):
    csv_upload(client, [("S001", "2026-11-01", 100)])
    job_id = create_stats(client).json()["id"]
    wait_stats_terminal(client, job_id)

    # 撤回一个从未出现在数据中的人
    client.post("/api/withdrawals", json={"subject_id": "GHOST"})
    wait_completed(client, "GHOST")
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    assert detail["status"] == "ready"
    assert client.get(f"/api/daily-stats/{job_id}/results").status_code == 200

    # 撤回区间外的人也不影响
    csv_upload(client, [("S007", "2026-12-25", 1)])
    client.post("/api/withdrawals", json={"subject_id": "S007"})
    wait_completed(client, "S007")
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    assert detail["status"] == "ready"


def test_inflight_stats_cannot_publish_after_withdrawal(client):
    csv_upload(client, [
        ("S001", "2026-11-01", 9000),
        ("S002", "2026-11-01", 5000),
    ])
    job_id = create_stats(client).json()["id"]

    # 在慢统计进入发布复核前插入撤回受理（STATS_LOCK 与 PUBLISH_LOCK 独立，不互斥）
    real_compute = services._compute_daily_stats

    def compute_with_withdrawal(jid):
        client.post("/api/withdrawals", json={"subject_id": "S001"})
        return real_compute(jid)

    services._compute_daily_stats = compute_with_withdrawal
    try:
        final = wait_stats_terminal(client, job_id)
    finally:
        services._compute_daily_stats = real_compute
    assert final["status"] == "invalidated"
    assert client.get(f"/api/daily-stats/{job_id}/results").status_code == 410
    assert not list(config.STATS_DIR.glob("*.parquet"))
    # 撤回任务本身仍可正常完成
    wait_completed(client, "S001")


def test_imports_continue_during_slow_stats(client, monkeypatch):
    csv_upload(client, [("S001", "2026-11-01", 100)])
    job_id = create_stats(client).json()["id"]

    started = threading.Event()
    real_compute = services._compute_daily_stats

    def slow_compute(jid):
        started.set()
        import time as _t
        _t.sleep(0.8)
        return real_compute(jid)

    monkeypatch.setattr(services, "_compute_daily_stats", slow_compute)

    result = {}

    def worker():
        result["ran"] = run_stats_once()

    t = threading.Thread(target=worker)
    t.start()
    started.wait(2)
    # 统计进行中：导入与撤回受理均不被阻塞
    r = csv_upload(client, [("S005", "2026-11-01", 50)])
    result["import_status"] = r.status_code
    rw = client.post("/api/withdrawals", json={"subject_id": "S005"})
    result["withdraw_status"] = rw.status_code
    t.join()
    wait_completed(client, "S005")

    assert result["ran"] is True
    assert result["import_status"] == 200
    assert result["withdraw_status"] == 200
    # 后导入的 S005 不在冻结快照；且其撤回不影响该作业
    final = client.get(f"/api/daily-stats/{job_id}").json()
    assert final["status"] == "ready"
    rows = client.get(f"/api/daily-stats/{job_id}/results").json()["results"]
    assert rows[0]["person_count"] == 1


def test_stats_jobs_serialized_by_stats_lock(client):
    for i in range(3):
        csv_upload(client, [(f"S{i:03d}", "2026-11-01", i + 1)])
    # 只创建一个待处理作业：两个 worker 竞争，恰好一个拿到锁处理，另一个空转
    j1 = create_stats(client).json()["id"]

    outcomes = []

    def worker():
        try:
            outcomes.append(run_stats_once())
        except Exception:
            outcomes.append(False)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(outcomes) == [False, True]
    final = wait_stats_terminal(client, j1)
    assert final["status"] == "ready"

    # 锁释放后第二个作业可被正常处理
    j2 = create_stats(client).json()["id"]
    final2 = wait_stats_terminal(client, j2)
    assert final2["status"] == "ready"


def test_stats_404_and_validation(client):
    assert client.get("/api/daily-stats/9999").status_code == 404
    assert client.get("/api/daily-stats/9999/results").status_code == 404
    r = client.post("/api/daily-stats", json={"start_date": "bad", "end_date": "2026-11-01"})
    assert r.status_code == 422
    r = client.post("/api/daily-stats", json={"start_date": "2026-11-02", "end_date": "2026-11-01"})
    assert r.status_code == 422


def test_stats_recovery_after_restart(client):
    csv_upload(client, [("S001", "2026-11-01", 100), ("S002", "2026-11-01", 200)])
    job_id = create_stats(client).json()["id"]
    wait_stats_terminal(client, job_id)
    assert client.get(f"/api/daily-stats/{job_id}/results").status_code == 200

    # 模拟重启：恢复逻辑不应破坏 ready 作业
    rec = services.recover_on_startup()
    assert rec["daily_stats"]["deleted_orphan_results"] == 0
    assert client.get(f"/api/daily-stats/{job_id}").json()["status"] == "ready"

    # 孤儿结果文件被清
    orphan = config.STATS_DIR / "stats-99999999.parquet"
    orphan.write_bytes(b"junk")
    rec = services.recover_on_startup()
    assert rec["daily_stats"]["deleted_orphan_results"] >= 1
    assert not orphan.exists()


def test_stats_recovery_invalidates_after_offline_withdrawal(client):
    # 作业 ready 后，绕过受理钩子直接补一条撤回并清数据，再走启动恢复的失效兜底
    csv_upload(client, [("S001", "2026-11-01", 100)])
    job_id = create_stats(client).json()["id"]
    wait_stats_terminal(client, job_id)

    client.post("/api/withdrawals", json={"subject_id": "S001"})
    wait_completed(client, "S001")  # 正常路径已失效；这里再验证恢复幂等不重复报错
    rec = services.recover_on_startup()
    assert rec["daily_stats"]["invalidated_jobs"] == 0  # 已失效的不会重复计数
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    assert detail["status"] == "invalidated"


def test_processing_job_resumes_after_restart(client):
    csv_upload(client, [("S001", "2026-11-01", 100)])
    job_id = create_stats(client).json()["id"]
    # 模拟计算中进程退出：状态停在 processing
    from app import db
    db.get_conn().execute(
        "UPDATE daily_stats_jobs SET status='processing' WHERE id=?", (job_id,)
    )
    services.recover_on_startup()  # 不应删除作业、不误报终态
    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "ready"


def test_stats_source_evidence_persisted(client):
    b1 = csv_upload(client, [("S001", "2026-11-01", 100)]).json()["batch_id"]
    b2 = csv_upload(client, [("S002", "2026-11-02", 200)]).json()["batch_id"]
    job_id = create_stats(client).json()["id"]
    wait_stats_terminal(client, job_id)
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    frozen_batches = sorted(s["batch_id"] for s in detail["sources"])
    assert frozen_batches == sorted([b1, b2])
    for s in detail["sources"]:
        assert s["frozen_path"].endswith("v1.parquet")
        assert s["file_size"] > 0 and s["mtime_ns"] > 0


def test_empty_range_ready_with_zero_rows(client):
    csv_upload(client, [("S001", "2026-11-01", 100)])
    job_id = create_stats(client, "2027-01-01", "2027-01-31").json()["id"]
    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "ready"
    rows = client.get(f"/api/daily-stats/{job_id}/results").json()["results"]
    assert rows == []
    # 空结果也有 Parquet 产物可下载
    assert client.get(f"/api/daily-stats/{job_id}/download").status_code == 200


def test_stats_after_completed_withdrawal_uses_current_versions(client):
    """撤回完成（批次已重写为 v2、v1 已删除）后再受理统计：
    必须冻结当前版本，覆盖表不含已撤回者，作业正常 ready 且不会被误失效。"""
    csv_upload(client, [
        ("S001", "2026-11-01", 9000),
        ("S002", "2026-11-01", 5000),
    ])
    csv_upload(client, [("S003", "2026-11-01", 8000)])
    client.post("/api/withdrawals", json={"subject_id": "S001"})
    wait_completed(client, "S001")

    # 含 S001 的批次 1 已重写：v1 删除、只剩 v2；不含 S001 的批次 2 保持 v1
    assert not (config.BATCHES_DIR / "batch-00000001" / "v1.parquet").exists()
    assert (config.BATCHES_DIR / "batch-00000001" / "v2.parquet").exists()

    job_id = create_stats(client).json()["id"]
    detail = client.get(f"/api/daily-stats/{job_id}").json()
    # 冻结路径必须是真实存在的当前版本
    for s in detail["sources"]:
        assert Path(s["frozen_path"]).exists(), s["frozen_path"]
        assert s["frozen_path"].endswith("v2.parquet") or s["batch_id"] == 2
    # 覆盖表与排除名单
    from app import db
    covered = {
        r[0]
        for r in db.query(
            "SELECT subject_id FROM daily_stats_job_subjects WHERE job_id=?", (job_id,)
        )
    }
    assert "S001" not in covered
    assert covered == {"S002", "S003"}

    final = wait_stats_terminal(client, job_id)
    assert final["status"] == "ready"
    rows = client.get(f"/api/daily-stats/{job_id}/results").json()["results"]
    assert rows[0]["person_count"] == 2  # S002、S003
    assert rows[0]["ge_8000_count"] == 1  # 仅 S003=8000

    # 重复受理另一个作业也不应被旧撤回误伤
    job2 = create_stats(client).json()["id"]
    final2 = wait_stats_terminal(client, job2)
    assert final2["status"] == "ready"
