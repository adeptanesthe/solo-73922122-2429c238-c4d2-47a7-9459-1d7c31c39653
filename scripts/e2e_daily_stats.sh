#!/usr/bin/env bash
# 日统计双实例端到端自测（共用同一数据卷）：
#   1. 两实例交错导入；实例1受理统计，两实例都能查到同一结果；
#   2. 受理后的导入不加入本次统计；
#   3. 同人同日异值 → 整次失败并列出冲突来源批次；
#   4. 撤回使已存结果在两个实例上立即永久停供（410），无关撤回不影响；
#   5. 结果 Parquet 可下载，失效后文件删除。
# 用法：bash scripts/e2e_daily_stats.sh [PORT1] [PORT2]
set -euo pipefail

PORT1="${1:-}"
PORT2="${2:-}"
if [[ -z "${PORT1}" ]]; then
  PORT1=$("${PYTHON:-python3}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
fi
if [[ -z "${PORT2}" ]]; then
  PORT2=$("${PYTHON:-python3}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
fi
BASE1="http://127.0.0.1:${PORT1}"
BASE2="http://127.0.0.1:${PORT2}"
E2E_DIR="$(mktemp -d)"
DATA_DIR="${E2E_DIR}/data"
mkdir -p "${DATA_DIR}"
LOG1="${E2E_DIR}/app1.log"
LOG2="${E2E_DIR}/app2.log"

PYBIN="${PYTHON:-python3}"
if [[ -n "${PYLIB:-}" ]]; then
  export PYTHONPATH="${PYLIB}"
fi

cleanup() {
  [[ -n "${PID1:-}" ]] && kill "${PID1}" 2>/dev/null || true
  [[ -n "${PID2:-}" ]] && kill "${PID2}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

STEP_DATA_DIR="${DATA_DIR}" STEP_WORKER_POLL=0.2 \
  "${PYBIN}" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT1}" >"${LOG1}" 2>&1 &
PID1=$!
STEP_DATA_DIR="${DATA_DIR}" STEP_WORKER_POLL=0.2 \
  "${PYBIN}" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT2}" >"${LOG2}" 2>&1 &
PID2=$!

wait_http() {
  local url="$1" n=0
  until curl -sf "${url}/healthz" >/dev/null; do
    n=$((n + 1)); [[ ${n} -gt 50 ]] && { echo "启动超时: ${url}"; exit 1; }
    sleep 0.3
  done
}
wait_http "${BASE1}"
wait_http "${BASE2}"
echo "== 两个实例已启动，共享数据卷 ${DATA_DIR}"

fail() { echo "❌ $*"; tail -20 "${LOG1}" "${LOG2}"; exit 1; }

json_field() { sed -n 's/.*"'"$2"'":\([0-9]*\).*/\1/p' <<<"$1" | head -1; }

wait_status() {
  # wait_status <base> <job_id> <status...>：轮询直到作业进入任一期望状态
  local base="$1" job="$2"; shift 2
  local n=0 body
  while true; do
    body=$(curl -sf "${base}/api/daily-stats/${job}") || true
    for want in "$@"; do
      if grep -q "\"status\":\"${want}\"" <<<"${body}"; then echo "${body}"; return 0; fi
    done
    n=$((n + 1)); [[ ${n} -gt 80 ]] && fail "作业 ${job} 未进入状态 $*，最后状态：${body}"
    sleep 0.3
  done
}

# 1. 实例1/实例2 交错导入两个批次（S001 达标 9000，S002 5000）
printf 'subject_id,date,steps\nS001,2026-11-01,9000\nS002,2026-11-01,5000\n' > "${E2E_DIR}/a.csv"
R=$(curl -sf -F "file=@${E2E_DIR}/a.csv" "${BASE1}/api/imports")
echo "== 实例1 导入批次A: ${R}"
printf '受试者号,日期,步数\nS003,2026-11-01,8000\nS001,2026-11-02,100\n' > "${E2E_DIR}/b.csv"
R=$(curl -sf -F "file=@${E2E_DIR}/b.csv" "${BASE2}/api/imports")
echo "== 实例2 导入批次B: ${R}"

# 2. 实例1 受理统计
R=$(curl -sf -H 'Content-Type: application/json' \
  -d '{"start_date":"2026-11-01","end_date":"2026-11-30"}' "${BASE1}/api/daily-stats")
echo "== 受理统计: ${R}"
JOB=$(json_field "${R}" id)
[[ "${JOB}" =~ ^[0-9]+$ ]] || fail "作业号解析失败"

# 3. 受理后再导入一批：不得进入本次统计
printf 'subject_id,date,steps\nS009,2026-11-01,777\n' > "${E2E_DIR}/c.csv"
curl -sf -F "file=@${E2E_DIR}/c.csv" "${BASE2}/api/imports" >/dev/null
echo "== 受理后导入 S009（应被本次统计排除）"

# 4. 两个实例都轮询到 ready，并核对结果
for base in "${BASE1}" "${BASE2}"; do
  body=$(wait_status "${base}" "${JOB}" ready failed invalidated)
  grep -q '"status":"ready"' <<<"${body}" || fail "作业未 ready：${body}（${base}）"
done
echo "== 两个实例均观察到作业 #${JOB} ready"

curl -sf "${BASE2}/api/daily-stats/${JOB}/results" > "${E2E_DIR}/results.json"
"${PYBIN}" - "${E2E_DIR}/results.json" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1]))["results"]
by_day = {r["stat_date"]: r for r in rows}
d1 = by_day["2026-11-01"]
assert d1["person_count"] == 3, d1          # S001/S002/S003，S009 被排除
assert d1["total_steps"] == 9000 + 5000 + 8000, d1
assert d1["ge_8000_count"] == 2, d1         # 9000 与 8000
d2 = by_day["2026-11-02"]
assert d2["person_count"] == 1 and d2["total_steps"] == 100 and d2["ge_8000_count"] == 0, d2
print("   按日聚合/去重/达标计数/受理后导入排除 全部正确")
PY

# 5. 结果 Parquet 可在任一端口下载
curl -sf "${BASE1}/api/daily-stats/${JOB}/download" -o "${E2E_DIR}/stats.parquet"
"${PYBIN}" - "${E2E_DIR}/stats.parquet" <<'PY'
import sys
import pyarrow.parquet as pq
t = pq.read_table(sys.argv[1])
assert t.num_rows == 2, t.to_pydict()
print("   结果 Parquet 下载校验通过")
PY

# 6. 同人同日异值 → 整次失败，冲突列出批次
printf 'subject_id,date,steps\nS001,2026-11-01,9999\n' > "${E2E_DIR}/d.csv"
R=$(curl -sf -F "file=@${E2E_DIR}/d.csv" "${BASE1}/api/imports")
BATCH_D=$(json_field "${R}" batch_id)
R=$(curl -sf -H 'Content-Type: application/json' \
  -d '{"start_date":"2026-11-01","end_date":"2026-11-01"}' "${BASE2}/api/daily-stats")
JOB2=$(json_field "${R}" id)
body=$(wait_status "${BASE1}" "${JOB2}" ready failed invalidated)
grep -q '"status":"failed"' <<<"${body}" || fail "冲突作业应 failed：${body}"
curl -sf "${BASE1}/api/daily-stats/${JOB2}" > "${E2E_DIR}/conflict.json"
"${PYBIN}" - "${E2E_DIR}/conflict.json" "${BATCH_D}" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); batch_d = int(sys.argv[2])
assert d["conflicts"], d
c = d["conflicts"][0]
assert c["subject_id"] == "S001" and c["stat_date"] == "2026-11-01", c
values = {x["steps"]: x["batch_ids"] for x in c["detail"]}
assert set(values) == {9000, 9999}, values
assert batch_d in values[9999], values
print("   冲突整次失败且来源批次明细正确：", values)
PY
# 失败无半份结果
code=$(curl -s -o /dev/null -w '%{http_code}' "${BASE2}/api/daily-stats/${JOB2}/results")
[[ "${code}" == "422" ]] || fail "失败作业结果接口期望 422，实际 ${code}"
echo "== 冲突失败不留半份结果（422）"

# 7. 实例2 受理 S001 撤回：两个实例上作业 #${JOB} 立即永久停供
curl -sf -H 'Content-Type: application/json' -d '{"subject_id":"S001"}' \
  "${BASE2}/api/withdrawals" >/dev/null
n=0
until [[ "$(curl -s -o /dev/null -w '%{http_code}' "${BASE1}/api/daily-stats/${JOB}/results")" == "410" ]]; do
  n=$((n + 1)); [[ ${n} -gt 60 ]] && fail "撤回后作业未停供"
  sleep 0.3
done
for base in "${BASE1}" "${BASE2}"; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "${base}/api/daily-stats/${JOB}/results")
  [[ "${code}" == "410" ]] || fail "期望 410 停供，实际 ${code}（${base}）"
  code=$(curl -s -o /dev/null -w '%{http_code}' "${base}/api/daily-stats/${JOB}/download")
  [[ "${code}" == "410" ]] || fail "期望下载 410，实际 ${code}（${base}）"
done
echo "== 撤回后两实例均永久停供（results/download 均 410）"
curl -sf "${BASE1}/api/daily-stats/${JOB}" | "${PYBIN}" -c '
import json,sys
d=json.load(sys.stdin)
assert d["status"]=="invalidated" and d["invalidations"][0]["subject_id"]=="S001", d
print("   失效原因可查：", d["invalidations"][0]["reason"])
'

# 7b. 等待撤回清理完成（重写结束），此后受理的统计冻结的是不含 S001 的当前版本
n=0
until curl -sf "${BASE2}/api/withdrawals/S001" | grep -q '"status":"completed"'; do
  n=$((n + 1)); [[ ${n} -gt 60 ]] && fail "S001 撤回未完成"
  sleep 0.3
done
echo "== S001 撤回清理完成"

# 8. 无关撤回不影响其他作业：新统计只含 S002/S003/S009，撤回 GHOST 后仍 ready
# （受理接口在有未完成撤回时返回 409，这里做有限重试）
post_stats() {
  local n=0 body code
  until body=$(curl -s -w '\n%{http_code}' -H 'Content-Type: application/json' \
      -d '{"start_date":"2026-11-01","end_date":"2026-11-30"}' "${BASE1}/api/daily-stats"); do
    n=$((n + 1)); [[ ${n} -gt 30 ]] && fail "统计受理请求失败"; sleep 0.3
  done
  code=$(tail -1 <<<"${body}")
  if [[ "${code}" == "409" ]]; then
    n=$((n + 1)); [[ ${n} -gt 30 ]] && fail "统计受理持续 409：${body}"
    sleep 0.3
    post_stats
  else
    [[ "${code}" == "200" ]] || fail "统计受理失败：${body}"
    head -1 <<<"${body}"
  fi
}
R=$(post_stats)
JOB3=$(json_field "${R}" id)
body=$(wait_status "${BASE2}" "${JOB3}" ready failed invalidated)
grep -q '"status":"ready"' <<<"${body}" || fail "新作业未 ready：${body}"
curl -sf -H 'Content-Type: application/json' -d '{"subject_id":"GHOST"}' \
  "${BASE1}/api/withdrawals" >/dev/null
n=0
until curl -sf "${BASE1}/api/withdrawals/GHOST" | grep -q '"status":"completed"'; do
  n=$((n + 1)); [[ ${n} -gt 60 ]] && fail "GHOST 撤回未完成"; sleep 0.3
done
code=$(curl -s -o /dev/null -w '%{http_code}' "${BASE2}/api/daily-stats/${JOB3}/results")
[[ "${code}" == "200" ]] || fail "无关撤回误伤了作业 #${JOB3}（${code}）"
echo "== 无关撤回不影响其他统计作业（仍 200 供数）"

# 9. 数据卷：失效结果文件已删除，stats 目录只保留仍有效的结果
"${PYBIN}" - "${DATA_DIR}" "${JOB}" "${JOB3}" <<'PY'
import sys
from pathlib import Path
root, invalid_job, alive_job = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
files = {p.name for p in (root / "stats").glob("*.parquet")}
assert f"stats-{int(invalid_job):08d}.parquet" not in files, files
assert f"stats-{int(alive_job):08d}.parquet" in files, files
print("   失效结果文件已删除，有效结果保留：", sorted(files))
PY

echo ""
echo "✅ 日统计双实例端到端全部通过。数据卷保留在: ${DATA_DIR}"
trap - EXIT
cleanup
echo "（实例已停止）"
