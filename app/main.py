"""FastAPI 入口：HTTP 路由、后台清理 worker、单页前端。"""
from __future__ import annotations

import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from . import config, db, services
from .parquet_store import ValidationError, parse_csv

_worker_stop = threading.Event()


def _worker_loop() -> None:
    """后台线程：持续接续撤回任务与日统计作业，两个实例各跑一个，靠文件锁互斥。"""
    while not _worker_stop.is_set():
        did_work = False
        try:
            did_work = services.process_due_withdrawal() or did_work
        except Exception:
            # 失败任务保留 failed 状态与 last_error，下一轮继续重试，绝不在此误报完成
            pass
        try:
            did_work = services.process_due_daily_stats() or did_work
        except Exception:
            # 统计作业失败已落 failed 状态与原因，下一轮重新受理生成
            pass
        _worker_stop.wait(
            config.WORKER_POLL_SECONDS if did_work else config.WORKER_BACKOFF_SECONDS
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    config.ensure_dirs()
    db.init_db()
    recovery = services.recover_on_startup()
    app.state.recovery = recovery
    if config.WORKER_ENABLED:
        t = threading.Thread(target=_worker_loop, name="withdrawal-worker", daemon=True)
        t.start()
    yield
    _worker_stop.set()


app = FastAPI(title="步数数据湖撤回管理", version="1.0.0", lifespan=lifespan)


@app.exception_handler(services.ServiceError)
async def service_error_handler(request: Request, exc: services.ServiceError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "recovery": getattr(app.state, "recovery", {})}


@app.post("/api/imports")
async def upload_csv(file: UploadFile = File(...)) -> dict:
    # 限制读取 256 KiB + 1 字节，超出立即拒绝；全程只在内存，绝不落盘
    max_bytes = config.MAX_UPLOAD_BYTES
    data = await file.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"上传文件超过 {max_bytes} 字节（256 KiB）限制",
        )
    if not data:
        raise HTTPException(status_code=400, detail="上传内容为空")
    try:
        table = parse_csv(data)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        result = services.publish_import(table)
    except services.ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc
    return {"detail": "导入成功", **result}


@app.get("/api/batches")
def get_batches() -> dict:
    return {"batches": services.list_batches()}


@app.get("/api/batches/{batch_id}/download")
def download_batch(batch_id: int) -> FileResponse:
    row, path = services.get_downloadable_batch(batch_id)
    # FileResponse 打开文件句柄后，重写/删除不影响已经开始的下载
    return FileResponse(
        path,
        media_type=config.PARQUET_MEDIA_TYPE,
        filename=f"batch-{batch_id:08d}.parquet",
    )


@app.post("/api/withdrawals")
async def create_withdrawal(payload: dict) -> dict:
    subject_id = (payload or {}).get("subject_id")
    if not subject_id or not isinstance(subject_id, str) or not subject_id.strip():
        raise HTTPException(status_code=400, detail="subject_id 必填")
    subject_id = subject_id.strip()
    result = services.accept_withdrawal(subject_id)
    return {"detail": "撤回已受理", **result}


@app.get("/api/withdrawals")
def get_withdrawals() -> dict:
    return {"withdrawals": services.list_withdrawals()}


@app.get("/api/withdrawals/{subject_id}")
def get_withdrawal(subject_id: str) -> dict:
    result = services.get_withdrawal(subject_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"受试者 {subject_id} 无撤回记录")
    return result


# ------------------------------------------------------------ 日统计作业

@app.post("/api/daily-stats")
async def create_daily_stats(payload: dict) -> dict:
    payload = payload or {}
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    if not isinstance(start_date, str) or not isinstance(end_date, str):
        raise HTTPException(status_code=400, detail="start_date、end_date 必填（YYYY-MM-DD）")
    try:
        result = services.create_daily_stats(start_date.strip(), end_date.strip())
    except services.ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc
    return {"detail": "统计申请已受理，来源批次已冻结", **result}


@app.get("/api/daily-stats")
def list_daily_stats() -> dict:
    return {"jobs": services.list_daily_stats_jobs()}


@app.get("/api/daily-stats/{job_id}")
def get_daily_stats(job_id: int) -> dict:
    result = services.get_daily_stats_job(job_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"统计作业 {job_id} 不存在")
    return result


@app.get("/api/daily-stats/{job_id}/results")
def get_daily_stats_results(job_id: int) -> dict:
    try:
        rows = services.get_daily_stats_results(job_id)
    except services.ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc
    return {"job_id": job_id, "results": rows}


@app.get("/api/daily-stats/{job_id}/download")
def download_daily_stats(job_id: int) -> FileResponse:
    try:
        row, path = services.get_stats_download(job_id)
    except services.ServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc
    return FileResponse(
        path,
        media_type=config.PARQUET_MEDIA_TYPE,
        filename=f"daily-stats-{job_id:08d}.parquet",
    )


INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>步数数据湖 · 撤回与日统计管理</title>
<style>
  :root { --bd:#d0d7de; --bg:#f6f8fa; --ok:#1a7f37; --warn:#9a6700; --err:#cf222e; }
  * { box-sizing: border-box; }
  body { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif; margin:0; color:#1f2328; }
  header { background:#24292f; color:#fff; padding:14px 24px; }
  header h1 { margin:0; font-size:18px; }
  header small { color:#adbac7; }
  main { max-width:1080px; margin:24px auto; padding:0 16px; display:grid; gap:20px; }
  section { border:1px solid var(--bd); border-radius:8px; background:#fff; padding:18px; }
  h2 { margin:0 0 12px; font-size:16px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th,td { border-bottom:1px solid var(--bd); padding:7px 8px; text-align:left; }
  th { background:var(--bg); }
  button { padding:7px 16px; border:1px solid #24292f; background:#24292f; color:#fff;
           border-radius:6px; cursor:pointer; font-size:13px; }
  button:hover { background:#000; }
  button:disabled { opacity:.5; cursor:not-allowed; }
  input[type=text] { padding:7px 10px; border:1px solid var(--bd); border-radius:6px; width:260px; }
  input[type=file] { font-size:13px; }
  .msg { margin-top:10px; padding:9px 12px; border-radius:6px; font-size:13px; white-space:pre-wrap; }
  .ok { background:#dafbe1; color:var(--ok); } .err { background:#ffebe9; color:var(--err); }
  .warn { background:#fff8c5; color:var(--warn); }
  .pill { padding:2px 9px; border-radius:999px; font-size:12px; }
  .published { background:#dafbe1; color:var(--ok); }
  .rewriting { background:#fff8c5; color:var(--warn); }
  .completed { background:#dafbe1; color:var(--ok); }
  .processing,.accepted,.pending { background:#ddf4ff; color:#0969da; }
  .failed { background:#ffebe9; color:var(--err); }
  .ready { background:#dafbe1; color:var(--ok); }
  .invalidated { background:#ffebe9; color:var(--err); }
  a.dl { color:#0969da; text-decoration:none; } a.dl.disabled { color:#8c959f; pointer-events:none; }
  .muted { color:#656d76; font-size:12px; }
  code { background:var(--bg); padding:1px 5px; border-radius:4px; }
</style>
</head>
<body>
<header><h1>运动研究步数数据湖 · 撤回（被遗忘权）与日统计管理</h1>
<small>CSV ≤ 256 KiB / 1000 行，仅内存解析；统计受理即冻结批次；Parquet 持久化，双实例共享数据卷</small></header>
<main>
  <section>
    <h2>① 导入 CSV 批次（列：subject_id,date,steps 或 受试者号,日期,步数）</h2>
    <input type="file" id="csvfile" accept=".csv,text/csv">
    <button onclick="upload()">上传并发布</button>
    <div id="upmsg"></div>
  </section>

  <section>
    <h2>② 受试者撤回</h2>
    <input type="text" id="subject" placeholder="受试者号，例如 S001">
    <button onclick="withdraw()">发起撤回</button>
    <div id="wdmsg"></div>
  </section>

  <section>
    <h2>③ 撤回进度 <button style="float:right" onclick="refreshAll()">刷新</button></h2>
    <table><thead><tr><th>ID</th><th>受试者号</th><th>状态</th><th>批次进度</th><th>删除文件</th><th>更新时间</th><th>错误</th></tr></thead>
    <tbody id="wdbody"><tr><td colspan="7" class="muted">暂无</td></tr></tbody></table>
  </section>

  <section>
    <h2>④ 日统计（受理时冻结已发布批次；同人同日异值整次失败）</h2>
    <label>起始 <input type="date" id="ds_start"></label>
    <label>结束 <input type="date" id="ds_end"></label>
    <button onclick="createStats()">生成统计</button>
    <div id="dsmsg"></div>
    <table style="margin-top:12px"><thead><tr>
      <th>作业</th><th>日期范围</th><th>状态</th><th>来源行数</th><th>更新时间</th>
      <th>结果 / 失效原因</th><th>下载</th>
    </tr></thead>
    <tbody id="dsbody"><tr><td colspan="7" class="muted">暂无</td></tr></tbody></table>
    <div id="dsdetail"></div>
  </section>

  <section>
    <h2>⑤ 批次与下载</h2>
    <table><thead><tr><th>批次</th><th>状态</th><th>行数</th><th>受试者</th><th>创建时间</th><th>下载</th></tr></thead>
    <tbody id="batchbody"><tr><td colspan="6" class="muted">暂无</td></tr></tbody></table>
  </section>
</main>
<script>
function show(el, cls, text){ el.className = 'msg ' + cls; el.textContent = text; }
function esc(s){ return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

async function upload(){
  const f = document.getElementById('csvfile').files[0];
  const el = document.getElementById('upmsg');
  if(!f){ show(el,'err','请先选择 CSV 文件'); return; }
  if(f.size > 256*1024){ show(el,'err','文件超过 256 KiB：' + f.size + ' 字节'); return; }
  const fd = new FormData(); fd.append('file', f);
  try{
    const r = await fetch('/api/imports', {method:'POST', body:fd});
    const j = await r.json();
    if(!r.ok) throw new Error(j.detail || r.status);
    show(el,'ok','导入成功 → 批次 #' + j.batch_id + '，' + j.row_count + ' 行，受试者：' + j.subjects.join(', '));
    refreshAll();
  }catch(e){ show(el,'err','导入失败：' + e.message); }
}

async function withdraw(){
  const v = document.getElementById('subject').value.trim();
  const el = document.getElementById('wdmsg');
  if(!v){ show(el,'err','请输入受试者号'); return; }
  try{
    const r = await fetch('/api/withdrawals', {method:'POST',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify({subject_id:v})});
    const j = await r.json();
    if(!r.ok) throw new Error(j.detail || r.status);
    show(el,'ok','撤回已受理：' + j.subject_id + '（任务 #' + j.id + '，状态 ' + j.status + '）');
    refreshAll();
  }catch(e){ show(el,'warn','请求失败：' + e.message); }
}

async function createStats(){
  const s = document.getElementById('ds_start').value;
  const e = document.getElementById('ds_end').value;
  const el = document.getElementById('dsmsg');
  if(!s || !e){ show(el,'err','请选择起止日期'); return; }
  if(s > e){ show(el,'err','起始日期不得晚于结束日期'); return; }
  try{
    const r = await fetch('/api/daily-stats', {method:'POST',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify({start_date:s, end_date:e})});
    const j = await r.json();
    if(!r.ok) throw new Error(j.detail || r.status);
    show(el,'ok','统计已受理 → 作业 #' + j.id + '（状态 ' + j.status + '），后台计算中');
    refreshAll();
  }catch(err){ show(el,'warn','受理失败：' + err.message); }
}

async function viewStats(id){
  const el = document.getElementById('dsdetail');
  try{
    const j = await (await fetch('/api/daily-stats/'+id)).json();
    let html = '<h2 style="font-size:14px;margin:14px 0 8px">作业 #'+id+' 详情</h2>';
    html += '<p class="muted">来源依据批次：'+(j.sources.map(s=>'#'+s.batch_id).join(', ')||'（无）')+
            '；受理时已排除撤回者：'+(j.excluded_subjects.map(esc).join(', ')||'（无）')+'</p>';
    if(j.invalidations.length){
      html += '<div class="msg err">永久停供：'+j.invalidations.map(x=>esc(x.reason)).join('；')+'</div>';
    }
    if(j.conflicts.length){
      html += '<div class="msg err">同人同日异值冲突（整次失败）：</div><table><thead><tr><th>受试者</th><th>日期</th><th>值 → 来源批次</th></tr></thead><tbody>'+
        j.conflicts.map(c=>'<tr><td><code>'+esc(c.subject_id)+'</code></td><td>'+esc(c.stat_date)+'</td><td>'+
          c.detail.map(d=>esc(d.steps)+' 步 ← 批次 '+d.batch_ids.map(b=>'#'+b).join(', ')).join('；')+
          '</td></tr>').join('')+'</tbody></table>';
    }
    if(j.status==='ready'){
      const rr = await (await fetch('/api/daily-stats/'+id+'/results')).json();
      if(!rr.results.length){ html += '<p class="muted">该日期范围内没有有效数据。</p>'; }
      else html += '<table><thead><tr><th>日期</th><th>人数</th><th>步数总和</th><th>≥8000 步人数</th></tr></thead><tbody>'+
        rr.results.map(r=>'<tr><td>'+esc(r.stat_date)+'</td><td>'+r.person_count+'</td><td>'+r.total_steps+
          '</td><td>'+r.ge_8000_count+'</td></tr>').join('')+'</tbody></table>';
    } else if(j.status==='failed' && !j.conflicts.length){
      html += '<div class="msg err">失败原因：'+esc(j.error_reason||'')+'</div>';
    }
    el.innerHTML = html;
  }catch(e){ el.innerHTML = '<div class="msg err">详情加载失败：'+esc(e.message)+'</div>'; }
}

async function refreshStats(){
  const tb = document.getElementById('dsbody');
  try{
    const j = await (await fetch('/api/daily-stats')).json();
    if(!j.jobs.length){ tb.innerHTML = '<tr><td colspan="7" class="muted">暂无</td></tr>'; return; }
    tb.innerHTML = j.jobs.map(x => {
      let info;
      if(x.status==='ready') info = '<a class="dl" href="#" onclick="viewStats('+x.id+');return false;">查看每日结果</a>';
      else if(x.status==='invalidated') info = '<span title="'+esc(x.error_reason||'')+'">已永久停供（点击查看）<br><a class="dl" href="#" onclick="viewStats('+x.id+');return false;">失效原因</a></span>';
      else if(x.status==='failed') info = '<a class="dl" href="#" onclick="viewStats('+x.id+');return false;">失败详情 / 冲突来源</a>';
      else info = '<span class="muted">计算中…</span>';
      const dl = x.status==='ready'
        ? '<a class="dl" href="/api/daily-stats/'+x.id+'/download">下载 Parquet</a>'
        : '<a class="dl disabled">不可下载</a>';
      return '<tr><td>#'+x.id+'</td><td>'+esc(x.start_date)+' ~ '+esc(x.end_date)+
        '</td><td><span class="pill '+x.status+'">'+x.status+'</span></td><td>'+x.source_row_count+
        '</td><td>'+esc(x.updated_at)+'</td><td>'+info+'</td><td>'+dl+'</td></tr>';
    }).join('');
  }catch(e){ tb.innerHTML = '<tr><td colspan="7" class="err">加载失败：'+esc(e.message)+'</td></tr>'; }
}

async function refreshAll(){
  const [wb, bb] = [document.getElementById('wdbody'), document.getElementById('batchbody')];
  try{
    const w = await (await fetch('/api/withdrawals')).json();
    if(!w.withdrawals.length){ wb.innerHTML = '<tr><td colspan="7" class="muted">暂无</td></tr>'; }
    else wb.innerHTML = w.withdrawals.map(x => '<tr><td>'+x.id+'</td><td><code>'+esc(x.subject_id)+
      '</code></td><td><span class="pill '+x.status+'">'+x.status+'</span></td><td>'+
      x.batches_done+' / '+x.batches_total+'</td><td>'+x.files_removed+'</td><td>'+esc(x.updated_at)+
      '</td><td class="muted">'+esc(x.last_error||'')+'</td></tr>').join('');
  }catch(e){ wb.innerHTML = '<tr><td colspan="7" class="err">加载失败：'+esc(e.message)+'</td></tr>'; }
  try{
    const b = await (await fetch('/api/batches')).json();
    if(!b.batches.length){ bb.innerHTML = '<tr><td colspan="6" class="muted">暂无</td></tr>'; }
    else bb.innerHTML = b.batches.map(x => {
      const dl = x.status==='published'
        ? '<a class="dl" href="/api/batches/'+x.id+'/download">下载 Parquet</a>'
        : '<a class="dl disabled" title="撤回重写中，禁止新下载">封锁中</a>';
      return '<tr><td>#'+x.id+'</td><td><span class="pill '+x.status+'">'+x.status+
      '</span></td><td>'+x.row_count+'</td><td>'+esc(x.subjects.join(', '))+'</td><td>'+
      esc(x.created_at)+'</td><td>'+dl+'</td></tr>';
    }).join('');
  }catch(e){ bb.innerHTML = '<tr><td colspan="6" class="err">加载失败：'+esc(e.message)+'</td></tr>'; }
  refreshStats();
}
refreshAll();
setInterval(refreshAll, 2000);
</script>
</body></html>"""
