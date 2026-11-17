# 步数数据湖 · 受试者撤回（被遗忘权）管理服务

运动研究数据管理员在受试者撤回后，需要从分析用数据湖中彻底清除其步数明细。
本服务用 **FastAPI + PyArrow + SQLite** 实现，支持：

- 上传每批 **≤ 256 KiB、≤ 1000 行** 的 CSV（受试者号、日期、非负整数步数），
  **CSV 仅在内存解析**，落盘的只有 Parquet（Snappy 压缩）；
- 按批次下载 Parquet；
- 按受试者号发起撤回并查看进度；
- 受理后**永久拒收**任何含该受试者的整批上传；受理前已开始但尚未发布的导入同样被清除；
- 撤回时重写受影响批次（版本化文件 + 原子替换），完整保留其他人的行；
- 重写期间禁止这些批次的**新**下载，**已开始**的下载因持有旧文件句柄可正常读完；
- 共享卷内含该受试者的旧版本与暂存件全部清除并复核后，才把任务标记为完成；
- 写入失败或进程退出后重启可接续，不会恢复旧下载、不会误报完成；重复撤回接续同一任务；
- **日统计网页**：选择日期范围生成按日统计（人数、步数总和、≥8000 步人数），
  受理时冻结来源批次与撤回名单，撤回立即永久停供相关结果；
- Docker Compose 启动**共用一个数据卷的两个应用实例**，浏览器在任一端口均可操作。

## 目录结构

```
app/
  config.py          # 路径与上传限制（全部位于共享数据卷）
  locks.py           # 跨进程 fcntl.flock 互斥锁（publish/withdraw/stats）
  db.py              # SQLite（WAL）连接、schema 与并发初始化重试
  parquet_store.py   # CSV 内存解析/校验、Parquet 原子写、批次重写、统计读取与结果写盘
  services.py        # 发布、撤回受理、日统计作业、后台清理、崩溃恢复（核心业务）
  main.py            # FastAPI 路由、后台 worker、内嵌单页前端
tests/               # 39 个 pytest 用例（校验/撤回/恢复/并发/日统计）
scripts/
  e2e.sh             # 双实例共享卷端到端自测（撤回）
  e2e_daily_stats.sh # 双实例共享卷端到端自测（日统计与撤回联动）
  restart_recovery.sh# 崩溃 → 重启 → 接续完成验证
Dockerfile
docker-compose.yml   # app1:8001、app2:8002，共享命名卷 stepdata
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器访问 http://localhost:8001 或 http://localhost:8002
```

两个容器挂载同一命名卷 `stepdata` 到 `/data`，其中包含：

```
/data/
  stepstore.db(-wal/-shm)      # SQLite 数据库（WAL 模式）
  locks/{publish,withdraw,stats}.lock # 跨进程文件锁
  staging/import-<uuid>.parquet# 暂存 Parquet（发布成功即移走）
  stats/stats-00000001.parquet # 日统计结果（失效即删除，只保留可供数的结果）
  batches/batch-00000001/
      v1.parquet               # 版本化批次文件，撤回重写生成 v2、v3…
```

## 本地运行（无 Docker）

```bash
pip install -r requirements.txt
STEP_DATA_DIR=./data uvicorn app.main:app --port 8000
# 第二个实例换端口、同一 STEP_DATA_DIR 即可模拟双实例
STEP_DATA_DIR=./data uvicorn app.main:app --port 8001
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/imports` | multipart 上传 CSV；>256 KiB 返回 413，内容非法 422，命中撤回 409 |
| GET  | `/api/batches` | 批次列表（含状态、行数、受试者） |
| GET  | `/api/batches/{id}/download` | 下载 Parquet；重写中返回 409 |
| POST | `/api/withdrawals` | `{"subject_id":"S001"}` 受理撤回（重复调用返回同一任务） |
| GET  | `/api/withdrawals` | 全部撤回任务与进度 |
| GET  | `/api/withdrawals/{subject_id}` | 单个任务进度 |
| POST | `/api/daily-stats` | `{"start_date":"2026-11-01","end_date":"2026-11-30"}` 受理日统计；重写中 409 |
| GET  | `/api/daily-stats` | 全部统计作业（状态、来源行数、失效原因摘要） |
| GET  | `/api/daily-stats/{id}` | 作业详情：冻结来源批次、排除名单、冲突明细、失效记录 |
| GET  | `/api/daily-stats/{id}/results` | 按日结果；失效 410、失败 422、计算中 409 |
| GET  | `/api/daily-stats/{id}/download` | 下载结果 Parquet；失效 410 |
| GET  | `/healthz` | 健康检查与本次启动恢复统计 |

CSV 表头支持英文 `subject_id,date,steps` 与中文 `受试者号,日期,步数`。

## 关键设计与正确性论证

### 1. 受理与发布的全序（永久拒收 + 在途导入）

- `publish_import`（导入发布）与 `accept_withdrawal`（撤回受理）都先获取
  **同一把 `publish.lock` 排他锁**，因此二者在系统中存在确定先后顺序：
  - 发布先于受理：该批进入数据湖，但会被随后的撤回重写清除；
  - 受理先于发布：发布在锁内查询 `withdrawals` 命中，整批 **409 拒绝**，不落任何批次数据。
- “受理前开始但尚未发布”的暂存导入：撤回最终清扫阶段扫描 `staging/`，
  含该受试者的暂存 Parquet 直接删除，对应 `imports` 行标记 `rejected`。

### 2. 批次重写与下载封锁

- 撤回处理把批次在同一事务内翻转为 `rewriting`，下载接口只对 `published`
  放行，因此重写窗口内的**新**下载得到 409；崩溃停在 `rewriting` 时旧下载同样无法恢复。
- 重写采用“写新版本 `vN.parquet`（同目录临时文件 + fsync + `os.replace`）→
  事务内切换 `current_path` → 删除旧版本”。已经开始的下载持有旧文件的打开句柄，
  Unix 语义下 unlink 不影响其读完（有测试覆盖）。

### 3. 完成前的强制清扫与复核

任务只有在以下全部完成后才置为 `completed`：

1. 每个受影响批次重写并剔除该受试者（保留其他人全部行）；
2. 删除批次目录中的历史旧版本；删除/拒收含该受试者的暂存件；
3. 对共享卷 `batches/` 与 `staging/` 内**所有** Parquet 复核，
   只要还有一个文件含该受试者就抛错并保持 `failed`，**绝不误报完成**。

### 4. 崩溃恢复（重启接续）

启动时 `recover_on_startup()`：

- 清理 `.tmp.*` 半截写入；
- 无关联批次/文件丢失的暂存导入标记 `rejected` 并删除文件；
- `rewriting` 批次保持封锁，由后台 worker 基于当前版本**幂等重写**接续；
- 清理无数据库引用的孤儿 Parquet；
- 已 `completed` 的任务不会被重复处理；重复 POST 撤回返回同一任务行。

后台 worker 两个实例各跑一个，通过 `withdraw.lock` 保证同一时刻只有一个实例执行清理，
另一个实例自动空转；任务失败会保留 `last_error` 并在后续轮询重试。

### 5. CSV 不落盘

上传接口最多读取 256 KiB+1 字节到内存，随后用 `io.StringIO` 在内存解析；
写入磁盘的第一个产物是 `staging/import-<uuid>.parquet`。测试断言数据卷内不存在任何 `.csv`。

### 6. 日统计：冻结快照、冲突失败与撤回联动

- **受理即冻结**：`POST /api/daily-stats` 在 `publish.lock` 内快照受理时刻全部
  `published` 批次（路径 + 文件大小 + mtime_ns 签名）、受理时已撤回者排除名单，
  并构建作业覆盖受试者表 `daily_stats_job_subjects`。**受理之后的导入不在快照内**，
  天然不加入本次统计；存在 `rewriting` 批次时直接 409，暂不受理。
- **慢统计不阻塞业务**：重计算在独立的 `stats.lock` 下由后台 worker 执行，不长占
  `publish.lock`，因此统计期间导入与撤回受理均可继续（有并发测试覆盖）。
- **同人同日规则**：同一受试者同一天的多条同值记录只计一次；出现异值则**整次失败**，
  `daily_stats_conflicts` 逐组记录每个值及其来源批次，前端与详情接口可直接列出；
  失败只落失败状态与冲突明细，不产生任何结果行或结果文件。
- **结果、来源与供数资格一起生效**：结果 Parquet 先原子写到 `stats/`，再在
  `publish.lock` 事务内复核（作业未被失效 + 冻结文件签名未变）后，同事务写入
  `daily_stats_results` 并翻转为 `ready`；复核不过则删除结果文件并置 `failed`，
  **失败不留半份结果**。
- **撤回立即永久停供**：撤回受理事务内，依据覆盖表精确命中相关作业（无关撤回不影响），
  同事务翻转为 `invalidated`、删除结果行、登记失效原因，事务提交后删除结果 Parquet。
  统计期间受理的撤回会让在途作业在发布复核时命中 `withdrawals` 而**不予发布**，须重新生成。
- **重启后仍成立**：启动恢复会对停机期间遗漏的撤回做失效兜底（仍按覆盖表精确判定）、
  删除无 `ready` 引用的孤儿结果、把丢失结果文件的 `ready` 作业转 `failed`；
  `processing` 作业由 worker 幂等接续。

### 7. 双实例交错与锁体系

- 两个容器挂载同一命名卷，SQLite（WAL + busy_timeout）、Parquet 与三把 flock 均在卷内；
  两个实例的 worker 通过 `withdraw.lock` / `stats.lock` 互斥，同一类任务全局只跑一个，
  另一个实例空转；在任一端口受理的统计/撤回，另一端立即可见同一状态与结果。

## 自测

```bash
pip install -r requirements.txt pytest httpx
pytest tests/ -q
PYLIB=$(python -c 'import site;print(site.getsitepackages()[0])') bash scripts/e2e.sh
PYLIB=... bash scripts/e2e_daily_stats.sh
PYLIB=... bash scripts/restart_recovery.sh
```

测试覆盖：上传大小/行数/字段/编码校验、CSV 不落盘、撤回重写内容正确性、
重写窗口 409 封锁、已开始下载不受影响、旧版本清理与全卷复核、
重复撤回幂等、在途暂存导入被清扫、模拟崩溃后重启接续、双 worker 锁互斥、
受理/发布并发全序；日统计的按日聚合与同值去重、≥8000 计数、受理快照排除后续导入、
同人同日异值整次失败与来源批次明细、重写中不受理、撤回即时停供（410）与结果文件删除、
在途统计遇撤回不发布、慢统计期间业务不阻塞、无关撤回不误伤、重启恢复与孤儿清理等。
