# fn-scheduler

任务调度应用，提供 Web UI 配置定时/条件触发脚本，并使用 SQLite 持久化任务与执行历史。

## 功能概览
- 可配置两种触发方式：
  - **定时**：标准 5 字段 Cron 表达式（分钟 小时 日 月 周）。
  - **事件**：支持三种子类型——条件脚本轮询、系统开机触发、系统关机触发。
- 支持设置是否启用、运行账号标识、前置任务依赖以及脚本内容（账号仅可从系统组 0 / 1000 / 1001 的成员中选择，并在支持的系统上以该账号身份执行任务）。
- 任务执行历史可查看/删除/清空，并展示输出日志。
- SQLite 数据库存储任务与 `task_results` 记录。
- 单进程后端内置调度引擎 + 原生 HTTP/静态文件服务，无第三方依赖。
- 列表多选后可批量删除、启用/停用或立即运行任务。

## 运行方式
### 通过应用脚本
默认 `cmd/main` 会调用 `app/server/start.sh`，环境变量说明：
- `SCHEDULER_PORT`：HTTP 服务端口，默认 `28256`。
- `SCHEDULER_DB_PATH`：SQLite 路径，默认为 `${TRIM_PKGVAR}/scheduler.db`。
- `SCHEDULER_TASK_TIMEOUT`：任务脚本最长执行秒数（默认 900）。
- `SCHEDULER_CONDITION_TIMEOUT`：事件条件脚本检测超时（默认 60）。

### 手动启动（开发/调试）
```bash
cd fn-scheduler/app/server
python3 scheduler_service.py --host 0.0.0.0 --port 28256 --db ./scheduler.db
```
启动后访问 `http://<host>:28256/` 打开前端页面。

## 数据库结构
见 `docs/design.md`，包含 `tasks` 与 `task_results` 表字段说明及 REST API 列表。

## 注意事项
- 任务脚本默认通过 `/bin/bash -c`（Linux）或 `powershell`（Windows）执行。服务以 root 运行时会在启动子进程前切换为所选账号（仅支持系统组 0/1000/1001 的成员）；若服务本身并非 root，则需选择与当前用户一致的账号；Windows 环境下默认使用当前登录账号，无法在任务中切换。
- 前置任务依赖会在依赖任务最近一次成功后才放行执行。
- 事件任务的条件脚本需快速返回（默认 60s 超时），长时间阻塞会影响检查频率。
- 系统开/关机事件依赖服务启动与停止过程触发，请确保退出前给予任务足够时间执行完毕。
