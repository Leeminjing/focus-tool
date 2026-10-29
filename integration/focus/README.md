# Focus 真实 Run 小范围接入验证

本目录使用本机已有的 Focus Python 环境和真实源码，执行一个小范围 Run 链路。核心运行时仍只依赖 Go／Python 标准库；本联调额外复用 Focus 已有的 LangGraph、SQLAlchemy、psycopg 等依赖，以及本机已缓存的 `postgres:17-alpine` Docker 镜像。脚本不会下载镜像、修改 Focus 源码、读写桌面正式数据库或调用模型 API。

## 自动 Patrol 唤醒（2026-10-29）

当前应用闭环入口是 `run_automatic.py`，调用 `PatrolResultService`，结果落盘后自动唤醒真实 Patrol／Kernel／RunPool。脚本只启动场景、控制本地叶子工具 fixture 并断言，不调用后继 `harness.schedule()`，A 的关键场景不等待 B 的结果。单宿主、本地确定性 provider，Focus 源码仍固定 `340bf2b8`。

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/run_automatic.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --out results/local/automatic-patrol.json
```

场景覆盖独立 Context、连续结果合并、消费中迟到结果、inbox 快照之后到达的结果、同一真实 Loop 两个 Context、实际 RunPool 容量等待、失权取消清理、inbox 提交而通知尚未发出时硬杀／无通知重启。shared-Loop fixture 在首个决策前建立真实 grant／membership，两个激活使用真实 read lease，独立服务 job workspace 的 write grant／lease 保持分离；不声称这个研究 service-principal 已等同生产 Context 文件权限系统。

`test_wake_controls.py` 另用真实适配类配 stub 控制依赖，验证部分 drain 的取消所有权、瞬时失败重试、关闭错误聚合、诊断回调隔离、失败 Run 的结算及页内归属；它不连接数据库，和真实 PostgreSQL 用例分开报告。全部可通过 `scripts/verify.py --focus-root <checkout> --focus-python <existing-python> --focus-patrol --focus-automatic` 顺序复现。合同见 [自动唤醒](../../docs/automatic-patrol.md)，证据见 [结果](../../docs/automatic-patrol-results.md)。下述旧七例保留用于恢复边界回归，其中手动调度只属于原场景控制。

## 独立 Patrol 性能用例

`benchmark_patrol.py` 经过上述自动服务和真实 Patrol 组件，使用 `patrol_benchmark_tools.py` 的有限固定工作量。barrier 只等待同 Context 的原始工具批，B 独立自然结束；event 可以提前纠偏，必要扫描场景则必须等扫描完成并持久消费。默认每臂 1 次 warmup、10 次正式测量，轮换场景和模式顺序。采集时单独运行，不与验证或其他基准并行：

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/benchmark_patrol.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --out results/local/patrol-performance
python integration/focus/benchmark_patrol.py --analyze results/local/patrol-performance
```

输出目录必须不存在。原始数据保留阶段调用、循环次数、产物读回、取消终态、typed checkpoint／回执和全部样本；失败不替换成成功样本。详见 [测量合同](../../docs/patrol-performance-method.md) 和 [实测报告](../../docs/patrol-performance-results.md)。

## Patrol 跨激活接入（2026-10-29）

新增 `run_patrol.py`，固定同一 Focus `340bf2b8`，使用真实 `LoopObservationService`、`PortfolioPatrol`、`LoopKernel`、`ContextRunPool`、`LoopCoordinator`、`LoopWaveDispatcher`、`RunOwnershipPolicy`、`LoopRunWorkspaceBinder`、`LoopRunExecutionBoundary` 和 `execute_prepared_run`／`run_agent`。本地确定性 agent／task-progress provider 与 Desktop launch port 是研究适配；没有模型调用，也未启动 Desktop HTTP 或完整 supervisor。

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/run_patrol.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --out results/local/patrol-integration.json
```

七个场景：两个 Context 跨激活提前纠偏、runtime 落盘而 inbox 尚未写入时硬杀、决策 checkpoint／回执落盘而 runtime 尚未 ack 时硬杀、消费回执／outbox 已提交但后续动作尚未执行时硬杀、runtime revision 领先 SQL owner 时硬杀、queued 的允许恢复和撤权恢复。六个恢复场景都实际重启 daemon／桥接服务，不使用假传输或只抛异常来替代重启。

每个 Context 有自己的 Loop／激活工作区，两个 Loop 共用同一个真实 ContextRunPool 和同一个 Go runtime。长期服务使用独立工具工作区、真实 lease，以及研究专用 `DesktopRun(kind="tool_service")` 租约锚点。此锚点不是模型激活，也不是已接入生产的服务身份系统。激活 lease 会释放，后台任务不持有原激活 token。原生 `AgentMessageDelivery` 仅证明 typed inbox 包含；另存精确决策 checkpoint、语义回执和稳定动作 outbox，之后才 ack。

父进程在 marker 确认后分别杀死真实 Go daemon 和桥接 Python 子进程，无 graceful close；恢复案例的执行在中断前已完成或尚未派发，不把运行中 crash 伪装成 exactly-once。运行中恢复为 unknown 的任务继续隔离，服务拒绝重新取得该 job workspace。研究表仅在本次隔离 PostgreSQL 中建立，没有迁移 Focus 主仓数据库。

完整证据与限制见 [Patrol 验收记录](../../docs/patrol-results.md)。全量本地检查可使用 `python scripts/verify.py --focus-root <checkout> --focus-python <existing-python> --focus-patrol`。以下记录保留早期 `run_live.py` 单次等待中 Run 的回归范围，不代表新增 Patrol 用例的完整范围。

## 已实际执行

验证日期：2026-10-27。Focus 检出提交为 [`340bf2b8607dfb536d9376d400a02b122a4542bb`](https://github.com/Leeminjing/Focus/commit/340bf2b8607dfb536d9376d400a02b122a4542bb)，它是已核验 remote main [`853969375f9df46936552809dce3d543dd2403e4`](https://github.com/Leeminjing/Focus/commit/853969375f9df46936552809dce3d543dd2403e4) 的合并父提交。报告记录实际执行版本，未将新 main 冒充已测试版本。

本机命令（路径按实际安装调整）：

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/run_live.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --out results/focus-integration-2026-10-27.json
```

实际结果：`allowed`、`permission_revoked`、`lease_released` 三例通过。原始记录见 [联调报告](../../results/focus-integration-2026-10-27.json)。

2026-10-28 在同一固定 Focus 源码和隔离数据库条件下，对分页／压缩版 runtime 再次通过三例，见 [本轮记录](../../results/focus-retention-integration-2026-10-28.json)。这不扩大下述 Patrol／生产覆盖范围。

常驻池改动后同一链路再分别以默认新进程模式和 `--python-workers 2` 通过三例，见 [默认模式](../../results/python-pool-focus-2026-10-28.json)／[池模式](../../results/python-pool-focus-reuse-2026-10-28.json)。启用该选项也将本地测试工具可信登记为 `reuse=True`，并在关闭后断言池 live／reserved 均为零。这三例验证真实治理与持久化边界；跨调用复用、故障和状态清理另外由核心集成测试覆盖。

| 用例 | 实际验证 |
|---|---|
| allowed | Focus Run 提交异步工具，真实 Go／Python 执行器写入独立临时文件；等待中的 Focus graph node 被完成事件唤醒；写入 typed checkpoint、确认 inbox receipt 后才 ack 运行时结果 |
| permission_revoked | 提交后、真正执行前将当前模式降为 read-only；真实 AccessPolicyMiddleware 刷新模式并返回 FILE_POLICY_DENIED；没有进程 PID，也未生成目标文件 |
| lease_released | 提交后、真正执行前通过真实 WorkspaceLeaseManager 释放租约；工具边界复核 lease/token 时拒绝；没有进程 PID，也未生成目标文件 |

每例均使用真实 Focus `run_agent`、`RunManager`、`MemoryStreamBridge`、`ToolExecutionMiddleware`／PostgreSQL submission ledger、`WorkspaceLeaseManager`、`DurableInboxMiddleware`／PostgreSQL inbox receipt、Focus checkpointer 工厂／`AsyncPostgresSaver`，以及 typed-history 的 `synchronize_items`／`replace_execution_items`。报告包含实际 lease 检查次数、精确 checkpoint ID、投递 receipt 数、submission ledger attempt 数和运行时终态。

## 流程与源码依据

1. 独立 Docker 容器在随机 `127.0.0.1` 端口运行临时 PostgreSQL，按 Focus 真实 ORM 定义建立桌面表。此链路不使用向量存储；其动态 vector 表不创建，checkpoint 表由真实 saver 的 setup 初始化。未用 SQLite 或假数据库替代 PostgreSQL 语义。
2. 由真实 [`derive_security_context`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/packages/harness/focus/security/context.py) 建立治理身份；[`WorkspaceLeaseManager`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/app/desktop/workspaces/coordination/leases.py) 实际签发与复核带 fencing token 的 lease。
3. 真实 [`AccessPolicyMiddleware`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/packages/harness/focus/security/middleware.py) 包装提交边界，并在 Go 调度器的执行前 callback 再次检查实际文件写入 effect。提交工具明确叫 `start_write`，其 ToolMessage 只表示提交。
4. 真实 [`ToolExecutionMiddleware`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/packages/harness/focus/runtime/tool_attempts.py) 与 [`ToolExecutionLedger`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/app/desktop/runs/execution_attempts/tool.py) 保存提交意图和 canonical ToolMessage；后台执行事实由本仓库运行时独立记账。
5. 真实 [`run_agent`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/packages/harness/focus/runtime/runs/worker.py) 以 sync durability 驱动编译后的 StateGraph。等待节点直接等待所属运行时 Run 的完成事件；路由事件到达后唤醒该节点。
6. 完成事件转换为有固定身份的真实 AgentMessage，经 [`DurableInboxMiddleware`／`AgentInbox`](https://github.com/Leeminjing/Focus/blob/340bf2b8607dfb536d9376d400a02b122a4542bb/backend/app/desktop/agents/inbox.py) 准备消息、写入真实 PostgreSQL checkpoint 并复核 typed source，持久化唯一 receipt 后，才 ack 运行时事件。

Windows 的 psycopg async 需要 Selector event loop，而该 loop 不支持 asyncio 的 Windows subprocess transport。本联调直接调用核心 `RuntimeClient.start`，自动使用 `focus_tool.transport.ThreadedProcess` 的真实 `subprocess.Popen` 加 stdlib thread pipe I/O fallback；协议、Go daemon、执行器均未模拟，本目录没有另一套传输实现。

## 结论与尚未覆盖的边界

这次是实际 Focus Run worker 和治理／存储组件的接入验证，决策节点使用明确的本地确定性代码，没有模型推理。它证明此小范围链路可以穿过实际权限、租约、checkpoint 和结果唤醒边界。

没有启动 Focus desktop HTTP 服务、Patrol 调度器或真实模型，没有修改或部署 Focus 主仓，也没有验证完整生产数据库迁移。唤醒覆盖的是正在等待事件的真实 Run；Run 已退出后的 Patrol 重调度不是本用例覆盖范围。运行时 journal 与 Focus checkpoint 的确认采用可重试的先 checkpoint／receipt 后 ack 顺序，不宣称跨数据库原子事务或通用 exactly-once 副作用。

容器仅属于本次运行，以独立名字创建且 finally 移除；所有工作文件、执行程序与工具产物位于临时目录。报告不保存数据库凭据。
