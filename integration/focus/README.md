# Focus 真实 Run 小范围接入验证

本目录使用本机已有的 Focus Python 环境和真实源码，执行一个小范围 Run 链路。核心运行时仍只依赖 Go／Python 标准库；本联调额外复用 Focus 已有的 LangGraph、SQLAlchemy、psycopg 等依赖，以及本机已缓存的 `postgres:17-alpine` Docker 镜像。脚本不会下载镜像、修改 Focus 源码、读写桌面正式数据库或调用模型 API。

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
