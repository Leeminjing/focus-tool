# Host Tool 执行协议（PR 1）

Go 继续拥有统一队列、并发、显式资源、任务身份、WAL、完成事实和消费回执。新的 `kind=host` 将一次已登记调用派发到持有真实 ToolRuntime 的 Python 宿主；框架对象、权限身份、lease 和 ledger 不跨 JSONL，也不在 Go 重写工具。原始架构图不变，host 是 Python 执行路径的一种模式。

## 协议与状态

可信注册 `ToolDefinition("host", handler=async_handler)`，handler 接收 `(task, cancellation_event)` 并返回 Result 字典。普通模型参数不能声明 handler、权限或资源；注册的 handler 按原 job 固定，在 ack／关闭／历史终态重试后回收。宿主 handler 必须异步，不能阻塞协议 reader。

| 消息 | 方向与合同 |
|---|---|
| `host_execute` | Go→Python；携带随机 execution_id 和完整 Task（owner／job／revision／host.tool／args），只派发一次 |
| `host_cancel` | Go→Python；同一 execution_id，传递取消信号；不等于清理确认 |
| `host_complete` | Python→Go；execution_id＋owner＋job 必须全部匹配；一个 execution 只接收一次回执 |
| completion event | 原有 Go→所属 Run；终态和事实已持久化，按现有 event_page／next_fact 查询，caller checkpoint 后才 ack |

宿主完成回执带 `host.process_cleanup`（`confirmed`／`not_started`／`unknown`）、可选 canonical `tool_message` 与结构化 `facts`。Shell 的退出码、模式、后端状态和清理告警在 facts 内保留。`host_complete` RPC 回应仅表示回执交给执行等待者；Go 的耐久终态以完成事件／status 为准，不把这个传输回应当 fsync 成功。

取消／超时后仍占执行位，等待宿主回执；默认额外等待 5 秒（CLI `-host-cleanup-timeout`），缺少回执转为 `unknown`。断连、异常、非法状态、无可靠清理证据和过大回执也保守隔离。即使函数返回，清理为 unknown 也不能变成普通 failed 来释放资源。unknown 保留任务额度和冲突资源，ack、关闭 Run、归档、压缩与重启均不清除；终态重提原身份不执行。PR 1 未提供清理未知资源的自动 reconcile／重放接口。

宿主回执在 Python 端先验证字段类型及 1 MiB 编码字节预算，Go 再验证一次；host Task 也限制为 1 MiB，保证携带任务和结果的分页仍有界。所有请求还有 4 MiB 发送前预算，避免一个坏返回值打断其他 Context 的连接。Go 关闭时继续读取 host_complete，直到执行清理／隔离收口；Python 关闭时信号通知并等待自己拥有的宿主调用，超时抛出清理未确认。

Go 的执行前 gate 检查原始可信登记；host 的应用准入 hook 在 Python 实际执行入口运行一次。宿主负责再走原生权限／lease 与 ledger，不把“恢复登记清单”当权限。queued 恢复仍采用暂停派发→原 ID 注册／恢复可信原生请求与当前策略→完整 inventory resume；running 恢复为 unknown，不能自动再次调用 handler。

## 最新 Focus 的原生 Shell 纵切

`integration/focus/host_shell.py` 通过可信 resolver 查找原始 `ToolCallRequest`，验证 owner、job、参数和 opaque 全局 Shell 资源。`NativeShellHost.definition()` 固定独占 `native-shell:global-host-effects`；缺少此声明拒绝执行。此单宿主试验只登记原生 Shell，保证不同工作区的 FULL Shell 也不会并行写坏。它不实现文件／目录层级资源，也不协调另一个 daemon；混合任意 Python/process 工具仍需统一声明冲突，不能据此宣称 PR 3 完成。

执行依次调用最新 Focus 的 `AccessPolicyMiddleware`、`ToolExecutionMiddleware` 和原工具 `ainvoke`。ToolRuntime 由宿主注入，canonical ToolMessage 由原工具格式化并由原 ledger 保存；不把提交成功伪装成普通 bash 的结果。ledger observer 只转发原操作并观察同一个服务端清理 receipt，不重复 claim／complete，也不把输出文本或取消请求当退出证据。

取消使用调用级信号，等待原生 Shell 后端退出。完整恢复证据继续留在 Focus 的 ledger／receipt 文件中，Go facts 只保留 run／attempt／call／receipt 身份引用，不复制机器启动标识。执行状态不明时保留 Go 隔离；测试失败且可能已执行工具时保留隔离数据库和工作区供核查，不自动删掉未知执行的 lease／证据。

本轮固定 Focus `0349f562b488eb06647a1204634e067903e6132b`，复用其声明的 context-index `af8e65658d9c5351ae5ca0d68ec2ac094f5f240c` 及已有 Windows Node/Koffi 依赖。旧 `340bf2b8` Patrol 联调和桥接代码保留，不作为新版原生工具成功依据。

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/test_host_shell.py `
  --focus-root '<0349f562-checkout>' `
  --context-index-root '<af8e656-checkout>' `
  --out results/local/native-host-shell.json
```

使用已有环境；若 context-index 已按 Focus 的锁定依赖安装，可省略其路径。Windows 还需要该检出的 `desktop/node_modules/koffi` 可用。本地检出可以复用已有依赖目录，不需要为 focus-tool 核心增加依赖。测试使用独立缓存 PostgreSQL 容器，不访问桌面数据库，不调用模型服务。

## 已实现与后续

- PR 1：host 派发／取消／回执、unknown 隔离、身份恢复、本地协议测试及真实原生 Shell 纵切。
- PR 2：四种 Shell 与 Windows 三种模式／POSIX 的完整矩阵、控制通道故障和真实崩溃恢复继续验收；本轮只验证 Windows 的显式 FULL 模式，不能外推受限 ACL 矩阵。
- PR 3：EffectContract 治理目标、路径／目录层级冲突、符号链接和工作区外副作用。
- PR 4：普通工具返回原 ToolMessage 的 Agent 接入，以及显式 start_tool_job 的持久早反馈与增量输出。

`make_lead_agent(additional_middlewares=...)` 在原生 policy／ledger 内部；直接在其内再排队并重包 ledger 会改变时机或重复记账。本 PR 显式组合一次原生链路，没有部署到 production Agent／Patrol，也没有声称原激活退出后可继续使用已释放的原生 ToolRuntime／lease。

TaskProgress／Accounting 内部探针、开关、专用测试和相关报告／数据已从当前树移除，可从合并历史恢复。保留旧 inbox、自动唤醒、typed checkpoint、动作 outbox、恢复用例及原 Patrol barrier 性能基准；本轮不继续性能研究。
