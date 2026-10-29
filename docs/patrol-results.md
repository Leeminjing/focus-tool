# Patrol 跨激活接入验收

本页保留 `4e15d3b` 阶段的手动场景证据；其后新增的持久结果驱动自动唤醒、合并和无通知恢复见 [自动唤醒验证](automatic-patrol-results.md)。不要把本页单次 5503.69 ms 与后续轨迹直接解释成优化比例。

2026-10-29，在只读 Focus 检出 [`340bf2b8607dfb536d9376d400a02b122a4542bb`](https://github.com/Leeminjing/Focus/commit/340bf2b8607dfb536d9376d400a02b122a4542bb)、本机已有 Focus Python 环境及独立缓存 PostgreSQL 容器上运行。没有模型 API、生产数据库或 Focus 源码修改。核心仍使用 Go／Python 标准库；额外依赖仅属于可选 Focus 联调。

## 实际通过的闭环

[正式原始记录](../results/patrol-integration-2026-10-29.json) 包含七例，全部通过。记录保留完整执行事实、owner／job／revision、原生 inbox checkpoint、语义消费 checkpoint、稳定动作、ack 标记、进程 PID、启动／结束和激活退出时间，以及实际源码字节 SHA-256。base commit 是构建时的 Git HEAD，未提交源码差异由指纹标识；不把 base commit 冒充本次源码。

| 场景 | 验证结果 |
|---|---|
| two_context_redirect | A、B 共用 runtime 和实际 ContextRunPool；首次激活退出前工具已启动，退出后才结束。A 的检查触发后继激活、revision 2、真实叶子扫描取消和 repair；B 的独立产物正确。A 共 3 次、B 共 2 次工作激活，另有各自 bootstrap |
| before_inbox | 工具终态已 fsync、Focus inbox 尚未写入时杀死 daemon／桥接进程；重启后重新投递，recovery／repair 各执行一次 |
| after_receipt | 决策、消费回执及动作已完成，runtime 尚未 ack；重启补 ack，不重复消费原决策，也不重复执行 repair |
| receipt_pending | 消费回执与 outbox 已提交、Focus 激活已退出，repair 尚未提交；硬杀恢复后执行原身份动作一次 |
| revision_reconcile | 耐久 outbox 指向 revision 2，故意构造 runtime 已为 2、SQL owner 仍为 1；硬杀后从 outbox 校准打开版本，完成原动作一次 |
| queued_allowed | 派发暂停时保存 queued 并硬杀；缺失可信登记清单时拒绝恢复，原身份重建后允许执行 |
| queued_revoked | 同样恢复 queued，但当前真实 Loop grant 已撤销 write；实际权限 hook 拒绝，没有工具 PID、尝试文件或目标产物 |

所有正常／恢复写入工具的尝试计数均为 1；撤权用例为 0。正式两 Context 场景还验证了两次“只有 inbox、没有决策消费回执”的 ack 被拒绝。旧 revision 的扫描取消事实进入 typed checkpoint 并分类为 obsolete，没有由客户端隐式 ack 掉。`cancelled` 来自执行器终态和直接子进程 Wait；没有声称进程树治理。

## 真组件与研究适配的界面

真实路径是 `LoopObservationService → PortfolioPatrol → LoopKernel → ContextRunPool → LoopCoordinator → LoopWaveDispatcher → RunOwnershipPolicy → LoopRunWorkspaceBinder → LoopRunExecutionBoundary → execute_prepared_run／run_agent`。任务记忆沉淀也使用实际 `TaskProgressRuntime`，其确定性 provider 保留 unknown 来源，不伪造任务完成结论。每个 Context 有独立 Loop／激活工作区，两个 Loop 共享同一全局 Run pool。

Desktop launch port 与决策图由本目录本地代码提供；没有启动 Desktop HTTP、完整 supervisor 或真实模型。直接的 ORM Run 身份装配使用原生 admission policy，Run 结算调用实际 coordinator；这不是 Focus 主仓生产服务的部署验收。

长期工具服务另持独立 job workspace、真实 `WorkspaceLeaseManager` 租约及研究专用 `kind=tool_service` 锚点。正式场景记录了 112 次续租，执行前复核使用该服务的租约，不依赖已释放的激活 token。服务失去续租权会暂停新派发并请求取消；终态未知的工作区拒绝重新挂接，正常关闭只释放已确认终态的工作区租约。

持久顺序为：runtime 终态 → 原 owner 的幂等 inbox → typed inbox checkpoint／原生 delivery receipt → typed 决策 checkpoint → 同事务语义回执＋action outbox → 原身份动作执行／恢复 → runtime ack。原生 delivery receipt 不是语义消费证明。owner 版本先于 runtime 切换保存，恢复打开版本还取 owner／耐久 outbox 的最大意图版本，解决 SQL／WAL 之间的崩溃窗口。

所有重启点由父进程在 marker 后杀死真实 Go daemon 和 Python 桥接子进程，没有 graceful close。中断点的工具均已完成或未派发；运行中 crash 仍按 unknown 隔离，不自动重放。这验证了列出的恢复窗口，不宣称任意副作用的通用 exactly-once 或跨数据库原子事务。

## 时间与性能的限度

原始记录含完成、持久投递、动作开始及激活退出的同机 Unix 时间戳。本次 A 检查完成到 repair 动作开始为 **5503.69 ms**，包含实际观察冻结、任务记忆、Kernel、派发和 checkpoint 链路。它是单个确定性场景的观测，不是 p95、优化收益或 Agent 加速比例；本轮还运行了其他回归，不作为独占性能基准。

短工具池／压缩的既有研究结论不变；本次没有重新测其吞吐或压缩策略。后续性能工作应在此真实跨激活链路上独占采集分阶段轨迹和分布，再决定优化观察／数据库／调度还是桥接开销。已披露的索引／I/O 竞争仍是独立待办。

## 回归与复现

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/run_patrol.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --out results/local/patrol-integration.json

python scripts/verify.py
go run ./cmd/lab
```

完整回归可额外指定 `scripts/verify.py --focus-root <checkout> --focus-python <existing-python> --focus-patrol`。Go test／vet、29 项 Python 测试和 Demo 的最终输出见 [本地检查记录](../results/patrol-verification-2026-10-29.json)；原有三个真实 Focus 权限／租约用例见 [原生链路回归](../results/patrol-native-regression-2026-10-29.json)。M1 另外重跑并保留 [原始记录](../results/patrol-m1-regression-2026-10-29.json)，不将其数字当作新增 runtime 或 Patrol 性能。

共享 runtime 的等输入 barrier／event 和持续负载回归见 [完整链路记录](../results/patrol-full-runtime-2026-10-29.json)，负对照见 [扫描仍必需的记录](../results/patrol-full-runtime-negative-2026-10-29.json)。持续负载的 1024 条样本中，近期缓存从 144 增至 1040 项，GC 后 Go 堆从约 1.081 增至 2.567 MiB；WAL 尚未达到 8 MiB，没有压缩。关闭保管 Run 后未 ack 事实归零。有限负载不证明长期 RSS 恒定，已有有界历史策略／压缩验收仍以此前万次和多段实验为准。

每组各三次，总耗时中位数：纠偏场景 barrier **3049.65 ms**、event **314.14 ms**；扫描仍必需的负对照 barrier **2885.79 ms**、event **2895.06 ms**。此处使用同样输入、并发上限、fsync 和最终产物，计入取消回收。它们是已有工具负载的行为回归，不是 Patrol 完整链路对照，也不用于推断本次提交相对前一提交的性能改善。

[2026-10-28 初版记录](../results/patrol-integration-2026-10-28.json) 是五例开发验证，保留原样；它早于工具跨退出时间戳断言、原生 typed source 补强及新增两个恢复边界，不用于代替正式七例证据。开发中还修复了 Windows 子进程输出编码导致验证脚本无法打印 Focus 失败的边界，脚本现在统一使用 UTF-8 子进程环境。

标准／需求两轴审查均发现同一个 revision 交接窗口，已修复并以 `revision_reconcile` 复验；需求审查补充的动作 pending 恢复由 `receipt_pending` 覆盖，定向复审无剩余具体问题。Linux race 由仓库 CI 执行，本机 Windows 未配置 cgo race 环境。

## 仍未完成的生产边界

尚未提供生产 Desktop 服务身份与授权迁移、完整 supervisor 启停／高可用、多宿主接管、运行中 unknown 自动核查，或桥接 SQL 历史的生产分页／保留策略。研究桥接表按场景规模读取，不能用核心 runtime 的历史有界承诺替代桥接服务的长期内存验收。正式合入 Focus 前仍需实现这些接入面；本仓库的原图本地闭环与可选真实 Patrol 原型已完成本阶段场景。
