# 自动 Patrol 唤醒验证

本阶段承接 `4e15d3b`，把结果接收、待消费状态、自动调度与结算合成可调用的单宿主服务。Focus 仍固定在 `340bf2b8607dfb536d9376d400a02b122a4542bb`，只读使用现有环境；真实测试在独立缓存 PostgreSQL 容器中执行，没有模型 API 或正式数据库连接。

## 自动闭环

新的应用入口是 `integration/focus/run_automatic.py`。脚本启动 `PatrolResultService`、控制本地叶子工具的 fixture gate 并断言，不调用后继 `harness.schedule()`；关键 A/B 用例也没有等齐结果的屏障。原 `run_patrol.py` 保留为七个恢复边界回归，不用其手动调度数据宣称自动唤醒。

| 场景 | 验证点 |
|---|---|
| A_repairs_before_B_finishes | A 的初始激活退出后 check 返回，自动调用真实 Patrol／Kernel／RunPool 启动后继、修订计划并运行 repair；此时 B 的工具仍为 running、没有 B 的结果或产物 |
| burst_coalesced_while_activation_busy | 三个结果在原激活未退出时连续进入 inbox；保留一份 Context pending，退出后一个后继激活消费三条事实，没有重复激活 |
| new_result_during_checkpointed_consumption | 已写语义消费 checkpoint 但激活尚未退出时，另一个结果到达；新的 pending 保留，自动进入下一次激活，两个事实各消费一次 |
| arrival_between_inbox_snapshot_and_fact_query | 恰在原生 inbox 快照之后、事实查询之前放行第二工具；只消费快照内事实，后到事实自动进入下一激活 |
| two_contexts_one_real_loop_coalesced | 同一真实 Loop 的两个 Context 合并为一个 Patrol decision 的两个动作，经过真实 Kernel 权限校验、read leases 和 RunPool 分别派发；初始两个及后继两个激活各唯一 |
| capacity_wait | 实际全局 RunPool 上限为 1，B 的激活占用槽时 A 的已提交决策与 pending 保留；释放槽后自动继续，A 开始 repair 时 B 的工具仍在运行 |
| lease_loss_skips_unsubmitted_and_cancels_all_live_jobs | 实际释放服务 lease，续租失败后遇到首个尚未提交的 outbox 身份仍继续处理两个运行叶子；均收到请求并实际进入 cancelled，结果继续保管 |
| inbox_before_notify | inbox／pending 已提交而通知尚未发送时，父进程硬杀真实 Go daemon 和桥接 Python 进程；恢复服务即使禁用全部通知，也能自动发现并消费，原工具和 repair 各执行一次 |

结果与完整轨迹见 [自动唤醒原始记录](../results/automatic-patrol-2026-10-29.json)。硬杀用例在注入前暂时停止 wake consumer，防止周期扫描在父进程杀进程前已经修复缺口；receiver 仍真实执行事务并停在通知之前。重启时没有恢复任何内存通知，startup／周期扫描承担全部续接。数据库容器保持运行；这是服务／runtime 重启测试，不是 PostgreSQL 或主机断电实验。

## 持久与调度责任

Context AgentMessage、事件事实及 wake generation 同事务提交。`receipt IS NULL` 是待消费真相，generation／handled 是诊断水位；语义回执、action outbox 和 handled 更新同事务完成。新到结果不会被旧 checkpoint 清掉，也不凭通知删除 pending。

`PatrolWakeService` 用标准库实现合并、周期恢复及有界 per-Loop planner；真实 `FocusPatrolPort` 复核活动 Run、已有权威决策与最新 pending，再使用原生 observation／PortfolioPatrol／Kernel／ContextRunPool。容量不足时保留已有 decision，避免每轮创建新激活。原生 inbox 每页至多 100 条，消费只选当前 typed source IDs 所代表的 owner／recipient 事实；未分页到或快照之后才到的结果留待后继处理。

服务单独持有部分 drain 任务，停止 wake driver 不会丢掉已开始的派发。完成的瞬时 drain 错误清除后可重试；关闭会等所有已启动激活并汇总错误，再停止结果泵。可选诊断回调不能影响任务跟踪／租约释放。正常 worker 失败也会结算 DesktopRun 的终态与 `settled_at`，不因提前断言 success 永久占用容量。真实失败后的 Loop 授权收敛仍由原生 coordinator 决定，不承诺自动修复任意生产故障。

失权清理先尝试暂停派发，再逐项查询身份，区分未提交、已终态、unknown、请求取消与失败；一项错误不终止后续项。`cancel_requested` 只表示请求，unknown 继续隔离。对已失效的 lease token，不会把它当作释放新 owner 租约的权限。

## 计时解释

每个 A/B 场景保留工具完成、inbox 持久化调用开始／commit 返回、通知、wake 被观察到、Patrol 开始／提交、后继激活开始和实际 repair 函数开始。端到端使用同机 Unix 时间戳取工具完成到 repair 函数真实开始，包含 Python 进程启动及完整 Focus 链路。

正式普通 A/B 场景记录的端到端为 **3977.47 ms**；A 开始 repair 时，B 仍为 running，且没有 B 的结果或产物。这是一个确定性场景的观测。其时间线以 A 工具完成为零点：

| 观测点 | 相对时间（ms） |
|---|---:|
| inbox 持久化调用开始 | 1.58 |
| inbox commit 返回／发出通知 | 20.81 |
| wake 被扫描到 | 58.20 |
| Patrol 准备开始 | 84.80 |
| Patrol 权威决策提交返回 | 2829.52 |
| 后继激活启动 | 3379.18 |
| repair 函数实际开始 | 3977.47 |

Patrol 准备到提交的跨度包含任务记忆沉淀、观察冻结和 Kernel 等工作，尚未拆开这些内部成本。全局容量用例为 **4910.80 ms**，包含显式保留 B 激活槽的 fixture 等待；它不参与普通响应分布，也不能据此断言更换容量配置的收益。

commit 可见性可能早于发布协程收到返回，因此 `wake_minus_commit_return_ns` 保留符号；通知也可能晚于周期扫描。不要把这些嵌套或重叠区间强行线性排序、丢弃负值或相加。容量等待用例包含显式 fixture 槽等待，不能混进普通响应分布。本轮是有限功能验收轨迹，没有 p95/p99、独占稳定吞吐或 Agent 加速比例结论；上阶段 5503.69 ms 也不构成本轮的对照基线。

## 验证记录与复现

- [顺序全量验证](../results/automatic-verification-2026-10-29.json)：Go test／vet、33 项标准库 Python 测试、Demo、原有真实 Focus 链路、恢复回归、控制测试及自动唤醒场景。
- [原有 Focus 三例](../results/automatic-focus-native-2026-10-29.json) 与 [原有七例跨激活／恢复](../results/automatic-patrol-regression-2026-10-29.json)：保留真实权限、lease、checkpoint／ack、pending action、revision 校准与 queued 允许／撤权握手。
- [五项控制／错误测试](../results/automatic-wake-controls-2026-10-29.json)：真实适配类配 stub 依赖，覆盖部分 drain 取消、瞬时错误重试、关闭聚合、失败 worker 结算、诊断隔离及页内归属。它们不连接数据库，不冒充真实 PostgreSQL 联调。

```powershell
python scripts/verify.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --focus-python 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  --focus-patrol --focus-automatic
```

正式报告保存实际构建 base commit 与源码字节指纹；指纹标识未提交源码差异，不把 base commit 冒充本轮源码。标准／需求审查发现的 typed 快照竞态、部分 drain 所有权、失败清理与诊断回调问题已修复并增加回归。M1 也另行重跑，没有更改其 baseline 或将其数字当作新增服务收益；本轮未重测短工具池或压缩性能。

M1 原始轨迹见 [回归记录](../results/automatic-m1-regression-2026-10-29.json)。共享 runtime 的等输入 barrier／event 及持续负载见 [回归](../results/automatic-full-runtime-2026-10-29.json)，扫描仍必需的 [负对照](../results/automatic-full-runtime-negative-2026-10-29.json) 单独保留。这两组验证原有工具执行行为，不代替真实自动 Patrol 链路对照；近期 WAL 缓存及 GC 堆仍按段增长，有限回归不证明长期 RSS 恒定。

每组各三次的总耗时中位数：可纠偏时 barrier **3055.72 ms**、event **307.54 ms**；扫描必需时 barrier **2930.36 ms**、event **2961.72 ms**。持续负载有 1024 条测量样本，近期缓存从 144 增至 1040 项，GC 后 Go 堆约 **1.101 → 2.646 MiB**；窗口内未达默认 8 MiB 段阈值，压缩次数为 0。归档后未 ack 事实为 0、近期缓存仍为 1040。这些是原有执行机制的回归，不作为本轮自动唤醒速度的对照。

原图拓扑继续保留。此阶段完成的是单宿主、确定性研究服务接入；Focus Desktop HTTP／完整生产 supervisor、服务身份与权限迁移、多宿主接管、桥接历史的生产分页／保留策略、运行中 unknown 的自动核查仍待接入。queued 恢复握手已完成，不属于这些未完成项。
