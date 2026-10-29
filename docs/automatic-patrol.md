# 持久结果驱动的自动 Patrol 唤醒

本阶段基于 `4e15d3b` 的跨激活／恢复闭环。工具结果持久进入所属 Context inbox 后，应自动推动所属 Loop 的真实 Patrol 和 RunPool。A 的结果不等待 B，测试脚本不安排后继激活。单宿主、本地确定性决策，不调用模型 API。

## 服务接口与权威

- 标准库 `PatrolWakeService(store, patrol)`：`start()`／`notify(loop_id)`／`close()`。`store.pending_wakes()` 返回 Context、所属 Loop 和待消费 generation；`patrol.prepare(loop_id, context_ids)` 提交一轮真实决策或明确因忙碌返回 False；`dispatch()` 使用既有全局 RunPool；`reconcile()` 非阻塞地结算已结束的激活和有证明的 ack。
- Focus 端 `PatrolResultService(tools, harness)`：`start(initial_contexts=...)` 恢复可信登记及当前准入策略，持有结果泵和唤醒驱动；初始激活由启动输入创建，后继激活完全由耐久结果驱动。`close()` 停止新调度、结算已启动激活并停止结果泵；工具连接／租约由长期 ToolService 单独关闭。
- 结果、实际 AgentMessage 和 Context wake generation 在同一 PostgreSQL 事务提交。通知是降低延迟的提示，不能作为唯一事实来源。启动和周期性扫描会从未消费 inbox 记录恢复，不要求通知成功。
- 待消费真相是 owner-scoped 的 `receipt IS NULL`；generation／handled 只作水位诊断。语义消费回执、动作 outbox 和 handled 更新在同事务提交。迟到结果不会被较早 checkpoint 清掉。
- 同 Loop 合并 Context，限制同时进行的 planner 数量；Loop 当前有未结束激活、已提交且排队的决策、或实际 RunPool 容量不足时不重复创建激活。最终准入仍由真实 Kernel／RunPool 决定。准备前重读 pending，防止过期快照产生空激活。
- 消费事实严格筛选到本次原生 typed inbox 快照所含的 owner／recipient source IDs。快照之后才提交的结果，或原生 inbox 单页 100 条以外的结果，留给下一激活；不将另一次查询的新事实强行放进旧 checkpoint。失败的正常 worker 仍结算真实 Run，不能因提前断言成功而永久占用容量。
- ack 仍要求精确 typed 决策 checkpoint 和语义消费证明。失去租约后暂停派发，逐项区分未提交、已终态、unknown、请求取消及请求失败，继续处理后续任务；请求取消不是退出确认。

停止唤醒驱动不能取消掉已开始的部分 RunPool drain：服务单独持有并 shield 该任务，在其结束时认领全部波次，再等待所有已启动激活结算。已完成的 drain 失败清除后可以重试。关闭时汇总每项错误，不因一项失败放弃其他激活或跳过结果泵清理；`PatrolResultService.close()` 返回 `shutdown_errors`。可选诊断回调失败被隔离。

`research_wakes`、部分 pending 索引及相关桥接表只属于本次隔离数据库，不是 Focus 生产迁移。真实组件路径沿用上一阶段的 observation／PortfolioPatrol／Kernel／ContextRunPool／coordinator／dispatcher／worker。决策 provider 和 Desktop launch port 保持研究适配；queued 的暂停→可信恢复→当前策略→恢复派发握手已完成并保留回归，不是未实现项。

## 验收场景

1. A 的 check 先完成，B 的叶子工具仍运行：自动启动 A 的后继激活、修订计划、取消扫描并启动 repair；B 不提前返回结果，随后独立完成。
2. 同一激活尚未退出时连续进入多个结果：保留 pending，退出后一次消费，不出现重复激活。
3. 一次语义消费已 checkpoint，但激活尚未退出时又有新结果：新 generation 保留并自动触发下一次消费。
4. 全局真实 RunPool 满载：决策可以等待，pending 保留，释放容量后自动继续，B 的工具无须先结束。
5. inbox／pending 已落盘、通知尚未发出时硬杀 daemon 和桥接进程：新服务不接收任何内存通知也自动续接，原工具及后续动作各执行一次。
6. 实际租约失效且首个 outbox 身份尚未提交：其后的运行叶子仍全部收到取消请求，并等待真实终态；未消费事实继续保管。
7. 恰在 native inbox 快照之后、待消费事实查询之前到达新结果：本次只消费快照内事实，新结果自动进入下一激活。
8. 两个 Context 属于同一真实 Loop：合并为一轮 Patrol decision 的两个 Context 动作，经真实 Kernel／RunPool 派发，保持分别消费和唯一激活。

复现：在现有 Focus 环境运行 `integration/focus/run_automatic.py --focus-root <固定源码> --out <原始记录>`。普通 CI 的标准库测试覆盖 Loop 合并、忙碌／容量等待、无通知恢复和逐项取消；真实 PostgreSQL／Focus 用例为显式可选联调。

## 计时规则

原始记录包含工具完成、inbox 持久化调用开始／commit 返回、通知、观察到 wake、Patrol 开始／提交、后继激活开始，以及实际 repair 函数开始。所有进程使用同机 Unix 时间戳；端到端取工具完成到 repair 函数真实开始。

数据库 commit 可先被扫描器观察到，发布协程稍后才收到提交返回。因此保留持久化调用区间及带符号的 `wake_minus_commit_return_ns`，不强行将它们排成互不重叠的阶段，也不累加嵌套区间。Loop 合并和调度可以跨通知发生；按 owner／generation 关联事实。容量／测试 gate 的等待单独说明，有限验收轨迹不代表独占性能分布或 Agent 加速比例。
