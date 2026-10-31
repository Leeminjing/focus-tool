# Host Executor 与原生 Shell 纵切验收

2026-10-31，本轮实现 PR 1：Go 调度、Python 原生 ToolRuntime 宿主执行。没有修改 Focus 主仓，没有增加核心依赖、模型调用或托管服务，也没有继续 TaskProgress 性能研究。[协议与复现命令](host-executor.md)。

## 最新 Focus 的实测范围

固定 Focus `0349f562b488eb06647a1204634e067903e6132b`，复用其锁定的 context-index `af8e65658d9c5351ae5ca0d68ec2ac094f5f240c`、既有 Python 环境及 Windows Node/Koffi。使用缓存 PostgreSQL 的独立容器和临时工作区，原生 SQL ledger、租约、权限中间件、Shell 后端及 PostgreSQL typed checkpoint 均实际执行，不访问桌面数据库。

| 场景 | 执行事实与断言 |
|---|---|
| CMD 正常完成 | exit 0、canonical ToolMessage、原生 claim/complete 各一次；保存并读回 typed checkpoint 后 ack |
| PowerShell 非零退出 | exit 7 保持 failed；原生错误、模式、输出和清理事实保留 |
| PowerShell 父子进程取消 | 取消等待原生 Windows Job 清理；耐久 runner receipt 的 job_drained 为 true，另用 Windows API 检查两个真实 PID 已退出 |
| 排队期间失租 | 另一个 Context 排队后释放其 fencing lease；真正派发时原生权限检查拒绝，ledger claim 为零、process_cleanup=not_started |

四例全部通过。[原始完整事件、ToolMessage、checkpoint、清理证明和 37 个源码指纹](../results/host-shell-0349-2026-10-31.json)。前三例均保留原生 canonical ToolMessage；失租用例没有假造 ToolMessage 或 checkpoint，未 ack 的执行事实仍由 runtime 保管。已终结的原身份重提不会再次调用宿主。

这些测试明确选择 `danger-full-access`。记录中的 `backend_applied=false`／`enforcement=none` 是该模式的真实含义，不能称为受限 ACL 验证；进程树清理证明来自 Focus 的原生后端，只适用于这里实际测试的 Shell 路径，通用 Python/process 执行器仍只有叶进程合同。

## 协议边界回归

新增 Go 测试覆盖 owner／execution nonce 不匹配、重复回执、取消请求不等于完成、清理超时，以及 unknown 在 ack、关闭、归档、压缩、重启后继续占用冲突资源。

新增五项 Python 测试运行真正的 Go daemon，覆盖跨进程执行与归属、关闭时继续接收回执、异步 handler 登记、异常／超大结果隔离、共享连接中其他 Context 继续执行、杀死 daemon 后不重放，以及暂停恢复 queued 身份后按当前权限拒绝执行。Task 和 receipt 分别限制为 1 MiB，避免完成事件超出分页预算；传输请求另有 4 MiB 发送前限制。

完整回归中 `go test ./...`、`go vet ./...`、Python 43 项测试和 Demo 均通过，见 [本地验证](../results/host-executor-verification-2026-10-31.json)。旧真实 Focus [执行三例](../results/host-executor-legacy-live-2026-10-31.json)、[Patrol 跨激活七例](../results/host-executor-legacy-patrol-2026-10-31.json)、[自动唤醒八例](../results/host-executor-legacy-automatic-2026-10-31.json)及[控制测试六项](../results/host-executor-legacy-controls-2026-10-31.json)全部通过，继续固定 `340bf2b8`；这些记录仅证明旧桥接没有因本轮改动退化，不作为最新版生产 Agent／Patrol 接入的依据。Windows 本地未运行 race 检测，Linux race 由 PR CI 执行。

另运行了 [M1 的九次本地对照](../results/host-executor-m1-regression-2026-10-31.json)，只作为原 baseline 的行为回归。本轮没有 Host Executor 性能对照，也不从这些数字推断 Agent 提速。

## 开发中发现的问题与剩余范围

完整回归首次发现：非 host 适配器也占用了 host 回调，阻止旧联调在同一连接上重建登记。修复为仅 host 注册安装回调后重跑。开发期还修正了新版 Focus 缺少锁定依赖的环境准备和测试 fixture 的 lease fencing 参数；这些失败不是正式通过样本。

TaskProgress／Accounting 专用探针、开关、测试与报告数据已从当前树移除；历史可从 Git 找回。原 inbox、自动唤醒、typed checkpoint、动作 outbox、恢复验证和 Patrol barrier 基准保留。

后续 PR 2 仍需完整 Shell／访问模式／平台矩阵和原生控制通道崩溃恢复；PR 3 处理真实文件目标与层级资源；PR 4 接普通 Agent ToolMessage 返回及显式后台任务早反馈。当前单宿主使用保守全局 Shell 锁，尚未与任意混合执行器或其他 daemon 统一治理资源。不能把本轮显式组合链路直接放入已有 policy／ledger 内部重复 claim，也不能在原 Focus 激活退出后继续借用已释放的 ToolRuntime／租约。
