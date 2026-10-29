# 真实 Patrol 性能对照：2026-10-29

40 个正式 trial、4 个未计分 warmup 全部通过。提前纠偏可以缩短本地修复响应；必要扫描没有观察到稳定收益，并多出一次 A 激活。本轮只增加测量，下一项研究集中到 TaskProgress drain 路径。

## 条件与原始证据

固定 Focus `340bf2b8607dfb536d9376d400a02b122a4542bb`，focus-tool 基于 `3745f557`，44/44 个被测源码文件的字节指纹在采集前后相同。Windows 10.0.26200、Go 1.26.5、已有 Focus Python 3.11.15；独立缓存 PostgreSQL 17 容器，逐 trial 新数据库／journal／工作区。未运行其他本仓测试或基准，也未调用模型 API。操作系统和 Docker 的延迟不是受控常量。

工具并发 4、激活并发 2、planner 默认 4；每次 WAL 修改 fsync，默认后台压缩配置未变。A 对固定 8 MiB corpus 做 1461 次 SHA-256，B 对自己的 corpus 做 365 次；次数由测量前一次校准冻结。所有模式／轮次保持同样字节输入与最终产物。有限工具不使用 sleep、gate 或无限循环，B 自然结束。barrier 仅等待 A 自己的 check＋scan，不等待 B。详见 [测量合同](patrol-performance-method.md)。

- [配置与源码指纹](../results/patrol-performance-2026-10-29/config.json)、[原始采集汇总](../results/patrol-performance-2026-10-29/summary.json)。目录内保留每个 trial 的 JSON 与 gzip JSONL，无慢样本剔除或替换。
- [采集后派生的成对差值与交接时间](../results/patrol-performance-2026-10-29/derived-handoffs.json)：新增派生文件，不改写上述原始文件。
- [开发预检失败记录](../results/patrol-performance-preflight-2026-10-29/README.md) 不属于正式 cohort；旧 Round 交接问题的 defer 检查与控制回归随本 PR 提交。

## 同场景比较

下表每格为 10 次中位数，单位 ms。任务结算包括最终产物读回、完整预期终态清单、取消实际退出确认、typed checkpoint／语义回执／ack 和激活结算；完整关闭还包括服务、runtime 与数据库关闭。setup 单列，不计入任务窗口。

| 场景／模式 | 任务结算 | 含完整关闭 | check 完成→有效后续 | 首次工具入口→结算 |
|---|---:|---:|---:|---:|
| 纠偏 barrier | 17630.92 | 17695.85 | 11667.01 | 15391.06 |
| 纠偏 event | 15805.82 | 15898.40 | 6229.38 | 12569.22 |
| 必要扫描 barrier | 18444.14 | 18531.17 | 13884.79 | 15205.52 |
| 必要扫描 event | 23828.46 | 23927.55 | 17266.94 | 18639.00 |

纠偏的有效后续取 repair 实际函数入口；必要扫描取 scan 语义回执事务成功返回，不能跨场景混成一个指标。纠偏 event 的 scan 10/10 为 `cancelled`，barrier 10/10 为 `completed`；必要扫描两臂均 10/10 `completed`。没有把取消请求当作退出，也没有自动重放失败任务。

宿主耗时明显波动，不能把两列中位数之差当作成对差的中位数。按同一 repetition 计算 event−barrier 后再取中位数，纠偏的响应差为 **−8155.21 ms**、含关闭总耗时差 **−8220.55 ms**；必要扫描分别为 **+270.11 ms**、**+620.09 ms**。必要扫描第 4 对含关闭差达到 +9355.08 ms，原样保留；纠偏第 8 对总耗时也回退 +921.88 ms。每对原值可在派生文件复算。这些描述统计没有证明固定比例的生产加速或回退。

纠偏 A 两臂均 3 次激活；必要扫描 A 为 barrier 2 次、event 3 次，均为 10/10；B 始终 2 次。额外一次激活是负对照的实际成本，尚不能将所有耗时差因果归给它。setup 的四臂中位数为 3112.56／3140.35／3462.60／3956.75 ms，关闭阶段分别为 69.65／84.96／90.02／75.14 ms；它们不是应与表内中位数直接相加的常量。4 个完整 warmup 与各 trial 的 setup 均留在原始数据中。

## 首次后继 Patrol 的耗时定位

窗口从 A 的 check 完成到该 Loop 首次后继决策提交。以下为每 trial 各阶段完整调用累计时间，再取 10 次中位数；父 schedule_total 单列，不能与子阶段相加。与窗口相交的调用全部计入，另存 wall overlap、边界穿越及 Unix 时钟异常计数；正式样本未出现边界穿越或时钟倒退。

| 阶段 | 纠偏 barrier | 纠偏 event | 必要扫描 barrier | 必要扫描 event | 每 trial 调用次数中位数 |
|---|---:|---:|---:|---:|---:|
| TaskProgress drain | 663.19 | **1180.19** | 827.95 | 1003.68 | 4 |
| Round 维护 | 629.01 | 667.99 | 612.82 | 769.64 | 2 |
| Round 状态读取 | 15.30 | 22.55 | 15.21 | 18.84 | 2 |
| Round 身份读取 | 5.56 | 10.01 | 6.26 | 7.86 | 1 |
| Observation 冻结 | 572.41 | 925.37 | 678.12 | **1017.94** | 1 |
| 调度权领取 | 47.05 | 58.63 | 43.12 | 60.64 | 1 |
| Patrol 决策 | 53.65 | 82.53 | 44.76 | 84.15 | 1 |
| Kernel 提交 | 322.51 | 640.33 | 406.56 | 597.40 | 1 |
| 调度权释放 | 9.35 | 12.74 | 10.41 | 13.56 | 1 |
| schedule_total（父） | 2303.51 | 3516.07 | 2676.90 | 3852.04 | 1 |

四臂的中位数均为两次 Round 循环、四次 drain、处理两个 work；不是单次 drain 耗时。其中纠偏 event 的 [第 6 轮](../results/patrol-performance-2026-10-29/026-sample-replan-event.json) 有一次 defer 后重试，累计七次循环、九次 drain、两个 work，父 schedule 调用两次，已全部纳入窗口；其余 39 个正式 trial 为 2／4／2。纠偏 event 中 TaskProgress 累计最大，必要扫描 event 中 Observation 略大，不能把所有场景概括为同一个根因。TaskProgress 仍使用保持 evidence 的本地确定性 provider，其时间不是模型服务延迟。

纠偏链路的交接中位数如下，单位 ms。各列分开取中位数，所以不能相加恢复端到端中位数。

| 交接 | barrier | event |
|---|---:|---:|
| check 完成→inbox 持久化返回 | 36.82 | 46.31 |
| inbox→Patrol 准备开始 | 8369.22 | 937.89 |
| Patrol 准备→决策提交 | 2303.61 | 3516.08 |
| 决策提交→后继 activation 启动 | 509.13 | 894.77 |
| activation 启动→repair 函数入口 | 663.99 | 984.42 |

barrier 的 inbox→准备含它自己原始批的等待；event 仍需等待原激活结算和调度机会。数据支持“提前放行减少等待”，没有支持“Patrol 内部各步骤也因此变快”。派生规则是：按 sample 的 A owner／loop，匹配 check sequence 的 publication，随后取首个 patrol_started、patrol_committed、activation_started 和 repair attempt Unix 时间戳，逐段相减；派生文件保存每个 sample 的值及 trace SHA-256。成对差使用同 scenario、同 repetition 的原始 metrics 逐项相减。

下一项只研究 **TaskProgress drain 的事务／输入装配／预算与校验／发布成本及往返合并机会**，先进一步分段，不跳过持久事实或冻结语义。当前阶段没有 SQL 调用级证据，不能认定数据库往返、token 估算或序列化中的某一项是根因；Observation 保留为后续候选。本 PR 不修改 Focus 主仓、调度算法或持久化策略。

## 验证与限制

`python integration/focus/benchmark_patrol.py --analyze results/patrol-performance-2026-10-29` 可用纯标准库独立重算 44 个 trial 的 markers、响应终点、阶段耗时／调用、循环／work 数和汇总，并验证输入／最终产物一致。本次另外将重算对象与原始 `summary.json` 比较，结果相同，同时复核全部 44 个源码指纹。Windows 与 Linux 的核心 CI 不运行可选 Focus／PostgreSQL 性能基准。

本地 `go test ./...`、`go vet ./...`、38 项 Python 测试、完整 Demo、真实 Focus 三例、跨激活／恢复七例、自动唤醒八例和六项适配控制测试全部通过。保留的 `go run ./cmd/lab` 九个 trial 回归也通过，数字不混入本轮性能结果。规范与规格两条独立审查均无剩余阻塞项，CI 状态见本 PR。

执行记录保存在 [全量验证](../results/patrol-performance-verification-2026-10-29.json)、[真实 Focus 三例](../results/patrol-performance-live-2026-10-29.json)、[跨激活／恢复七例](../results/patrol-performance-regression-2026-10-29.json)、[自动唤醒八例](../results/patrol-performance-automatic-2026-10-29.json)、[适配控制回归](../results/patrol-performance-controls-2026-10-29.json)、[M1 回归](../results/patrol-performance-m1-2026-10-29.json) 和 [纯标准库重算核对](../results/patrol-performance-2026-10-29/reanalysis-check.json)。

样本仅针对本机有限 SHA 负载、固定 Focus 版本与研究 launch/provider 端口。未测试模型推理、Desktop HTTP／完整 supervisor、生产数据库迁移或进程树清理；结果不能外推为完整 Focus Agent 的加速比例，也不能与过去的单次功能验收计时计算提升率。
