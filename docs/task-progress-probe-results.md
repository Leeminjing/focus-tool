# TaskProgress 成本定位：2026-10-31

可选原生探针已完成：40 个正式 trial、4 个 warmup，以及关闭探针的 12 个正式控制、4 个控制 warmup，全部完成业务断言与原始轨迹重算。没有修改 Focus 的业务实现。这一轮将“有任务”的成本定位到了领取冻结、accounting 读模型及事件登记、发布等路径，预算估算和本地 provider 成本很小。

## 条件和证据

Focus 固定 `340bf2b8607dfb536d9376d400a02b122a4542bb`，focus-tool 基于合并后的 `e37960f`。已有 Focus Python 3.11.15、Windows 10.0.26200、Go 1.26.5，独立缓存 PostgreSQL 17 容器。45/45 个源码文件指纹在两组采集前后一致；原始 summary 与纯标准库重算对象相同。采集后仅修正绕过构造器的控制测试夹具，原始指纹及数据未改写，详见下方验证说明。执行语义、权限／租约、fsync、checkpoint／receipt／ack 均保留，没有模型 API 调用或新增运行依赖。

固定沿用上一轮 corpus 和 SHA 次数：A 的 8 MiB／1461 次，B 的 8 MiB／365 次。barrier 只等同一 Context 原始批，B 自然结束；保留纠偏／必要扫描两场景、相同最终产物、实际取消确认和完整关闭。probe-on 和 probe-off 的输入 SHA-256 与最终产物逐项相同，详细边界见 [方法](task-progress-probe-method.md)。

- 正式 probe-on：[配置](../results/task-progress-probe-complete-2026-10-31/config.json)／[汇总及所有 sample 索引](../results/task-progress-probe-complete-2026-10-31/summary.json)。每臂 10 次正式＋1 次 warmup，轮换场景及模式顺序。
- probe-off 控制：[配置](../results/task-progress-probe-off-2026-10-31/config.json)／[汇总](../results/task-progress-probe-off-2026-10-31/summary.json)。每臂 3 次正式＋1 次 warmup，在 probe-on 完成后单独采集。
- [重算与跨组一致性检查](../results/task-progress-probe-complete-2026-10-31/reanalysis-check.json)：重算全部 60 个 trial；40 个正式 probe-on trial 共 25,164 条 SQL 执行记录、7,396 个 span，未记录 SQL 或 span 错误。
- [未完成的首批采集](../results/task-progress-probe-2026-10-31/failure.json)：21 个 trial 完成，第 22 个在创建环境、首次连接本地端口 49865 时超时，尚未进入该 trial 的任务测量。配置、全部已完成样本及容器日志原样保留，没有从中挑选样本补入正式批次。日志不能定位具体网络／数据库根因；重新完整采集成功不等于该环境问题已修复。

## 有 work 的时间花在哪里

先复算上一轮原始 trace，纠偏 event 的有任务累计中位数为 1162.68 ms、空查为 16.43 ms，与用户复算相同。本次内层 probe drain 对应数值为 **521.85 ms／8.62 ms**；与旧记录同口径的外层 task_progress span 为 522.10／8.67 ms。不同日期、宿主负载及观测开销均不同，且本轮未实施业务优化，不能把两次绝对时间下降当作优化收益。

以下均取本次纠偏 event 首次响应窗口，每个 trial 内先累计调用，再对 10 次取中位数，单位 ms。`work` 指尝试处理，不单凭 drain 返回值宣称发布成功；本组最终业务断言另行通过。SQL 是当前 Engine 的 cursor 执行次数与驱动往返耗时，不是数据库 CPU 时间。父子行重叠，不能相加。

| 原生阶段 | 调用数 | inclusive | exclusive | SQL 次数 | SQL 时间 |
|---|---:|---:|---:|---:|---:|
| drain（有 work，父） | 2 | 521.85 | 0.12 | 245 | 371.22 |
| 领取 claim | 2 | 134.28 | 33.13 | 46 | 104.03 |
| ↳ 输入冻结／绑定 | 2 | 88.98 | 88.98 | 34 | 71.73 |
| 输入装配（含重核重读） | 6 | 9.72 | 9.72 | 2 | 6.07 |
| payload 装配 | 2 | 0.06 | 0.06 | 0 | 0 |
| candidate schema | 2 | 3.77 | 3.77 | 0 | 0 |
| 预算请求消息装配 | 2 | 3.49 | 3.49 | 0 | 0 |
| 预算估算 | 2 | 1.31 | 1.31 | 0 | 0 |
| 预算预留 | 2 | 127.06 | 20.36 | 70 | 91.58 |
| 本地确定性 provider | 2 | 0.06 | 0.06 | 0 | 0 |
| 用量回填 | 2 | 110.09 | 17.37 | 70 | 80.90 |
| 校验 apply | 2 | 0.23 | 0.23 | 0 | 0 |
| 校验诊断记录（含重核） | 2 | 22.87 | 6.19 | 8 | 14.71 |
| 发布（repository 返回） | 2 | 79.31 | 78.72 | 43 | 62.64 |
| accounting 事件登记 | 4 | 193.40 | 83.91 | 124 | 147.19 |
| ↳ accounting view | 4 | **109.33** | **109.33** | **64** | **83.56** |
| DBAPI commit 返回 | 12 | 19.25 | 19.25 | — | — |
| 初始／续租复核 | 2 | 18.82 | 15.71 | 4 | 11.78 |

accounting 事件登记嵌套于预算预留／回填，accounting view 又嵌套在事件登记内；commit 也已经计入其原生外层。exclusive 仅扣除被观测直接子跨度与父生命周期交集的并集，仍包含未单列的业务和 Python／ORM 工作，不表示纯 CPU。

两个 work 分别经历 reserved 与 actual：reserved 的 view 累计中位数 **57.27 ms、2 次 read、32 次 SQL**；actual 为 **49.27 ms、2 次 read、32 次 SQL**。两类状态各自保留，分别取中位数不能相加恢复合并中位数。六次 input_assembly 仅有两次 SQL，也说明“方法调用次数多”不能直接理解为同样数量的数据库重读。

在当前观测粒度下，accounting view 是 exclusive 最大的命名阶段，其后有输入冻结 88.98 ms、accounting 事件登记剩余部分 83.91 ms、发布 78.72 ms。领取／发布和事件登记仍有实质成本；不能用一个 view 解释全部 521.85 ms，更不能从这些跨度直接推断数据库执行计划、锁等待或服务端 CPU 根因。

## 响应与关闭探针控制

同一行是 10 个 probe-on 样本或 3 个 probe-off 控制的中位数，单位 ms。总耗时包含实际取消清理、产物读回、持久消费／ack、激活结算及完整关闭。

| 场景／模式 | probe-on 响应 | probe-off 响应 | probe-on 总耗时 | probe-off 总耗时 |
|---|---:|---:|---:|---:|
| 纠偏 barrier | 7921.86 | 7608.30 | 12129.53 | 11376.69 |
| 纠偏 event | 2838.55 | 2645.53 | 7126.97 | 6381.88 |
| 必要扫描 barrier | 7343.98 | 6978.34 | 9499.67 | 9022.75 |
| 必要扫描 event | 7334.08 | 7013.66 | 9832.77 | 9117.14 |

关闭探针控制的有 work／空查中位数：纠偏 barrier 为 397.30／6.50 ms，纠偏 event 为 473.55／6.88 ms；必要扫描 barrier 为 427.24／6.68 ms，event 为 464.82／8.49 ms。两批按先 on 后 off 执行、样本量不同，宿主存在波动；此控制披露观测条件，**不能据此精确估计探针固定开销或宣布其无扰动**。主要内部定位依据是 probe-on 的真实嵌套记录与逐 SQL 重算。

纠偏 event 10/10 取消失效 scan，必要扫描两模式均 10/10 自然完成。必要扫描仍需独立保留其端点和额外激活成本，不能把接近的两个响应中位数解读成稳定收益。

## 下一项优化落点

建议下一项实际变更放在 Focus 的 `journal/accounting_query.py`／`accounting_events.py`：先针对每次 accounting view 的多次查询、整行 ORM 装载和同一读取过程中的派生汇总，研究能够减少往返或读取量的改动。用同样 probe、相同固定负载和完整 barrier／event／必要扫描对照验证；`focus-tool` 保留探针和前后数据，Focus 核心不导入 `focus_tool`。

reserved 与 actual 是不同账本状态，不能跨状态直接共用旧 view，也不能合并它们的 durable 事件。查询优化需保留 Round 历史、unknown、预留、活动 owner 和同事务记账语义。本轮没有进行这一业务优化，也未测试大量历史 work 下的扩展趋势；这些数据尚不能证明“装载全部历史 work”已经是当前最大内部根因。

## 验证与边界

`python integration/focus/benchmark_patrol.py --analyze <目录>` 可用标准库重算叶子 SQL 对父 span 的计数／时间／错误归属、阶段统计、响应终点、业务输入及最终产物。原始数据及源指纹不改写，采集后追加的 `reanalysis-check.json` 只保存独立核验与状态拆分结果。

首次全量验证在旧控制测试夹具失败：[原始失败记录](../results/task-progress-probe-verification-2026-10-31.json)。夹具通过 `object.__new__` 绕过构造器，因此补齐其应有的 `progress_probe=None`，随后重新完整验证。此修正只改变 `integration/focus/test_wake_controls.py`，其余 44 个源码文件（包括运行时、探针和性能基准）保持一致；[独立变更记录](../results/task-progress-probe-post-validation-2026-10-31.json) 保留原／新指纹及原因，未重写采集配置、summary 或 trace。

第二次验证在 Demo／验证 JSON 写入时遇到 D 盘空间不足，报告文件为空；[失败摘要](../results/task-progress-probe-disk-full-2026-10-31.json) 与空文件保留。之后把完整验证的输出改到有可用空间的 C 盘独立临时目录，再归档到仓库；没有改变测试、运行时配置或删除其他项目文件。这是验证环境处理，不是性能优化。

本地最终执行记录：[全量验证](../results/task-progress-probe-final-verification-2026-10-31.json)、[真实 Focus 三例](../results/task-progress-probe-final-live-2026-10-31.json)、[跨激活／恢复七例](../results/task-progress-probe-final-patrol-2026-10-31.json)、[自动唤醒八例](../results/task-progress-probe-final-automatic-2026-10-31.json)、[控制测试](../results/task-progress-probe-final-controls-2026-10-31.json)。固定研究 launch／provider、独立 service lease、完整生产 supervisor／数据库迁移和桥接历史保留的边界保持不变；核心 CI 不运行可选 PostgreSQL 性能基准。本轮未将本地确定性 provider 的成本外推为真实模型 Agent 性能。

最终本地 Go tests／vet、43 项 Python 测试（含新增 5 项）、完整 Demo、真实 Focus 3 例、Patrol 恢复 7 例、自动唤醒 8 例和控制测试 6 项均通过；[M1 九个 trial 回归](../results/task-progress-probe-m1-2026-10-31.json) 也通过，其数字不作为本轮内部成本结果。规范与规格两条独立审查无剩余阻塞项。
