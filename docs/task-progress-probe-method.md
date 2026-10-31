# TaskProgress 内部成本探针

本轮从 `e37960f` 开始，固定外部 Focus `340bf2b8607dfb536d9376d400a02b122a4542bb`。研究仓库包装真实方法，不拷贝 TaskProgress 或 accounting 业务实现，不修改 Focus 源码。核心 `runtime`／`python/focus_tool` 不导入 Focus 或 SQLAlchemy；SQLAlchemy 监听器仅在已有可选 Focus 环境内安装，没有新增运行依赖或模型调用。

## 观测边界

`--task-progress-probe` 显式启用；默认路径不安装探针。每个研究进程最多一个已安装探针，支持该探针下多个 Loop 并发。ContextVar 关联 drain、schedule、loop、work 和 fence；原生 claim 完成前的记录暂存，之后用返回的真实身份关联。原生 lease worker、keeper、provider 和 shielded 回填任务继承上下文。独立 Task 的 SQL 不会归入另一 Context。

| 名称 | 原生观测点 |
|---|---|
| claim／input_freeze | `TaskProgressRuntime._claim`／`TaskProgressQueue._bind`，包括领取事务、首次冻结和冻结复用检查 |
| input_assembly／payload_assembly | repository `consolidation_inputs`／原生 `interpretation_payload`；重复校验引起的重读也保留 |
| candidate_schema／budget_messages／budget_estimate | 原生 schema 构建、request_messages、estimate_request_budget；不是模型服务调用 |
| budget_reserve | `_reserve`，含锁、预留记账、accounting 事件和提交 |
| provider | 本地确定性 `LocalProgress.invoke`，没有实际模型 API |
| usage_settlement | `_record_usage`，与预留分开，保留不同状态 |
| validation_apply／validation_record／validation_recheck | consolidator apply、运行时诊断记录、repository 冻结身份重核 |
| publish | repository publish，返回不代表事务已提交；外层 consolidate／commit 单独记录 |
| accounting_record／accounting_view | `LoopAccountingEventRecorder.record`／`LoopAccountingQuery.read`；保留 reserved／actual 等 transition 和父跨度 |
| lease_execution／lease_renewal／interpret／consolidate | 原生父方法与续租阶段，解释未细分余量 |
| transaction_commit | 当前 Engine dialect 的 `do_commit`，到 DBAPI commit 返回；不是只触发提交前 event，也不表示已经单独测到服务端 fsync |

预算估算行只表示这些实际方法的执行时间；例如调用实参中额外的 `model_json_schema()`、schema 校验或其他 Python 装配可能留在 interpret 的 exclusive 余量，不能把它们默认为 SQL、空转或 provider。领取前的 enqueue／join 维护属于已有 Round 计时，不混入 drain 内冻结。

SQL 使用当前 Engine 的 before/after cursor 及 handle_error 边界。计数指 cursor 执行次数，耗时包含驱动往返，不等于服务端 CPU；连接等待、ORM 物化、flush 的 Python 工作不会自动计成 SQL。每条只保存语句 SHA-256、verb、所属 span、耗时和异常类型，不保存 SQL 正文或参数。DBAPI commit 是独立跨度；ORM flush 的 SQL 由 SQL 行计入，不能把两者当作同一指标。

每次函数仍仅调用原方法一次，返回值、原异常和取消继续传播。诊断接收器异常被隔离，未修改权限、租约、预算、WAL、checkpoint 或 ack；关闭时逆序恢复全部 hook／监听器，某项失败不阻止其余清理，未完成项保留供重试，并报告失败。安装回滚失败作为原异常的 note，不覆盖安装错误。诊断记录受本次有限 trial 的工作量约束，未声明桥接服务的长期历史保留已解决。

## 计时、归属和重算

所有 span 保存同机 Unix ns 和本进程 perf_counter_ns；内部 elapsed 由后者计算。父 inclusive 时间、SQL inclusive 时间都包含嵌套工作，不能跨层相加。exclusive 时间扣除直接子跨度与父生命周期的**交集之并集**，保留并发重叠和取消后迟到的完整子跨度，不把其父生命周期之外的时间扣走。

分类中的 `work` 表示原生 drain 返回非零，即尝试处理 work，不代表一定发布成功；原生 drain 可能捕获校验／发布异常后返回 1。`empty` 表示返回 0；`failed` 只表示 drain 自身抛出。错误、failure_record、SQL 错误和取消记录保留在原始 trace 中，不能仅凭 work 数推断业务成功。

首次响应窗口沿用上一轮，从 A 的 check 完成到首次后继 Patrol 决策提交；与窗口相交的 span 完整计入，同时另存窗口内 wall overlap 和边界穿越次数。完整 trial 的探针统计在服务清理完成后生成，避免关闭中的记录遗漏。分析器从 SQL 叶子事实按父链重新验证每个 span 的 SQL 计数、累计时间和错误数，再重算所有阶段统计；原始 JSONL、summary、源码指纹不因分析修改。

## 负载与运行

两 Context、同 Context 原始批 barrier、独立自然结束的 B、提前纠偏／必要扫描、真实 Patrol／Kernel／RunPool、执行准入、checkpoint-before-ack 和结果终点均沿用 [已有合同](patrol-performance-method.md)。本轮显式固定上一轮工作量：A 的 8 MiB corpus 扫描 1461 次，B 为 365 次；输入和最终产物逐项读回相等。依旧采用默认新进程执行，取消以终态和实际 Wait 退出确认为准。

正式探针采集每臂 10 次＋1 次 warmup，轮换场景及模式顺序；采集独占运行，不并行测试。另以相同固定次数运行关闭探针的控制采集，单列结果与宿主波动，不能从不同时段的两列中位数推断精确探针开销或优化比例。

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/benchmark_patrol.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --passes 1461 --repeat 10 --warmup 1 --task-progress-probe `
  --out results/local/task-progress-probe
python integration/focus/benchmark_patrol.py --analyze results/local/task-progress-probe
```

关闭探针控制使用同命令去掉 `--task-progress-probe`，输出到新的目录。两种采集都不能覆盖旧目录或替换慢样本。5 项纯标准库测试覆盖并发 owner／返回值、取消／异常、observer 隔离、SQL 计数与脱敏、嵌套并发／迟到跨度和失败清理重试；真实 SQL 与 commit 的语义由隔离 PostgreSQL 基准验证。本阶段只定位成本；是否修改 Focus 的 accounting 查询，需要由正式实测决定。
