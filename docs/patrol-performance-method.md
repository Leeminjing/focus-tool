# 真实 Patrol 响应性能对照

本 PR 从 `3745f557` 开始，只增加可选 Focus 适配层计时、有限本地工具与独立基准，不调整调度算法、模型、数据库持久化策略、worker 池或 Go runtime。真实 Focus 固定 `340bf2b8607dfb536d9376d400a02b122a4542bb`，使用已有环境和单独缓存 PostgreSQL 容器，不调用模型 API，不触碰正式数据库。

## 公平对照

两个 Context 各有独立 Loop、工作区和服务 lease，共用实际 runtime（工具并发 4）与真实 RunPool（激活并发 2），planner 配额保持既有默认 4。barrier 只在**同一个 Context 的原始工具批**全有终态事实后允许后续决策；B 的单独任务完成后即可进入自己的 Patrol，不等 A。event 每个有用结果都可以触发后继。差异仅放在基准的 pending eligibility 端口，inbox、权限／lease hook、ledger、typed checkpoint、语义回执、WAL fsync 和 ack 完全沿用真实链路。

| 场景 | A 的工具和正确结果 |
|---|---|
| replan | check 发现配置无效；有限 scan 可以取消；后继真实激活修订计划并提交 repair，最终配置与 final.json 正确 |
| necessary | valid_check 不触发 repair 或取消；scan 必须自然完成，其事实进入持久语义回执后任务才算完成；event 早消费 check 可能产生额外激活 |

scan 使用固定 corpus 和固定次数的 SHA-256，B 使用自己的固定 corpus／次数并自然结束。工具没有无限等待、sleep 或测试 gate。默认先用 32 次探测校准目标约 8 秒，再冻结 A 的循环数，B 为其四分之一；所有场景／模式／轮次使用同一组循环数和 corpus。校准目标不是工具定时器，也不保证 event 一定赶在 scan 完成前取消；记录真实 completed／cancelled 分布。

两模式在同一场景下逐项验证 input.json、不可变 scan-input.json、workload.json 和 corpus 字节 SHA-256 相同；最终 A 的配置／final.json、B 的 final.json 完全相同。scan-fact 是可能失效的中间证据，不是用户最终产物。scan 只读取创建于测量前的不可变输入，不能读到 repair 后的新配置。取消成功仅取自 Go 实际终态，计入直接叶子进程 Wait 及结果消费确认，不声称进程树治理。

每场景／模式 1 次未计分 warmup，原样保留；随后每臂 10 次，共 40 个测量、4 个 warmup。按轮次交替模式和场景顺序。每 trial 使用新数据库、journal 和工作区，避免跨 trial 历史量不同；数据库准备、两个 bootstrap 和固定输入写入属于 setup，另行报告。正式采集独占执行，不与其他验证／基准并行。

## 时间与次数

`schedule_many()` 分解为 task_progress、round_maintenance、round_state_read、round_identity_read、observation_capture、coordinator_claim、patrol_decision、kernel_commit、coordinator_release。保存每次调用、每次迭代、处理 work 数，以及父 schedule_total。子阶段不与父跨度相加，跨 Loop 的重叠也不当作串行墙钟。

阶段内部 elapsed 使用同进程高分辨率 perf_counter_ns；关联 Go／Python 工具、通知和激活使用同机 Unix ns。原始 JSONL 同时保留 start／finish 与 measurement markers，不强行调整可能重叠的 commit 可见性／回调时序。首次响应窗口从 A 的检查完成至对应 Patrol 决策提交，包含期间重复尝试；不只挑最后一次成功调用。所有与窗口相交的调用都纳入，分别报告完整调用 total_ms、窗口内 wall overlap，以及穿过左右边界的次数；不丢弃跨界阶段，也不把父子时长相加。Unix clock 倒退诊断另存，不能用 overlap 代替高分辨率完整调用耗时。

主要任务窗口从 `PatrolResultService.start(initial_contexts)` 前开始，到最终产物读回正确、全部事实持久消费／ack、所有激活结算为止，包含初始真实 Patrol 规划。首次工具函数到同一终点也单列。之后完整 service／runtime／数据库关闭时间单列，并同时报告含 shutdown 的总时间。

响应指标分别是 replan 的 check 终态到 repair **实际函数入口**，以及 necessary 的 check 终态到必要 scan 的**语义回执事务成功返回**。后者由 consume 返回后独立 observer 记录，不使用事务内部较早生成的 consumed_at。两场景定义不同，不能混作一条分布；同场景内两模式定义相同。完成窗口要求完整预期终态身份清单、相应消费／ack、激活结算及成功读回验证，不因文件提前写出或尚无已投递事件就宣告结束。所有初始／后继阶段、执行事实、attempts、消费回执、激活、canonical 产物和状态计数均保存。

## 复现与重算

```powershell
& 'C:\Users\brubing\.focus\runtime\venv\Scripts\python.exe' `
  integration/focus/benchmark_patrol.py `
  --focus-root 'C:\Users\brubing\Desktop\ag-project\focus' `
  --repeat 10 --warmup 1 --out results/local/patrol-performance

python integration/focus/benchmark_patrol.py --analyze results/local/patrol-performance
```

输出目录必须不存在；失败记录、未计分 warmup 和原始 trace 都不覆盖。失败 trial 在资源关闭后保存 best-effort 事实／回执／typed checkpoint／attempts 快照，以及完整工作区 tar.gz（包括 WAL、.store 和任何 .sealed）；SQL 不可用等采集错误也写入。分析器从 gzip JSONL 的 measurement markers 和阶段 span 重算耗时／调用数，验证 cohort 完整、重复编号、输入与最终产物一致，再给出中位数。失败采集不能冒充完整 10 轮，不能剔除慢样本重跑后悄悄替换。

有限自然完成的预检暴露了研究适配层的 Round 交接问题：join 尚未完成时可能使用已提交旧 Round 的 observation。适配层现在 defer 已 settled 的旧 Round，并核对领取的 Round 与 observation 相同；不降 planner 并发、不修改 Focus 主仓。对应控制回归与预检失败记录单独保留，不与正式性能样本混合。

本 PR 只完成测量与定位；不将小样本外推为完整模型 Agent 的加速，也不在得到结果前预选 TaskProgress、Observation 或 Kernel 为瓶颈。
