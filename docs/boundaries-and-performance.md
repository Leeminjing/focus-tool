# 正确性边界与完整运行时性能验证

日期：2026-10-27。针对提交 `4fca176e` 的检查，先固化三个可失败复现，再修复；本次性能数据经过新增共享 runtime、Python 适配器和 fsync journal。历史 M1 数据保留作独立实验，不用来代表本次新增运行时。

## 三项边界修复

| 边界 | 修复与验证 |
|---|---|
| 等待取消吞交付 | 用可读信号等待，清理完可取消等待后才同步取队首；取消与到达碰撞时仍按 1、2 顺序交付；失效结果 ack 被取消也恢复队首 |
| 回执断连误报拒绝 | 区分 RemoteRejected 与 TransportError；不确定提交保留原 job ID 和注册信息，返回 acceptance_unknown；后续未发送的重试不覆盖此前的不确定性 |
| 无人消费 Run 堵塞其他 Context | 全局 Capacity 只限制非终态任务；PerRunCapacity 限制本 Run 任务与未消费结果；关闭后持久转交 runtime_archive，保留原事实和 Ack，原 Run 失去消费权 |

可重跑回归：`python -m unittest discover -s python/tests -p test_delivery_contract.py -v`，以及 `go test ./runtime -run "TestClosedRunRetainsFacts|TestUnacknowledgedResults" -count=1`。最初实际捕获的失败分别为 `2 != 1`、`rejected != acceptance_unknown`、`closed Run blocked B: runtime capacity exhausted`。

提交不确定时只能查询／重提原身份，不能换新 ID 重做写操作。关闭的 Run 只移交结果保管责任，未完成执行仍需停止确认，unknown 资源隔离不解除。详细合同见 [运行时协议](runtime.md)。

## 完整链路 barrier／event 对照

```powershell
python benchmarks/full_runtime.py --out results/local/full-runtime-benchmark.json
python benchmarks/full_runtime.py --negative-control --repeat 3 --out results/local/negative-control.json
```

相同配置：一个 Go daemon、Python FocusToolAdapter、执行前准入回调、本地 Python 工具／进程、fsync JSONL、决策输入文件 fsync 后 ack；并发上限 2、全局任务容量 32、每 Run 配额 16。每个模式先预热 1 次，正式纠偏负载各 5 次，模式顺序逐轮轮换。输入固定为 8 MiB 文件，扫描做 512 遍 SHA-256。编译和生成数据不计时；计入服务／工具进程启动、完整协议、checkpoint、ack、修复、真实取消确认和服务退出。

| 负载 | barrier 总耗时中位数 ms | event 总耗时中位数 ms | 各组次数 |
|---|---:|---:|---:|
| 配置错误，扫描失效 | 3486.87 | 767.53 | 5 |
| 配置有效，扫描必须完成（负对照） | 2182.94 | 2175.34 | 3 |

纠偏负载下约 4.5 倍的差异来自提早消费检查结果并停止失效扫描；全部 event 试验都确认了扫描取消、修复产物相同。无需纠偏时两组耗时近似，不能把这种小样本差异解释为显著收益。

纠偏组检查结果的决策消费滞后中位数：barrier 2764.11 ms，event 1.06 ms。

这些是特定本地工作负载的端到端结果，不是 Go 相对 Python 的加速比，也不是完整 Focus／模型任务的加速比。文件扫描工作量人为固定；单机负载、缓存、进程启动和持久化都会改变绝对时间。样本少，不作置信区间或统计显著性主张。两份原始报告包含 LF 归一化源码 SHA-256，已核对与本次源码一致。

## 多 Context 持续负载与内存

配置：4 个持续消费的 Context，每个保持最多 4 个在途任务；并发 4、全局任务容量 64、每 Run 配额 16。另有一个停止消费的 Run，先堆满 16 个已完成结果，并在整个负载期间不 ack。

共 1024 个新任务，8 个 epoch，每 epoch 每个活动 Run 32 个任务；总时长 63.42 秒。每个 epoch 结束、活动任务消费完后，对 Go 触发 GC 再采样。这里只测 Go 堆，不冒充整个进程树的 RSS。

| epoch | 累计秒 | 排队 p95 ms | 结果消费滞后 p95 ms | GC 后 Go 堆 MiB | 保留任务数 |
|---|---:|---:|---:|---:|---:|
| 1 | 10.5 | 972.8 | 10.5 | 1.057 | 144 |
| 2 | 17.7 | 783.5 | 9.0 | 1.285 | 272 |
| 3 | 24.7 | 858.8 | 10.4 | 1.529 | 400 |
| 4 | 33.9 | 981.7 | 7.4 | 1.728 | 528 |
| 5 | 42.6 | 916.7 | 9.0 | 2.000 | 656 |
| 6 | 50.1 | 741.9 | 36.2 | 2.149 | 784 |
| 7 | 57.5 | 840.4 | 4.9 | 2.342 | 912 |
| 8 | 63.4 | 595.2 | 4.5 | 2.615 | 1040 |

活动 Context 全部完成任务，停止消费 Run 未堵住其提交。Python 调用注册表和邮箱每次采样均只保留该停止消费 Run 的 16 项；关闭后这 16 条事实转交 archive，活动未消费数归零。Go 历史索引仍保留 1040 个任务／事件；GC 后堆从 1.057 MiB 增至 2.615 MiB。

**本次确认了历史保留导致的堆增长，尚未修复。** 结果保管权转交不会删除历史，也不等于内存压缩。在这个短窗口内排队与消费延迟没有单调恶化，但不能外推至无限时长；分页索引／journal 压缩应作为下一阶段研究。

queue_ms 从入队时间到执行槽位分配时间；consume_lag_ms 从准备终态记录到 Python 决策消费时间，含终态 fsync 与交付。它们不等于模型推理延迟，也没有把进程墙钟当 CPU 时间。

## 真实 Focus 小范围接入

使用真实 Focus 检出 `340bf2b8`，执行实际 Run worker、权限 middleware、WorkspaceLeaseManager、PostgreSQL ledger／inbox receipt／checkpoint。允许执行、提交后权限撤销、提交后租约释放三例通过；拒绝场景没有 PID 和目标文件，允许场景产生真实文件。每例均核对两次 lease 检查、一个提交 ledger attempt 和一个精确 checkpoint inbox receipt。

决策源是本地确定性 StateGraph，没有模型调用。Windows Selector loop 通过核心客户端的真实 Popen＋thread pipe fallback 接到同一 Go daemon。未覆盖 Desktop HTTP、Patrol 对结束 Run 的重调度或生产迁移；不宣称跨数据库原子事务。复现与固定源码依据见 [接入说明](../integration/focus/README.md)。

## 测试与原始记录

本地完整 Go 测试、go vet、Python 回归／集成测试、完整演示及真实 Focus 三例已通过。统一命令 `python scripts/verify.py` 可追加 `--focus-root`、`--focus-python`、`--focus-out` 启用真实联调。Linux race 检测和双平台结果以本次提交的 GitHub Actions 为准。

- [纠偏对照与持续负载原始数据](../results/full-runtime-benchmark-2026-10-27.json)
- [负对照原始数据](../results/full-runtime-negative-control-2026-10-27.json)
- [完整本地验证输出](../results/boundary-verification-2026-10-27.json)
- [真实 Focus 三例](../results/focus-integration-2026-10-27.json)
