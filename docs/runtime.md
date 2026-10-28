# 本地运行时协议与 Focus 适配

原图拓扑保持为 Run → Focus 工具适配层 → 统一调度器 → Python／进程执行器 → 结果路由 → 所属 Run。实现使用 Go 与 Python 标准库，不调用模型服务，不引入外部任务调度框架。

## 启动与接入

完整演示：`python examples/demo.py`。服务也可独立启动：

```powershell
go build -o bin/focus-tool-runtime.exe ./cmd/runtime
```

Python 接入时将仓库的 `python` 目录加入模块搜索路径（例如设置 `PYTHONPATH`），导入 `focus_tool`。每个工作区启动一个 RuntimeClient，多个 Context 的 Run 共用这个客户端和调度器。服务只使用本地 stdio，不监听网络端口。

```python
from focus_tool import FocusToolAdapter, RuntimeClient, ToolDefinition

client = await RuntimeClient.start(
    ["bin/focus-tool-runtime.exe"], journal=".focus-tool/runtime.jsonl",
    concurrency=4, capacity=128,
)
adapter = FocusToolAdapter(
    client,
    {"start_read": ToolDefinition("python", module="my_tools", function="read_config")},
    workspace="D:/project",
    before_execute=check_current_authority_and_workspace_lease,
)
run = await client.open_run("context-A", "run-A", revision=1)
try:
    # message 可以是 Focus 的 AIMessage，也可以是等价的 dict。
    submission_messages = await adapter.submit_tool_calls(run, message)
    # 将 submission_messages 转为 Focus 现有 ToolMessage，并由现有 ledger 记录提交。
    event = await run.next_event()
    await save_focus_decision_input_and_checkpoint(event)
    await run.ack(event)
finally:
    await run.close()
    await client.close()
```

`check_current_authority_and_workspace_lease`、checkpoint 保存以及 ToolMessage 对象构造使用接入方真实实现。适配器本身不导入 LangChain；它返回 `role/tool_call_id/name/content` 字段供现有消息层使用。注册的模型工具必须明确表达“启动后台任务”，accepted 表示提交完成；真实结果经 Run 的事件输入返回。不得把现有同步工具语义静默替换为 accepted，也不得为同一个 call ID 再生成第二条 ToolMessage。

一批提交不保证原子性。适配接口为每个 call ID 返回一条配对消息：明确接收为 accepted，明确拒绝或本地未发送为 rejected；发送后断连、回执丢失或提交日志写入不确定则为 acceptance_unknown，并携带原 job_id。未知状态不能被解释为“没有执行”。

遇到 acceptance_unknown，应重连后使用 `adapter.reconcile(run, parent_message_id, call_id)` 查询，或以完全相同的父消息、call ID、参数重提；禁止通过换调用 ID 盲目重试写操作。适配器保留不确定调用的注册信息，取消提交等待也不会撤销可能已经发生的接收事实；同一不确定调用在断连客户端上再次遇到“请求未发送”，仍保持 acceptance_unknown。明确服务拒绝与传输不确定分别使用 RemoteRejected 和 TransportError。

每次模型调用必须有稳定的父消息 ID 和 call ID。后台 job ID 由两者确定性计算，并以 Context／Run 为身份作用域。同一身份参数相同时返回既有任务，参数不同时拒绝。一个 Context 只能有一个活动 Run；要开始后继 Run，先关闭旧 Run。

## 调度与依赖

`runtime.New(Config)` 提供可直接使用的 Go 接口。Concurrency 限制实际执行数，Capacity 限制全局非终态任务与未解决的 unknown 隔离；PerRunCapacity 限制各活动 Run 的“非终态任务＋未 ack 结果”，默认 Capacity／Concurrency。Python 参数名为 `per_run_capacity`，CLI 为 `-per-run-capacity`。`max_active_runs`／`-max-active-runs` 限制活动 Run 数，默认 max(128, capacity)。一个停止消费的 Run 达到自身额度会收到 ErrRunFull，但其已完成结果不会占用其他 Run 的任务容量。unknown 保留全局额度直到明确解决，避免反复崩溃产生无限隔离。全局执行资源仍共享，不提供正在执行任务的抢占。

按 Run 轮转选择任务，跳过依赖或资源阻塞的队首，允许同 Run 的独立任务推进。完整链路的对照和 1,024 任务持续提交实测见 [边界修复与性能报告](boundaries-and-performance.md)。

依赖必须引用同一 Run、同一计划版本中已提交的任务，因此不允许循环或跨 Run 偷读结果。全部依赖成功后才启动任务；任一依赖失败、取消、skipped 或 unknown，则后继任务记为 skipped。Python 参数可从依赖 JSON 输出绑定，例如：

```python
await adapter.submit(
    run, parent_message_id="message-2", call_id="child",
    name="start_increment", args={}, dependencies=[parent_job_id],
    bindings={"value": {"task_id": parent_job_id, "path": ["value"]}},
)
```

路径支持对象字段及数组索引。缺字段、非 JSON 输出等绑定失败会成为该任务的失败事件，不执行工具。资源以显式名称及 read／write 声明；相同名称允许读读并行，涉及写入时互斥。调用方应使用工作区／文件等稳定的共同键；运行时不从工具名称推测副作用，也不自动推导目录层级冲突。

## 执行前检查与取消

服务默认在每次实际执行前发 authorize 请求，Python 适配器核对工具注册、身份、工作区和参数，然后执行 `before_execute` 回调。回调可同步或异步，拒绝或异常会形成失败事件，不启动工具。缺失适配器授权处理时默认拒绝。直接使用 Go API 时通过 Config.BeforeExecute 接入相同检查；可信独立 Go 实验可以不配置该钩子。

诊断追踪默认关闭。Go 可配置 `Config.Trace`，CLI 使用 `-trace <新的 JSONL 路径>`；Python `RuntimeClient.start` 可传 `trace_path` 和同步 `trace` 回调。诊断缓冲不 fsync、不替代执行事实或消费回执。子进程的内部计时帧会在 Go 执行器内解包，原结果与归属不变；取消仍等待实际退出。阶段定义及独立基准见 [短工具测量合同](short-tools-method.md)。

`Run.revise` 持久化新版本并撤销旧版本的排队／运行任务。旧版本终态仍保存，Run 在决策入口拒收并确认该失效事实。当前版本策略保守地撤销全部旧任务；跨版本保留兼容结果仍属后续优化。

取消状态严格区分 cancel_requested 和 cancelled。排队取消不启动进程；运行中请求取消后，必须等待执行器返回、直接子进程 Wait 回收后才释放执行槽与冲突资源。取消和正常结束竞态允许记录 completed，以真实退出结果为准。`Run.close` 禁止新提交并取消未完成任务，同时持久地把剩余结果收尾责任转交给 runtime_archive；运行时 close 等待活动执行退出。

## 事件、ack 与恢复

完成事件包含全局递增 sequence、完整 Task（含 Context、Run、revision）及 Result。stdio 连接的唯一事件泵从耐久记录读取，再由 RuntimeClient 按 `(context_id, run_id)` 投递到各 Run 的独立 mailbox；结果正文无权改变归属。每个 Run 按事件顺序消费，保存决策 checkpoint 后 ack。不要同时为同一 Run 启动多个消费者。

等待只订阅“邮箱可读”的信号，所有可取消的清理等待完成后，才同步取出事件并交给 Run。取消等待不会取走事件；失效版本的隐式 ack 若被取消，也会把原事件放回队首。已交给调用方但尚未 ack 的事件仍由调用方保管，重连可按持久水位重投。

ack 只允许确认当前 Run 的下一个未消费事件，重复 ack 幂等，越序或跨归属 ack 被拒绝。收到事件、形成终态和已消费是不同事实。重连会重投未 ack 事件；消费者必须使用 sequence／job 身份对外部动作去重。服务退出前仍可能有尚未传输的已落盘事件，下次连接按水位恢复。

关闭 Run 返回含 `custodian=runtime_archive`、pending_executions 和 unacknowledged_facts 的回执。剩余完成／取消事实继续写入耐久记录，保留原 owner、sequence 与 Ack，不伪造消费确认。它们退出活动事件泵，后续通过 `Run.archive_page()` 或 `iter_archived_events()` 分页查询，包括关闭前已 ack 的历史事实；`archived_events()` 仅适用于单页归档，超出时明确要求分页。原 Run 的 next_event 和 ack 不再有消费权限。关闭时 pending_executions 可大于零，之后到达的终态由 archive 保管。资源仍等实际退出才释放，unknown 的资源隔离也不会因归档解除。

journal 为 fsync 的 JSONL，每个终态及其完成事件写在同一个 frame。Windows 使用内核 LockFileEx、Unix 使用 flock，防止两个进程同时拥有 journal。重启时截断未完成的最后一帧，完整帧损坏则拒绝启动。运行中或取消中的任务恢复为 unknown，禁止自动重放；其读写资源继续隔离，避免未停止的旧任务与新任务冲突。已确认终态任务重试提交时返回原结果，不再执行副作用。

queued 任务的恢复需要重新通过执行前检查。新 Python 适配器默认拒绝尚未重新登记到本地调用表的任务；恢复时先建立可信注册与准入策略，不能默认批准历史排队任务。

活动结果和未完成状态受配额限制；已确认／关闭的历史在磁盘索引与有界近期 WAL 缓存中保留，按需查询。每条变更仍单独 fsync；段达到 `segment_bytes`／`-segment-bytes`（默认 8 MiB）后合并索引并压缩。旧 ID 和依赖结果不随压缩过期，unknown 不随归档解锁。游标绑定 owner 与查询上界，默认每页 100 条／2 MiB，最大 1000 条／4 MiB。详见 [存储协议及恢复边界](history-retention.md)。升级需先停止旧进程；备份须包含 journal、`.store` 和任何 `.sealed` 段。

客户端只订阅活动 Run。关闭后迟到的帧被丢弃于连接缓冲层，事实仍由耐久归档保管；重新打开活动 Run 时使用 `event_page` 补取历史未确认结果，并与实时泵按序去重合并。历史终态的重提／reconcile 不保留多余工具授权注册。

## JSONL 方法

所有请求带递增 `id`、`method`，响应使用相同 id；event 与 authorize 是独立帧。stdin／stdout 专用于协议，诊断写 stderr。协议输入上限 4 MiB。

| 方法 | 关键字段 | 效果 |
|---|---|---|
| open_run | owner、revision | 注册 Run 或更新计划；拒绝版本倒退和 Context 双活动 Run |
| submit | task | 幂等登记任务；检查容量、执行器、依赖及资源 |
| status | owner、task_id | 获取所属任务的执行状态 |
| cancel | owner、task_id | 请求取消指定任务 |
| events | owner、after | 单页小结果兼容接口，需继续分页时返回明确错误 |
| event_page | owner、cursor 或 after、limit、max_bytes | 分页补取活动 Run 未 ack 的事件 |
| archive_events | owner、cursor 或 after、limit、max_bytes | 返回 items／next_cursor／through，查询关闭 Run 历史，不改变 Ack |
| compact | 无 | 在线合并索引、轮换 WAL 并删除冗余段，保留执行事实 |
| stats | collect（可选） | 活动任务／结果／Run、隔离、近期 WAL 缓存、压缩次数、WAL 字节和 Go 堆；collect=true 在采样前触发 GC |
| ack | owner、sequence | 顺序确认持久消费水位 |
| authorize | token、allowed、reason | 回答执行前检查；过期 token 失效 |
| close_run | owner | 关闭 Run、取消任务，返回向 runtime_archive 转交结果保管权的回执 |
| close | 无 | 取消并等待活动执行结束，再退出服务 |

## 运行边界

- 工具是可信本地代码，要求为不生成后代的叶子进程。通用 Shell、子进程树、沙箱隔离与生产权限系统尚未实现；不得宣称已提供这些能力。
- Python 工具以 `module + function + kwargs` 执行，支持同步及 async 返回值；结果必须可编码为 JSON。工具 stdout 重定向至 stderr，避免污染协议；单个工具 stdout／stderr 各保留最多 1 MiB，超限形成失败事件。
- 每个服务实例单独拥有 journal。stdio 连接可集中服务多个 Context；多独立客户端或跨机器分布式服务不在当前协议范围内。
- 已去除 128 MiB 整体 journal 上限，使用流式回放和单帧 32 MiB 上限。在线压缩保留 canonical 磁盘事实；磁盘总量随事实增长，磁盘满或写入失败仍 fail closed，不自动清除 unknown 或提供通用 exactly-once 副作用。
- 耐久模式的活动状态受配额限制，近期历史受 WAL 段大小限制；内存模式仍保留全部历史。单个事件超过 4 MiB 不能由当前分页接口返回，会明确报错，需要后续 blob／分块协议。有限实验不能证明无限时长或生产 Patrol 的稳定性。
- 已穿过真实 Focus Run worker、权限／租约、PostgreSQL checkpoint／ledger／inbox 做三例小范围联调，见 [真实接入](../integration/focus/README.md)。决策为确定性本地代码；未部署生产 Focus Desktop 或验证 Patrol 重调度，不宣称跨数据库原子事务。
