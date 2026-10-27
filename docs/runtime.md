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

一批提交不保证原子性。某个调用被接收、后续调用因容量或工具注册失败时，批量适配接口仍为每个 call ID 返回一条配对消息：已接收的为 accepted，未接收的为 rejected，避免已执行任务丢失提交回执。

每次模型调用必须有稳定的父消息 ID 和 call ID。后台 job ID 由两者确定性计算，并以 Context／Run 为身份作用域。同一身份参数相同时返回既有任务，参数不同时拒绝。一个 Context 只能有一个活动 Run；要开始后继 Run，先关闭旧 Run。

## 调度与依赖

`runtime.New(Config)` 提供可直接使用的 Go 接口。Concurrency 限制实际执行数；Capacity 同时限制非终态任务与未 ack 的完成事件，慢消费者会收到背压。按 Run 轮转选择任务，跳过依赖或资源阻塞的队首，允许同 Run 的独立任务推进。长期吞吐与尾延迟仍需独立测量。

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

`Run.revise` 持久化新版本并撤销旧版本的排队／运行任务。旧版本终态仍保存，Run 在决策入口拒收并确认该失效事实。当前版本策略保守地撤销全部旧任务；跨版本保留兼容结果仍属后续优化。

取消状态严格区分 cancel_requested 和 cancelled。排队取消不启动进程；运行中请求取消后，必须等待执行器返回、直接子进程 Wait 回收后才释放执行槽与冲突资源。取消和正常结束竞态允许记录 completed，以真实退出结果为准。`Run.close` 禁止新提交并取消未完成任务；运行时 close 等待活动执行退出。

## 事件、ack 与恢复

完成事件包含全局递增 sequence、完整 Task（含 Context、Run、revision）及 Result。stdio 连接的唯一事件泵从耐久记录读取，再由 RuntimeClient 按 `(context_id, run_id)` 投递到各 Run 的独立 mailbox；结果正文无权改变归属。每个 Run 按事件顺序消费，保存决策 checkpoint 后 ack。不要同时为同一 Run 启动多个消费者。

ack 只允许确认当前 Run 的下一个未消费事件，重复 ack 幂等，越序或跨归属 ack 被拒绝。收到事件、形成终态和已消费是不同事实。重连会重投未 ack 事件；消费者必须使用 sequence／job 身份对外部动作去重。服务退出前仍可能有尚未传输的已落盘事件，下次连接按水位恢复。

journal 为 fsync 的 JSONL，每个终态及其完成事件写在同一个 frame。Windows 使用内核 LockFileEx、Unix 使用 flock，防止两个进程同时拥有 journal。重启时截断未完成的最后一帧，完整帧损坏则拒绝启动。运行中或取消中的任务恢复为 unknown，禁止自动重放；其读写资源继续隔离，避免未停止的旧任务与新任务冲突。已确认终态任务重试提交时返回原结果，不再执行副作用。

queued 任务的恢复需要重新通过执行前检查。新 Python 适配器默认拒绝尚未重新登记到本地调用表的任务；恢复时先建立可信注册与准入策略，不能默认批准历史排队任务。

## JSONL 方法

所有请求带递增 `id`、`method`，响应使用相同 id；event 与 authorize 是独立帧。stdin／stdout 专用于协议，诊断写 stderr。协议输入上限 4 MiB。

| 方法 | 关键字段 | 效果 |
|---|---|---|
| open_run | owner、revision | 注册 Run 或更新计划；拒绝版本倒退和 Context 双活动 Run |
| submit | task | 幂等登记任务；检查容量、执行器、依赖及资源 |
| status | owner、task_id | 获取所属任务的执行状态 |
| cancel | owner、task_id | 请求取消指定任务 |
| events | owner、after | 查询该 Run 未 ack 的后续事件 |
| ack | owner、sequence | 顺序确认持久消费水位 |
| authorize | token、allowed、reason | 回答执行前检查；过期 token 失效 |
| close_run | owner | 停止 Run 的新任务并取消未完成任务 |
| close | 无 | 取消并等待活动执行结束，再退出服务 |

## 运行边界

- 工具是可信本地代码，要求为不生成后代的叶子进程。通用 Shell、子进程树、沙箱隔离与生产权限系统尚未实现；不得宣称已提供这些能力。
- Python 工具以 `module + function + kwargs` 执行，支持同步及 async 返回值；结果必须可编码为 JSON。工具 stdout 重定向至 stderr，避免污染协议；单个工具 stdout／stderr 各保留最多 1 MiB，超限形成失败事件。
- 每个服务实例单独拥有 journal。stdio 连接可集中服务多个 Context；多独立客户端或跨机器分布式服务不在当前协议范围内。
- journal 研究上限为 128 MiB，超过上限 fail closed，需归档后继续；未实现在线压缩、自动 unknown 清理或通用 exactly-once 副作用。
- Focus 主仓尚未部署此适配器。实际权限、租约和 checkpoint／ledger 的事务一致性需在主仓接入时验证；独立仓库测试使用真实本地工具与等价 AIMessage 字段，未调用模型 API。
