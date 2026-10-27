# Focus 异步工具运行时：协议与执行边界核验

核验日期：2026-10-27。GitHub `main` 本次解析为 [`ec2201f395696e635cedf72eb486cb102047c178`](https://github.com/Leeminjing/Focus/commit/ec2201f395696e635cedf72eb486cb102047c178)，与提供的方案一致。以下区分源码事实与研发建议；未调用模型 API，未运行完整 Focus。

## 已验证的现状

1. **AIMessage 携带工具调用，工具结果通过 call ID 配对。** `decode_response()` 仅接受 `completed` 响应，对工具名、调用身份、JSON 参数和 schema 做校验，再构造带 `tool_calls` 的 `AIMessage`。`validate_messages()` 要求未配齐结果时下一条仍为 `tool` 消息，并拒绝重复/未知结果和结尾未完成调用。这是实际消息边界，不能只删掉一个 `gather()` 就提前插入下一条 AI 消息。[响应解码](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/models/response_output.py#L29-L81) · [消息校验](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/runtime/runs/events.py#L150-L183)

2. **同批工具可以并行，但普通模型回合存在阶段屏障。** Focus 使用 `create_agent()`；锁文件固定 `langchain==1.3.10`、`langgraph==1.2.6`。对应 LangChain 对每个未完成调用产生 `Send("tools", [tool_call])`。LangGraph 的 Pregel 执行阶段并行运行选中的 actor，直到全部完成、失败或超时，阶段写入在下一阶段才可见。因此“并行执行、等本批完成再推理”对普通工具路径成立，不能扩展成所有中断/委托/自定义控制路径都相同。[Focus 装配](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/agents/lead/agent.py#L174-L210) · [版本锁定](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/uv.lock#L574-L579) · [逐调用派发](https://github.com/langchain-ai/langchain/blob/a807a9c7f61f50404ac65ffbe0521bad61f5551f/libs/langchain_v1/langchain/agents/factory.py#L1820-L1865) · [阶段语义](https://github.com/langchain-ai/langgraph/blob/ce6c8b2410f9921d379c8fe8ea8087d1934c07a9/libs/langgraph/langgraph/pregel/main.py#L453-L476)

3. **当前流式适配器没有按参数生成进度提前交出可执行调用。** `accept()` 只产出正文和 reasoning 增量；`finish()` 完成组装与解码校验后才交出完整 `tool_call_chunks`。完整 JSON 与整个响应通过验证是不同边界。[流桥接](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/models/response_chunks.py#L31-L81)

4. **ContextRunPool 管的是 Run 容量。** 它按全局活跃 Run、每 Loop 活跃 Run 和已预留 Run 槽位分配容量。该文件没有统一工具级资源调度合同。[ContextRunPool](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/app/desktop/patrol/agent_loop/resources/context_run_pool.py#L23-L71)

5. **已有结果投递边界，但不是任意时刻打断推理。** `DurableInboxMiddleware.abefore_model()` 在下一次模型调用前准备消息，`awrap_model_call()` 根据精确 checkpoint 确认投递；消息还保留 recipient/task 来源，并在事务中登记投递回执。新工具完成邮箱可参考这种“准备—耐久写入—确认”的边界，但现有 inbox 并未因此自动变成能唤醒工具阶段的调度器。[耐久 inbox](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/app/desktop/agents/inbox.py#L25-L103)

6. **执行身份与完成记录已有严格合同。** `ToolExecutionMiddleware` 使用 thread、checkpoint namespace、agent、父 AI 消息、call ID 形成稳定身份，先 claim 再执行，最终要求对应 call ID 的 `ToolMessage`。桌面端 ledger 对已有意图但无可靠结果的调用拒绝自动重放，对相同身份但参数不同的调用拒绝执行。[middleware](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/runtime/tool_attempts.py#L25-L64) · [ledger](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/app/desktop/runs/execution_attempts/tool.py#L45-L74)

7. **取消请求不等于 Shell 已停止。** `AccessPolicyMiddleware` 对受管理 Shell 使用 `asyncio.shield()`；收到取消后设置调用级信号并等待底层任务完成，再传播取消。ledger 只有收到 `confirmed` 或 `not_started` 清理回执才解除 Shell 结算屏障；否则保留 `unknown`，阻止结算与自动重放。异步调度器必须兼容实际进程树清理，不能仅标记 `cancelled` 并立即复用冲突资源。[取消处理](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/security/middleware.py#L131-L152) · [清理结算](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/app/desktop/runs/execution_attempts/tool.py#L76-L110)

8. **排队之后执行前仍须重新验证治理状态。** 异步工具边界先刷新 runtime context，再核验 workspace lease，随后准入与执行。外置队列若在这些检查后长时间排队，就必须把最终检查移动/重复到真正执行前，不能只保留提交时检查。[执行准入](https://github.com/Leeminjing/Focus/blob/ec2201f395696e635cedf72eb486cb102047c178/backend/packages/harness/focus/security/middleware.py#L97-L129)

## 建议：把三类身份与三个完成边界分开

以下是新运行时设计建议，不是 Focus 已有功能。

| 对象 | 最小身份 | 完成的含义 |
|---|---|---|
| 模型工具调用 | 父消息 + call ID | 如工具名为 `submit_job`，返回 accepted 表示提交操作完成 |
| 后台执行任务 | job ID + attempt ID + 参数摘要 | 执行器产生真实终态；取消需要实际停止证据 |
| 结果交付 | event ID + recipient/context + 消费 checkpoint | 事件耐久进入相应决策输入后才确认已消费 |

`accepted` 方案可闭合原来的 tool-call 配对，但只解决消息层的提交返回。必须为后台任务另设执行账本、生命周期和恢复合同；原工具若仍叫“运行测试并返回结果”，却返回 accepted，会改变工具语义并误导模型。调用应明确表达“启动测试任务”。真正完成事件不能再次冒充同一个 call ID 的第二条 ToolMessage。

建议先用一个显式 `wait_events`/恢复入口，在 Context 没有新工作时挂起控制器；结果邮箱负责持久事件，唤醒器负责调度。仅把事件放进 inbox，不保证该 Context 会启动下一次推理。模型推理输入在调用开始时确定，后来到达的结果可在下个边界消费；若要中止旧推理，需取消并验证旧决策提交权已失效。

## 建议：提前反馈的正确性规则

- **决策提交权独立于任务归属。** 每 Context 设置递增决策 epoch；决策使用明确的输入事件水位和计划版本，提交时比较 epoch。旧推理可以结束，但不能以旧 epoch 发布新任务。
- **任务不必随每次计划递增全部报废。** 保存任务的输入版本、前提与相关依赖。计划改变时显式撤销受影响任务；仍兼容的新旧结果可以继续使用，避免只比较 `plan_revision` 就丢弃有用结果。
- **取消与重规划不是回滚。** 对写文件、发送消息等已发生副作用，记录真实结果并复核；不把“新计划已生成”当作旧执行未发生。冲突资源直到执行真正结束或清理确认后才释放。
- **依赖派发与语义纠偏分开。** B 的参数依赖 A 时，B 尚未就绪，A 完成后才能绑定参数并派发；B 已运行时，A 的结果不能直接修改它的参数。只能继续、取消后重提，或使用工具本身明确支持的交互协议。
- **副作用去重与事件去重分开。** 同一 event ID 重复投递不触发二次决策动作；同一执行身份参数冲突必须报错。进程崩溃且副作用状态未知时进入 unknown，不能承诺通用 exactly-once。
- **工具结果作为事实输入，不能提升授权。** 邮箱校验归属与来源；事件携带执行证据，不能凭结果正文赋予权限。跨 Context 共享只读任务时还须保存每个订阅者的可见性、输入版本和取消关系。

这些规则可先在完全本地的确定性控制器中测试，无须模型 API。对照必须保留“同批并发后等齐”模式，相同任务、资源上限和决策规则只改变结果消费时机；计入取消清理与后续修复耗时，避免把“停止等待慢任务”误记为“实际任务已经停止”。

## 尚未验证

- 没有运行完整 Focus 验证具体工具的并行安全、取消行为与 middleware 组合效果。
- 没有证明 Go 比 Python 调度更快；语言、IPC、进程复用应是可测变量，不作为先验收益。
- 没有证明提前消费一定减少真实 Agent 总耗时；规则控制器只能验证调度与因果链，无法替代模型决策质量、推理时间和额外推理成本的测量。
- 没有证明所有工具都支持安全取消。线程内同步函数、无法控制的远端操作和已发生写入需要单独合同。
- 还未定义 Run 结束后后台任务的 owner、租约和账本保存期限；接入 Focus 前必须明确继续、转移、取消或阻止 Run 结算中的哪一种语义。

首个实验建议只验证：快结果使确定性控制器改变方向，在相同并发上限下开始有效后续工作，撤销失效任务，并保留迟到/取消结果的真实记录。这个实验通过后再研究跨 Context 的统一资源调度。
