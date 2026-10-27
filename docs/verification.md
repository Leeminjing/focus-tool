# 完整架构本地验证

日期：2026-10-27。环境为 Windows amd64、Go 1.26.5、Python 3.12。原图全部模块已在独立仓库的本地闭环中运行，Focus 主仓接入与生产部署不包含在本次验证中。

复现命令：

```powershell
python scripts/verify.py --out results/local/verification.json
```

本次执行结果：

| 检查 | 结果 | 主要覆盖 |
|---|---|---|
| `go test ./... -count=1 -timeout=90s` | 通过 | M1 基线、共享队列、依赖、资源、取消、真实崩溃、日志恢复、输出与进程退出 |
| `go vet ./...` | 通过 | Go 静态检查 |
| Python unittest | 12 项通过 | AIMessage 形状适配、多 Context 归属、依赖绑定与整数精度、准入复核、异常、超时、Run 关闭、ack、背压与部分批次提交 |
| 完整架构演示 | 通过 | Context A 检查配置后重规划并取消真实扫描进程，Context B 独立消费结果并绑定后续 Python 参数，checkpoint 后 ack |
| 调度器重复测试 | 10 轮通过 | 轮转、取消与资源释放、恢复及背压的重复验证 |
| Linux amd64 交叉构建 | 通过 | Go stdio 服务与 Unix journal 锁编译 |
| M1 三模式回归 | 各 3 次通过 | 原有串行、并发等齐、事件驱动模式最终结果一致 |

Windows 本机未配置 cgo 编译器，因此没有将本地 `-race` 记为通过。已添加 Windows／Linux CI，Linux 执行 `go test -race ./...`；其实际结果以 GitHub Actions 为准。

原始记录：[完整验证输出](../results/verification-2026-10-27.json)、[完整闭环与 journal](../results/full-runtime-2026-10-27.json)、[M1 回归](../results/m1-regression-2026-10-27.json)。M1 回归期间同时进行了其他测试，耗时不作为新的受控性能对比结论。

## 验证的限制

工具均为可信的本地叶子进程，没有模型调用。验证覆盖各模块及它们之间的真实协议连接，但不等同于已验证 Focus 主仓的权限、租约、checkpoint 和执行账本事务。通用 Shell 进程树清理、journal 在线压缩、unknown 人工恢复工具及真实 Agent 效果测量仍需后续工作；详见 [运行边界](runtime.md#运行边界)。
