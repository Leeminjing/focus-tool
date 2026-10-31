# 开发预检记录：不计入正式性能对照

这两组原始文件从 `results/local/patrol-perf-development-1`／`-2` 原样复制，没有补写缺失字段、替换失败 trial 或与正式样本合并。它们来自基准开发中的不同代码快照，配置内保留当时 source SHA；早期 sample 尚无完整 cohort 元数据，不能交给最终分析器充当正式测量。

- `settled-round`：首个 barrier 成功，event 暴露已提交旧 Round 的 observation 被再次使用，出现 `Patrol observation 已被替换或提交`。后续加入 settled Round defer 和 observation／claim Round 一致性检查。
- `sql-connectivity`：前三个 trial 成功，第四个在 B bootstrap 连接临时 PostgreSQL 时超时。这一组执行期间仍有开发修改，不能作性能比较。此时尚未加入失败工作区归档和容器日志捕获；不能从现存文件判断连接超时的根因。

两组均未完成四臂 cohort，不报告中位数或提速。正式采集使用冻结源码、完整 warmup 和 40 个测量，另见 `../patrol-performance-2026-10-29/`。失败保留机制也已在正式代码中补齐，但这些旧文件没有被追补成更完整的证据。
