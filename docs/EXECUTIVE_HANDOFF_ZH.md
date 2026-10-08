# Bayesdesk V0.2：架构结论与后续工程 Agent 交接说明

> 当前仓库为**本地可测试原型**，而非部署到山西主机的生产服务。完整推导见 `DESIGN_RATIONALE.md`；可执行命令见 `OPERATIONS.md`；部署与改造顺序见 `ADAPTER_HANDOFF.md`。

## 一、我们真正要解决什么

原有工作流由人类在 ChatGPT 完成研究与方向决策，Claude Code 负责实现和长期实验。稀缺的 GTX1060 经常被一个实验占满，但同一台山西主机 CPU 仍可能有余量。长实验需要十几个小时，不应让大模型反复 `sleep` 和查看进度。多个 Worker 同时读一次 `nvidia-smi` 并自行决定启动会产生竞态；给整个主机加一把全局锁又会浪费可并发的 CPU 计算资源。现有的 SH 资源排队脚本已经证明“向统一窗口提交 Job”是可行方向，后续适配**必须先检查该脚本，不得盲目覆盖或丢弃旧队列**。

## 二、职责划分（不可由下游 Agent 随意更改）

| 角色 | 唯一职责 | 明确禁止 |
|---|---|---|
| 你 + ChatGPT | 科研方向、重大架构、人类审批、研究预算 | 将单次模型回复误当自动执行授权 |
| Engineer Leader | Task DAG、Worker 分配、租约、状态、例外升级 | 写业务代码、Debug、自己改变研究目标 |
| Worker Pool | 编码、测试、定位工程错误、选择已批准的资源档、记录假说和证据 | 直接抢占共享 GPU、凭空扩大资源预算、长期 sleep 轮询 |
| Node Resource Broker | 资源账本、原子准入、排队、执行、监控、日志/事件 | 解释自然语言、重写训练设计、执行 Worker 提交的任意 Shell |

正常路径是 `Worker → Broker → Event Inbox → 新 Worker`，只有架构级例外需要 `Worker → Leader → 人类`。

## 三、两个时钟、三个状态机

Task 可持续数天，Worker Session 可只运行数分钟，Node Job 可持续十几小时。**三者相互独立**。Worker 提交已持久化 Job 后退出，Broker 继续管理。Broker 接受后返回带签名的回执，Leader 才确认 Handoff；如果 ACK 丢失，按 `handoff_id` 查找，不创建新的 Job。Node Broker 在 `UNKNOWN` 时冻结预留，不能盲目恢复或重跑。

- Task：`READY → ASSIGNED → HANDOFF_PREPARED → WAITING_JOB → RESULT_READY → ASSIGNED → DONE`。
- Worker：`CLAIMED → ACTIVE → RELEASED`；分配租约的 `generation` 用于防止旧 Worker 回写。
- Job：`QUEUED → STARTING → RUNNING → SUCCEEDED/FAILED`；不能确认进程状态时 `UNKNOWN`。

## 四、多维资源调度的硬边界

本机容量账本采用 `cpu_units / memory_mib / gpu_count`；`quote` 仅是资源快照，`submit` 负责持久接单，`dispatch` 在 SQLite 写事务中再次校验并原子预留。GTX 1060 在 V0.2 按**整卡独占**处理；GPU 被占满仍可 Backfill CPU-only Job，只要 CPU/RAM 仍满足预算。Broker 不能因为资源紧张就自行修改 Batch Size、窗口、精度或负控制实验；只能执行 Worker 已经在 Task Contract 中获准的 Profile。CPUQuota 和 MemoryMax 不能代替 GPU 设备级隔离，必须由后续部署 Agent 加上真正的系统权限边界。

## 五、代码完成了什么

V0.2 增加：Task execution policy（Recipe/输入规则/资源 Profile/运行时长/尝试次数）、绑定准确输入和 Git SHA 的签名 Permit、Broker 签名 Receipt、丢失交接 ACK 的本地 Reconciler、类型化 Job/验收、Evidence SHA256 小文件存储与验收时复核、Lesson 的候选→独立复核→人工 VERIFIED、精确 Scope 检索，以及 Worker 环境白名单。本地 `coordinator --interval 5` 还可以在无人操作时修复交接 ACK 丢失、推动完成事件；Broker 验证入队时的 Recipe 指纹，并防止进程仍存活时提前释放资源。测试脚本使用独立的实际子进程和故障注入。

## 六、哪些事情明确留给后续 Agent

1. **优先**：读取山西现有 SH 队列，记录真实资源预留和业务，做无损兼容方案。
2. **部署硬边界**：用不同 Unix 用户、目录权限、受保护 Unix socket / API，把 Worker 与 Leader/Broker 的签名密钥、数据库、root/GPU 权限隔开。目前 CLI 的 `approved_by` 字符串不提供真正的身份认证。
3. **真实执行**：校准资源画像与系统预留，接入 systemd 和 GPU 权限限制，验证断电/进程异常的 UNKNOWN 恢复。
4. **远程通信**：日本 OCI ↔ 山西的事件 Outbox/Inbox 网络传输、重试和认证；当前 `bridge-pump` 只支持本地数据库。
5. **Worker CLI**：验证当前 Claude/Codex 版本的真实调用方式，提供短时 Worker Skill/RPC；禁止把模型 Session 留在 GPU 实验中等待。
6. **科研验收和组织记忆**：以固定类型的指标验证、数据和环境 Hash、Checkpoint、负控制、原始日志远程复制；将 ARTEX 因果/冷热图谱作为后续扩展，不应强塞进 Broker。

**重要：** `Job SUCCEEDED` 只能说明进程/机器判据通过，并不证明策略存在 Alpha，更不能自动修改研究计划。`VERIFIED` Lesson 也只在明示硬件/软件/任务范围内有效，日后出现相反证据需要支持撤销。

## 七、下游实施顺序和验收门槛

先 `python -m unittest discover -s tests -v`。然后获取山西 SH 脚本和生产状态，编制节点资源表，再实现安全 Worker RPC，部署无侵入式 Broker，验证 GPU/CPU 并行、Handoff 断线、事件去重、服务重启、进程隔离，最后接长实验。所有测试必须留下 Task ID、Job ID、Attempt ID、提交 Hash、数据/配置 Hash 和日志引用。**未证明安全和正确性前，不得让系统自动管理正在运行的真实 GPU 作业。**
