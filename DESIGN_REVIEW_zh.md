# Battleworld — 设计复盘与已知弱点（中文版）

面试准备用的内部复盘笔记。该项目有一个**分层合理的骨架**（网关 / 协调器 / 节点 / 存储，冷热分离，主备故障转移），
但其中不少决策要么尚未完成，要么在真实生产环境站不住脚。本文是对这些弱点的诚实、结构化梳理，
按「海拔高度」分组，按「严重程度」排序。

> 说明：本文为 [DESIGN_REVIEW.md](DESIGN_REVIEW.md) 的中文翻译，内容一一对应。
> 配套的「按什么顺序修」见 [REMEDIATION_PLAN.md](REMEDIATION_PLAN.md)。

---

## 严重程度图例

| 标记 | 含义 |
|------|------|
| 🔴 | 架构层面或正确性风险 |
| 🟠 | 值得重新审视的设计决策 |
| 🟡 | 细节 / 打磨 / 技术债 |

---

## 1. 架构

### 1.1 🔴 网关是控制面 + 数据面 + 模拟循环三合一的单点

`cluster.Cluster` 在**网关进程内部**启动，并跑着五个后台循环
（`src/cluster/cluster.go` 中的 `Cluster.Start()`）：

- `discoveryLoop` — 节点发现
- `heartbeatLoop` — 故障检测
- `mapCacheLoop` — 快照缓存
- `checkpointLoop` — 副本复制
- `backgroundLoop` — 世界模拟

网关崩溃的后果远比「玩家掉线」严重：

1. **世界冻结** —— `backgroundLoop` 正是通过 RPC 调用每个节点的 `BackgroundStep()` 来驱动 NPC 移动/攻击/刷怪。
2. **故障检测停止** —— 没有 `heartbeatLoop`，死掉的节点永远不会被故障转移。
3. **副本复制停止** —— 没有 `checkpointLoop`，副本会越来越陈旧。

**修复：** 把协调器拆成独立进程，并用 leader 选举（Redis 锁 / etcd），让网关降级为无状态的连接代理。与 §1.2 是耦合问题。

### 1.2 🔴 节点不自治 —— 世界依赖协调器来「滴答」

`node.NodeService.Start()` 只启动了 `flushLoop`（把热数据刷到 Redis）。节点**没有自己的模拟 tick 循环**
—— NPC 移动、战斗、刷怪全部由协调器的 `backgroundLoop` 每 700ms 远程驱动。

这与真实游戏服务器的做法完全相反：应该是每个场景服务器自己 tick，协调器只负责路由。在当前设计下，
一个失去协调器的节点无法让世界继续运转。

**修复：** 把 `BackgroundStep()` 移进节点自己的 tick 循环；让协调器只做事件聚合。

### 1.3 🟠 `NodeClient` 接口泄漏了「本地进程内」的假设

`NodeClient`（`src/cluster/cluster.go`）本意是对节点客户端的抽象，但它唯一的实现 `NodeGRPCClient`
包含**空操作桩**：

```go
func (c *NodeGRPCClient) Start() error                     { return nil }
func (c *NodeGRPCClient) InstallPrimaryMap(cfg world.MapConfig) {}
func (c *NodeGRPCClient) RestorePrimaryMap(cfg world.MapConfig, cp protocol.MapCheckpoint) {}
```

结果就是，`discoveryLoop` 里「节点发现时从 checkpoint 恢复」的路径是一个静默空操作
—— 新发现的节点实际上从来没有真正从 Redis 恢复过。当实现从进程内切换到网络时，这个接口没有跟着演进。

**修复：** 要么把这些变成真正的 RPC，要么把它们从接口里删掉。

### 1.4 🟠 静态 1:1 主备，无动态分片

主/备拓扑通过启动参数（`-maps`、`-replicas`）硬编码。没有动态再平衡，也没有基于玩家负载的迁移。
规模一大，热点不可避免。demo 可以接受，但不算生产级 HA。

---

## 2. 技术决策

### 2.1 🔴 三种序列化方案并存；gRPC 承载的是 JSON 语义

代码库同时使用三种编解码：

| 通道 | 编码 |
|------|------|
| 节点 ↔ 网关 gRPC | protobuf（`src/pb/battle.proto`） |
| 网关 ↔ 终端客户端 | JSON（`src/protocol/message.go` 的 `Conn.Send`） |
| Redis 热数据 | `goccy/go-json`（`src/storage/store.go`） |

更糟的是，`GameStream` 双向流里的 `Message` 是一个「扁平的 string 字段包」外加一个嵌套的 `WorldState`
—— 语义上就是「gRPC 里套 JSON」。它既没拿到 protobuf 的强类型，也没拿到紧凑的二进制表示，
还让 `.proto` 定义近乎摆设。

代价是实打实的：`src/protocol/adapter.go` 有 **约 430 行机械化的 `ToProtoXxx` / `FromProtoXxx` 手写转换**，
而且这些转换已经在彼此之间慢慢失同步了。

**修复：** 选定一种线上协议。要么定义真正的强类型 gRPC 消息，要么丢掉 protobuf、端到端全用 JSON。
这种混合是「两边都不讨好」。

### 2.2 🔴 安全：无盐 SHA-256、硬编码凭据、明文 gRPC

- 密码用**裸 SHA-256、无盐**（`src/storage/store.go` 的 `hashPassword`）哈希 —— 极易被彩虹表攻击；
  生产环境必须用 bcrypt/argon2。
- 数据库地址和密码**硬编码在源码里**（`storeAddr`、`NewStore` 里的 DSN 密码），并且躺在 git 历史中。
- 节点间 gRPC 使用**明文**（`src/cluster/grpc_client.go` 里的 `insecure.NewCredentials()`）。

### 2.3 🟠 Redis `KEYS` 用于节点发现

`GetActiveNodes` 用 `rdb.Keys("battle:node:registry:*")` 扫描。`KEYS` 会阻塞 Redis，规模一大就是生产事故。
应改用 `SCAN`，或者维护一个索引集合。

### 2.4 🟠 半成品事件系统：纯内存、无重放

两条事件路径并存且被混为一谈：

- `pushEvent` / `broadcastGlobalEvent` 发布到 Redis Pub/Sub。
- `eventLoop` 订阅后把事件缓冲进**进程内内存**（`globalEvents`、`mapEvents`、`userEvents`），
  再由 `SnapshotFor` 读取。

所以事件是进程本地状态：网关一重启就丢，而且没有重放。这些字段上的 `// to understand` 注释
也暗示作者当时对此也不确定。

---

## 3. 细节

### 3.1 🟡 版本字段没有一致语义，实际上形同虚设

`SessionVersion` / `Version` 在不同地方含义不同：

- `Login` 硬编码 `session.Version = 1`。
- `SwitchMap` 会递增它。
- `HotSession.SessionVersion` 硬编码为 `0`，还配了句注释说「不需要维护」。
- `MapView.Version`、`MapCheckpoint.Version`、`BossState.Version` 是彼此无关的计数器。

没有一个被用于冲突/一致性检测 —— 它们是没有消费者的计数器。这是一个「`Version` 是干嘛用的？」
的陷阱题，值得给出一个自信的回答。

### 3.2 🟡 `WorldState` 对象池泄漏、且重置不彻底

`SnapshotFor` 从池里分配（`protocol.AllocWorldState()`），但好几处提前 `return error` 的路径
没有 `FreeWorldState`。另外，`FreeWorldState` 重置了切片，但**没有重置 `Self` / `Map` 的标量字段**，
所以复用的对象可能携带陈旧数据。

### 3.3 🟡 代码里残留着教学痕迹

- `studentTodoNotice sync.Map` 全局变量。
- `logStudentTODO` / `studentTODOError` 调用点。
- `// to understand`、`// TODO(Labc.mu.Unlock3-2)` 这类半成品注释。
- 大量 `fmt.Println("[debug]...")` 语句。

这些读起来像作业残留，展示仓库前应该清掉。

### 3.4 🟡 错误被静默吞掉

- `checkpointLoop`：`CP, _ := owner.Checkpoint(...)` —— checkpoint 失败会静默回退到故障转移时的陈旧快照。
- `handleNodeFailure` 忽略了 `Promote` 的错误 —— 如果副本从未收到那张地图的快照，提升会失败，地图会暂时无人接管。
- 大量 `_ = c.store.SaveHotSession(...)`、`_ = conn.WriteJSON(...)` 没有错误处理。

---

## 4. 优先级矩阵

| 严重度 | 问题 | 层次 |
|--------|------|------|
| 🔴 | 网关 = 控制 + 数据 + 模拟单点 | 架构 |
| 🔴 | 节点不自治；世界被远程 tick | 架构 |
| 🔴 | 三种编解码；gRPC 里套 JSON 语义 | 决策 |
| 🔴 | 无盐 SHA-256、硬编码凭据、明文 gRPC | 安全 |
| 🟠 | `NodeClient` 空操作桩 / 抽象泄漏 | 决策 |
| 🟠 | Redis `KEYS` 用于发现 | 决策 |
| 🟠 | 内存事件、无重放 | 决策 |
| 🟠 | 静态 1:1 主备 | 架构 |
| 🟡 | 版本字段语义形同虚设 | 细节 |
| 🟡 | `WorldState` 池泄漏 / 重置不彻底 | 细节 |
| 🟡 | 教学残留 | 细节 |
| 🟡 | 错误被吞掉 | 细节 |

---

## 建议的修复顺序

1. 把控制面从数据面拆出来（leader 选举的协调器；网关变代理）。
2. 让节点自 tick；去掉协调器驱动的模拟。
3. 把线上协议统一成一种编解码。
4. 用 bcrypt/argon2 替换 SHA-256；凭据移入 env/config；启用 TLS。
5. 用 `SCAN` 替换 `KEYS`；增加会话 TTL/租约 + 接管。
6. 清理教学残留和被吞掉的错误。
