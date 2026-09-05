# 第 2 梯队 — 修改方案设计

> 对应 [REMEDIATION_PLAN.md](../REMEDIATION_PLAN.md) 第 2 梯队（2.1 节点自治、2.2 拆分控制面/数据面、
> 2.3 统一 wire 协议）。本文档是「怎么做」的具体设计，供实施前评审。
>
> 现状定位基于分支 `tier1-remediation` 的代码快照核实。

---

## 总览与实施顺序

第 2 梯队与前两梯队不同：**依赖严格、成本高、价值最高**。核心是把三件事从焊死的一个
`Cluster` 进程里拆开。

| 序 | 内容 | 依赖 | 成本 | 定位 |
|----|------|------|------|------|
| 2.1 | 节点自治（自 tick / 自 checkpoint / 事件直发） | 1.1 已完成 | 高 | **必须先做** |
| 2.2 | 拆分控制面/数据面（协调器独立进程 + 网关降级代理） | 2.1 | 很高 | 架构核心 |
| 2.3 | 统一 wire 协议 | 建议 2.2 之后 | 很高 | 压轴 |

> 2.3 独立于 2.1/2.2（可单独做），但架构一变协议又得改一次，故排在最后。本文档聚焦 2.1/2.2，
> 2.3 只给方向（见末节）。

---

## 现状：`Cluster` 五循环职责混乱（已核实）

| 循环 | 周期 | 现在做什么 | 归属 |
|------|------|-----------|------|
| `backgroundLoop` | 700ms | 远程驱动每个节点 `BackgroundStep()` | 该归**节点** |
| `checkpointLoop` | 700ms | 抓 owner 快照→落盘→推副本 | 该归**节点** |
| `heartbeatLoop` | 1s | ping 节点、判健康、`handleNodeFailure` | **控制面** |
| `discoveryLoop` | 1s | 发现节点、建路由拓扑 | **控制面** |
| `mapCacheLoop` | 150ms | 拉快照缓存喂 `SnapshotFor` | **数据面/网关** |

矛盾本质：`backgroundLoop`/`checkpointLoop` 是「协调器推动节点」的产物——节点不自己动，世界才靠
协调器远程踹一脚。真实游戏服务器相反：场景服务器自己 tick，协调器只路由。

---

## 2.1 节点自治

### 目标

把「世界模拟 + checkpoint 落盘」的驱动权还给节点。协调器不再远程驱动世界。

### 改动（节点侧）

1. `NodeService.Start()` 除 `flushLoop` 外，新增 `tickLoop`（~700ms），对每张主地图调
   `world.BackgroundStep()`。
2. tick 产生的事件**节点自己发布到 Redis Pub/Sub**（复用 `PublishEvent`），不再回传给协调器。
   事件流变为：
   ```
   节点 tickLoop → world.BackgroundStep() → 节点 PublishEvent → 网关 eventLoop 订阅 → 缓存 → SnapshotFor
   ```
3. 主地图**自落盘**：tickLoop 内定期 `CaptureCheckpoint` 写 Redis（对称 1.1 的自拉恢复）。
4. 副本**自拉**（副本同步方案 iii，见下）：新增 `replicaSyncLoop`，对副本地图定期
   `LoadCheckpoint` 更新 `replicaSnapshots`。副本地图列表由 `-replicas` 启动参数传入（main.go 调
   `AddReplicaMap`），节点**本就知道自己托管哪些副本**，无需读协调器拓扑。

### 改动（协调器侧）

5. 删 `backgroundLoop`、`checkpointLoop`。
6. `NodeClient` 接口删 `BackgroundStep()`、`StoreReplica()`；`grpc_client.go` 同步删这两个实现。
7. 删 `discoveryLoop` 里的 `client.InstallPrimaryMap` / `client.RestorePrimaryMap` 空转桩调用
   （[cluster.go:730-740](../src/cluster/cluster.go#L730)）及接口方法——1.1 节点已自拉，此 push 路径
   冗余。
8. 删 `Close`/`failNode`/`recoverNode` 里的 `StoreReplica` 调用（副本已自拉，不再需要协调器推）。

> `Promote`（副本提升为主）**保留**——它是协调器故障转移的决策动作，仍由协调器在 `handleNodeFailure`
> 中调用，不在副本同步（日常数据流）范畴。

### 副本同步方案（已决定：方案 iii「副本拉」）

对标 Redis/MySQL 主从的「从库拉取」模式，**让副本节点自己从 Redis 拉快照**，而非主推或协调器中转：

```
主节点 tickLoop ──CaptureCheckpoint──> Redis (checkpoints hash)
                                          ▲
                                          │ LoadCheckpoint (副本自拉)
副本节点 replicaSyncLoop ─────────────────┘
```

- 主节点只需「自落盘」，**无需知道副本是谁**（解耦拓扑发现，这正是 Redis PSYNC / MySQL binlog 的思路）。
- 副本节点用 `-replicas` 启动参数自知的副本地图列表，定期 `LoadCheckpoint` 更新 `replicaSnapshots`。
- 协调器 `checkpointLoop` 整个删除，不再中转；`StoreReplica` RPC 的 Go 侧调用/实现删除。
- `StoreReplica` 的 **proto 定义 + grpc_server handler 留到 2.3 统一协议时清理**（避免 2.1 动 proto 重新生成）。
- 副本新鲜度从「协调器 700ms 推」变为「副本自拉」，有 1~2 个周期延迟窗口——异步复制固有，可接受。
- `Promote` 保留：副本自拉的快照就是 Promote 时 `RestoreCheckpoint` 的输入，链路自洽。

---

## 2.2 拆分控制面/数据面（路线 Y：真拆两进程）

### 目标形态

```
协调器（独立进程 cmd/coordinator）
   ├─ discoveryLoop   —— 发现节点
   ├─ heartbeatLoop   —— 心跳 + 故障转移
   ├─ 拓扑维护         —— owners/replicas 决策，写 Redis
   ├─ leader 选举      —— Elector 接口
   └─ boss 全局状态初始化

网关（改 cmd/server）
   ├─ 终端 client TCP + 认证 + 转发
   ├─ 读 Redis 拓扑 + 建 NodeClient 连接路由
   ├─ mapCacheLoop（软无状态，方案 A）
   ├─ 事件订阅 eventLoop
   └─ 业务方法 Login/Move/Attack/SnapshotFor…
```

### 核心：拓扑交换机制

协调器是「决策者」，网关是「路由执行者」。两者通过 Redis 交换拓扑：

1. 新增存储方法：`SaveTopology(owners, replicas map[string]string)` / `LoadTopology()`。协调器把
   `owners`/`replicas` 决策写进 Redis（`topology:owners`、`topology:replicas` 两个 hash）。
2. 网关轮询读拓扑 + 读节点注册表（`GetActiveNodes` 已有），自行建立 NodeClient 连接并路由玩家。
3. 拓扑变更（故障转移）后，网关通过轮询（几百 ms）感知，对 demo 足够；如需实时可后续加 Pub/Sub 通知。

### leader 选举：先抽 `Elector` 接口

**为什么选举**：协调器是「决策者」（判生死、切拓扑），一旦多副本就脑裂。单实例无脑裂但自身单点。
所以选举 = 「多副本里谁有资格当唯一决策者」。

**做法**：抽 `Elector` 接口隔离实现，协调器只依赖接口：

```go
type Elector interface {
    Campaign(ctx context.Context) (leaderCtx context.Context, err error) // 阻塞直到成为 leader
    IsLeader() bool
    Resign()
}
```

分两档推进：
- **先 `AlwaysLeader` 实现**：单实例协调器，`Campaign` 立即返回。先把「拆进程」跑通，不被选举卡住。
- **再 `RedisElector` 实现**（可选加分）：`SetNX` + 租约续期。能讲「租约/续期/脑裂防护」。

**Redis → etcd 迁移负担**：因接口隔离，换 etcd 就是新增一个 `EtcdElector` 实现，业务代码零改动。
差异在于一致性强度（Redis 锁有 GC/续期窗口、可能短暂脑裂；etcd Raft 严格单 leader），demo 用 Redis 足够。

### 决策 #2（已定）：`NodeClient` 接口按进程拆分

拆进程后，协调器和网关对节点的 RPC 需求不同：
- 协调器：`Ping`/`View`/`Checkpoint`/`Promote`/`SetHealthy`/`IsHealthy`/`Start`/`Stop`
- 网关：`AddPlayer`/`RemovePlayer`/`MovePlayer`/`Attack`/`Heal`/`BuyItem`/`AttackBoss`/`Profile`/`RewardPlayer`/`Snapshot`

**决定**：拆成两个接口 `CoordinatorNodeClient` / `GatewayNodeClient`。`NodeGRPCClient` 同时实现两者
（方法已在，无额外成本）。放在 2.2 后半段细化，不阻塞进程拆分主线。

### 决策 #3（已定）：boss 状态归属

**决定**：boss 是全局状态，初始化 + 生命周期（复活倒计时 `respawnBossAfterCooldown`）归**协调器**；
网关 `SnapshotFor` 从 Redis 读（`LoadGlobalBoss`），**只读不写**。

---

## 涉及文件清单（2.1 + 2.2）

| 文件 | 改动 |
|------|------|
| [src/node/node.go](../src/node/node.go) | 2.1 tickLoop、自 checkpoint、事件直发、副本自拉 `replicaSyncLoop`、`AddReplicaMap` |
| [src/cmd/node/main.go](../src/cmd/node/main.go) | 2.1 副本地图列表传入 `AddReplicaMap` |
| [src/cluster/cluster.go](../src/cluster/cluster.go) | 2.1 删两循环；2.2 拆 `Cluster` → `Coordinator` + `Gateway` |
| [src/cluster/grpc_client.go](../src/cluster/grpc_client.go) | 2.1 删空转桩/StoreReplica；2.2 接口拆分（可选） |
| [src/storage/store.go](../src/storage/store.go) | 2.2 拓扑读写方法 |
| [src/coordinator/](../src/coordinator/)（新增） | 2.2 协调器独立进程 |
| [src/cmd/coordinator/](../src/cmd/coordinator/)（新增） | 2.2 协调器 main |
| [src/cmd/server/main.go](../src/cmd/server/main.go) | 2.2 网关降级为代理 |
| [src/elector/](../src/elector/)（新增） | 2.2 Elector 接口 + AlwaysLeader/Redis 实现 |

---

## 2.3 统一 wire 协议（方向）

现状（DESIGN_REVIEW §2.1）：protobuf / JSON / goccy-go-json 三套并存，gRPC 里套 JSON 语义，
`adapter.go` ~430 行手写转换已失同步。

方向二选一（**留待 2.2 之后独立设计**）：
- **A 真 protobuf**：把 `Message` 的扁平 string 字段包拆成强类型 oneof，去掉 `WorldState` 里的 JSON 嵌套。
- **B 丢 protobuf 端到端 JSON**：gRPC 只当传输层，`bytes` 承载 JSON。

> 不在此文档展开。此项需单独一份设计 + 约 430 行 adapter 重写，是压轴工程。

---

## 建议的提交切分（可选）

1. `feat: 节点自治——自 tick + 事件直发 + 自 checkpoint`（2.1）
2. `refactor: 协调器独立进程 + 网关降级代理`（2.2 进程拆分主线）
3. `feat: Elector 接口 + Redis 锁 leader 选举`（2.2 加分项）
4. `refactor: 统一 wire 协议`（2.3，独立）

每步独立可编译、可回滚。
