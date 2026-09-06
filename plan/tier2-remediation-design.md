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

### 核心：拓扑交换机制（已根据 Lab4 对照更新）

协调器是唯一的「拓扑决策者」，网关是「路由执行者」，节点是「地图状态执行者」。三者通过 Redis
交换**版本化的单一拓扑快照**，而不是分别读写多个 owner/replica Hash：

```go
type Topology struct {
    Version    uint64            // 每次拓扑提交递增，供网关和节点发现变更
    LeaderTerm uint64            // 当前协调器任期，拒绝旧 Leader 覆盖新决策
    Owners     map[string]string // mapID -> 主节点 ID
    Replicas   map[string]string // mapID -> 副本节点 ID
    MapEpochs  map[string]uint64 // mapID -> 主权 fencing token
    UpdatedAt  time.Time
}
```

1. 新增 `SaveTopology(Topology)` / `LoadTopology()`：协调器把完整对象原子写入一个 Redis key（建议
   `battle:topology`）。单 key 可避免网关读到「新 owner + 旧 replica」的撕裂视图。
2. 节点注册表只表达**节点地址、健康租约、承载能力和 drain 状态**；`-maps` / `-replicas` 不再是
   节点自授的主从所有权。只有协调器写入的 `Topology` 才有路由权威性。
3. 网关订阅 `events:topology` 立即刷新；同时保留几百毫秒轮询 `LoadTopology()`，防止 Pub/Sub 漏消息。
   网关只按已发布 `Topology.Owners` 建立/复用 `GatewayNodeClient` 并路由玩家。
4. 每次主节点切换必须递增对应 `MapEpochs[mapID]`。节点在执行 tick、接收地图写操作、保存 checkpoint 前
   校验自己仍是该地图当前 epoch 的 owner；失去主权的旧主立即拒绝写入。这是防网络分区双主的 fencing，
   比「只靠协调器选主」更关键。
5. 副本提升的提交顺序固定为：**验证副本 checkpoint → Promote 成功 → 带新 epoch 写 Topology → 更新
   GlobalSession → 发布 topology 事件**。任一步失败不能把网关路由到未就绪的新主。

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
- **先 `AlwaysLeader` 实现**：单实例协调器，`Campaign` 立即返回。先把「拆进程 + 拓扑读写」跑通，
  不被选举卡住。
- **再 `RedisElector` 实现**：每个实例生成唯一 token，用 `SET key token NX PX ttl` 抢租约；续租和
  释放均用 Lua 做「value 仍等于我的 token」的 compare-and-renew / compare-and-delete；成功当选时递增
  `LeaderTerm`。仅 Leader 运行 discovery、heartbeat、failover 与 Boss 生命周期。

Lab4 的 `cluster/leader.go` / `cluster/redis_leader.go` 提供了「TTL 租约、定时续约、仅 Leader 服务」的
良好结构参考；但其 Redis 版本不能原样照搬：读锁后直接 `SETEX` 续租无法阻止过期旧 Leader 覆盖新 Leader。
本项目以 token + Lua + `LeaderTerm` 作为最低实现标准。

**Redis → etcd 迁移负担**：因接口隔离，换 etcd 就是新增一个 `EtcdElector` 实现，业务代码零改动。
差异在于一致性强度（Redis 锁仍有 GC/续期窗口，MapEpoch fencing 是第二道保护；etcd Raft 可提供更强
单 Leader 语义），demo 阶段 Redis 足够。

### 决策 #2（已定）：`NodeClient` 接口按进程拆分

拆进程后，协调器和网关对节点的 RPC 需求不同：
- 协调器：`Ping`/`View`/`Checkpoint`/`Promote`/`SetHealthy`/`IsHealthy`/`Start`/`Stop`
- 网关：`AddPlayer`/`RemovePlayer`/`MovePlayer`/`Attack`/`Heal`/`BuyItem`/`AttackBoss`/`Profile`/`RewardPlayer`/`Snapshot`

**决定**：拆成两个接口 `CoordinatorNodeClient` / `GatewayNodeClient`。`NodeGRPCClient` 同时实现两者
（方法已在，无额外成本）。放在 2.2 后半段细化，不阻塞进程拆分主线。

### 决策 #3（已定）：boss 状态归属

**决定**：boss 是全局状态，初始化 + 生命周期（复活倒计时 `respawnBossAfterCooldown`）归**协调器**；
网关 `SnapshotFor` 从 Redis 读（`LoadGlobalBoss`），**只读不写**。

### Lab4 对照结论（沉淀）

参考仓库：<https://gitee.com/hnu-cloudcomputing/cloud-compute-book-code/tree/master/Lab/Lab4>。
Lab4 与本项目同属 battleworld 演进代码，其设计可作为边界与流程参考，但不是直接复制的目标实现。

| Lab4 模块 | 可借鉴内容 | 本项目决策 |
|---|---|---|
| `cmd/cloud-coordinator` / `cmd/cloud-gateway` / `cmd/cloud-map` | 协调器、网关、地图服务独立进程 | **采纳进程边界**；但不让协调器承载 Move/Attack 等高频数据面请求，网关应按拓扑直连节点。 |
| `cloud/mapservice.go` | 单地图服务自驱动、独立快照与 HTTP/RPC 边界 | **已验证 2.1 方向**：现有 Node tick、自 checkpoint、副本自拉保留。 |
| `cluster/leader.go` / `redis_leader.go` | 租约选主、Leader 状态、非 Leader 不作决策 | **采纳抽象**，但 Redis 实现补 token + Lua + term，且增加每地图 epoch fencing。 |
| `cluster/http_lifecycle.go` / `pod_lifecycle.go` | `/healthz`、`/readyz`、`/drain` 与主动下线 | **采纳语义**，不依赖 Kubernetes；节点 drain 后禁止新路由、保存最终 checkpoint、再退出。 |
| `twopc/twopc.go` | `Prepare/Commit/Abort`、事务 ID 幂等、Prepare 失败时 Abort | **借鉴状态机**：跨地图迁移升级现有 TCC，不把经典 2PC 套到故障转移。 |
| `cloudapi/types.go` | 控制面与地图服务的显式请求/响应契约 | **采纳接口分层**：拆 `CoordinatorNodeClient` / `GatewayNodeClient`；传输协议仍在 2.3 决策。 |

明确不采纳的部分：Lab4 将大量玩家业务聚合到 Coordinator，不符合本项目网关直连节点的数据面目标；
其 Kubernetes API、Pod 删除优先级等实现不作为当前裸 Redis 部署的前置条件；其 Redis 选主续租实现不满足
本项目的 fencing 要求。

### 2.2 分阶段实施、决策点与验收

| 阶段 | 主要改动 | 本阶段必须做出的决策 | 验收标准 |
|---|---|---|---|
| 2.2-A：拓扑基础 | 新增 `Topology`、`SaveTopology/LoadTopology`、拓扑事件、节点 `draining` 字段 | 拓扑采用**单一版本化对象**；节点注册不等于主权声明 | Redis 中只存在一个可原子读取的拓扑版本；网关能在轮询/事件后刷新路由。 |
| 2.2-B：先拆进程 | 新建 `cmd/coordinator`，抽出 discovery、heartbeat、failover、Boss 生命周期；先接 `AlwaysLeader` | 协调器只做控制面，网关保留认证、会话、缓存和玩家业务转发 | 关闭网关后，节点 tick 仍运行；重启网关能从 Redis 拓扑恢复并继续路由。 |
| 2.2-C：故障切换加固 | Promote → 写新 epoch Topology → 更新会话 → 通知网关；节点检查主权 epoch | `MapEpoch` 为旧主 fencing 的权威 token；Promote 失败不改路由 | kill 主节点后，副本仅提升一次；旧主恢复后不能继续 tick/写入；在线会话最终指向新主。 |
| 2.2-D：协调器高可用 | `RedisElector`、token/Lua 续租与释放、`LeaderTerm`；Leader-only 循环 | Redis 租约作为 demo 方案，etcd 仅保留替换接口 | 两协调器并发运行时仅一者执行故障转移；Leader 切换后旧 term 不能覆盖新拓扑。 |
| 2.2-E：优雅下线 | 节点/协调器的 health、ready、drain；最终 checkpoint 和路由摘除 | drain 是主动下线，不等同于 ping 超时故障 | drain 节点不接新玩家；完成最终 checkpoint 后退出且不触发重复提升。 |
| 2.3：协议治理 | 独立设计并替换手写 adapter | 真 Protobuf `oneof` 或端到端 JSON 二选一 | 2.2 稳定后再启动；删除或显著收敛 `adapter.go` 的重复转换。 |

### 尚待确认的决策（实施前评审）

1. **网关会话归属**：推荐继续把 `GlobalSession` 放 Redis，网关本地缓存仅作加速；多网关场景不得以
   `sync.Map` 作为权威状态。
2. **节点主权校验位置**：推荐由节点在每次地图写操作与 tick 前本地校验已缓存的 `Topology + MapEpoch`，
   并定期/事件驱动刷新；避免为 2.2 提前大改全部 gRPC 请求结构。
3. **跨地图切换**：保留 TCC 语义，但引入 `transferID`、目标/源的 Prepare 状态、幂等 Commit/Abort 以及
   Redis 超时清理；不直接改成需要失联旧主参与的经典 2PC。
4. **checkpoint 策略**：当前 700ms 全量落盘可先保留；建议在 2.2-E 或之后改为 `dirty` 标记 + 周期合并
   + drain 强制落盘，降低无变更写入而不改变 RPO。
5. **协议路线**：在 2.2 完成后单独评审。若保留 gRPC，倾向强类型 Protobuf `oneof`；若优先浏览器兼容和
   迭代速度，评估端到端 JSON。两种路线不可混做。

---

## 涉及文件清单（2.1 + 2.2）

| 文件 | 改动 |
|------|------|
| [src/node/node.go](../src/node/node.go) | 2.1 已完成 tickLoop、自 checkpoint、事件直发、副本自拉；2.2 增加 Topology/MapEpoch 主权校验与 drain |
| [src/cmd/node/main.go](../src/cmd/node/main.go) | 2.1 已完成副本地图 `AddReplicaMap`；2.2 增加节点就绪与优雅退出流程 |
| [src/cluster/cluster.go](../src/cluster/cluster.go) | 拆分：保留网关侧路由/缓存/事件，控制面移动到 Coordinator |
| [src/cluster/grpc_client.go](../src/cluster/grpc_client.go) | 拆成 `CoordinatorNodeClient` / `GatewayNodeClient`；连接按拓扑版本刷新 |
| [src/storage/store.go](../src/storage/store.go) | 版本化 Topology、LeaderTerm、节点 drain/注册租约、迁移事务记录 |
| [src/coordinator/](../src/coordinator/)（新增） | 控制面：发现、心跳、故障转移、Boss 生命周期、拓扑提交 |
| [src/cmd/coordinator/](../src/cmd/coordinator/)（新增） | 协调器独立 main 与 health/ready/drain 管理端点 |
| [src/cmd/server/main.go](../src/cmd/server/main.go) | 网关加载拓扑、直连节点；不再启动协调器循环 |
| [src/elector/](../src/elector/)（新增） | `Elector`、`AlwaysLeader`、带 token/Lua 的 `RedisElector` |
| [src/protocol/](../src/protocol/) | 2.2 仅补必要拓扑/管理视图；2.3 再统一 wire 协议 |

---

## 2.3 统一 wire 协议（方向）

现状（DESIGN_REVIEW §2.1）：protobuf / JSON / goccy-go-json 三套并存，gRPC 里套 JSON 语义，
`adapter.go` ~430 行手写转换已失同步。

方向二选一（**留待 2.2 之后独立设计**）：
- **A 真 protobuf**：把 `Message` 的扁平 string 字段包拆成强类型 oneof，去掉 `WorldState` 里的 JSON 嵌套。
- **B 丢 protobuf 端到端 JSON**：gRPC 只当传输层，`bytes` 承载 JSON。

> 不在此文档展开。此项需单独一份设计 + 约 430 行 adapter 重写，是压轴工程。

---

## 后续增强：动态地图副本组与主权调度（第 3 梯队，不阻塞 2.2）

### 结论

**不取消副本节点，也不做同地图多主写入。** 对实时地图/房间这类强顺序状态，采用
「单权威写者（single writer）+ warm standby + 可恢复 checkpoint」是更通用的业界模式：

```text
一张逻辑地图 / 一个 shard
  ├─ Owner（唯一主）：接收写请求、执行 tick、发布事件、保存 checkpoint
  ├─ 0..N 个 Replica：拉取 snapshot / 增量，仅作为候选主
  └─ MapEpoch：Owner 唯一主权凭证，防旧主与网络分区双写
```

扩展同一地图的**吞吐**依靠地图拆 shard、zone、房间或世界线；增加同地图副本只提升可用性和
恢复速度，不能让同一地图的写吞吐线性增加。

### 与当前静态主备、Lab4 的关系

| 模式 | 主权来源 | 副本职责 | 故障后接管 | 适用性 |
|---|---|---|---|---|
| 当前实现 | `-maps/-replicas` 启动参数 + Cluster 内存 | Redis 自拉 checkpoint | 协调器对固定副本调用 `Promote` | 教学演示可用，但旧主重启可能重新声明 owner |
| Lab4 | 同地图 Deployment 的 lease Leader | 非 Leader 恢复 checkpoint，必要时代理到 Leader | 租约到期后候选 Pod 竞选 Leader | K8s 下的地图副本组；本质仍是 active-passive，不是多主 |
| 目标实现 | 协调器 Leader 提交的版本化 `Topology` + `MapEpoch` | 0..N 个 warm standby，自拉状态、等待授权 | 协调器选择候选、Promote、递增 epoch、原子提交拓扑 | 裸进程与 K8s 都适用，且可防旧主/分区双写 |

Lab4 的 `cmd/cloud-map` 已体现这个本质：Leader 才运行 `BackgroundStep` 和保存 checkpoint，非 Leader
周期性恢复 checkpoint。因此可借鉴其「每地图副本组、leader lease、health/ready/drain」的运维模型，
但不采用其「网关 → 协调器 → 地图」承载全部高频业务，以及 follower 反向代理请求的路径；本项目网关应
根据 `Topology.Owners` 直连当前主节点。

### 与 2.2 的边界（重要）

动态副本组是**较大重构**，因为它改变“谁拥有地图主权”的来源。它不应与 2.2-A～E 绑成一个巨型提交。

- **2.2 必须完成的最小闭环**：静态的一主一备可以暂时保留为初始部署配置，但唯一权威主权改为
  `Topology + MapEpoch`；节点不得因旧 `-maps` 参数自动抢回 owner。
- **第 3 梯队再做的能力**：节点只上报可用性、容量、drain 状态和可承载地图能力；协调器自动选择
  Owner/Replica、补齐副本数、在节点加入/退出时重平衡。
- **不做的能力**：同一地图 active-active 写入。它需要操作全序、状态合并或共识，成本高且不符合当前
  `world.World` 的单进程权威状态模型。

### 第 3 梯队实施分段

| 阶段 | 改动 | 关键决策 | 验收 |
|---|---|---|---|
| 3.3-A：副本组模型 | `Topology` 支持 `mapID -> {owner, replicas[]}`；节点注册增加 capacity/capability/draining | 副本数量按地图 SLA 配置，默认仍为 1 主 + 1 备 | 启动参数仅表示候选能力；重启旧主不会覆盖当前 owner。 |
| 3.3-B：调度与补副本 | 协调器根据健康、容量、反亲和策略选择副本；节点离开后自动补齐 | 先用简单确定性策略（最低负载 + 不与 Owner 同节点），不做复杂调度器 | 新增节点可被选为副本；副本故障后可自动补到目标数量。 |
| 3.3-C：提升与回归 | 副本提升走 `Promote -> 新 MapEpoch -> Topology -> 会话路由`；旧主恢复为候选副本 | 旧主永不自动回抢；回归后先同步、后由调度器决定角色 | 主节点故障时仅有一个副本被提升；旧主恢复后无法 tick/写入。 |
| 3.3-D：K8s 可选适配 | 一个地图 shard 对应一个 Deployment/Stateful 工作负载；使用 Lease、readiness、drain | K8s Lease 优先于 Redis lease；保持 `Elector` 接口以支持两种环境 | 滚动升级时先 drain，玩家不被路由到即将退出的 Pod。 |
| 3.4：动态分片/负载迁移 | 将热点地图拆 zone/room/shard，并支持迁移 | 只有确认单 shard 已成瓶颈后才实施 | 新 shard 可独立扩容；单地图副本增加不再被误用为吞吐扩容。 |

### 需提前明确的运行指标

副本数量应由 RTO/RPO 与成本决定，而不是固定照抄 1 主 1 备：

- **RTO**：主失效到新主开始接收请求的时间；受健康检测、提升、拓扑传播影响。
- **RPO**：最多回退的状态窗口；当前主要受 checkpoint 周期影响。
- 普通/教学地图可使用 `1 主 + 0 备`，故障后从 checkpoint 重建。
- 核心公共地图建议 `1 主 + 1 warm standby`。
- 高价值全局场景可使用 `1 主 + 2 备`，并在后续引入 snapshot + 增量事件/WAL 降低 RPO。

---

## 建议的提交切分（可选）

1. `feat: 拓扑版本化 + 节点 drain/主权模型`（2.2-A）
2. `refactor: 协调器独立进程 + 网关按拓扑直连节点`（2.2-B）
3. `fix: 故障转移引入 MapEpoch fencing 与有序拓扑提交`（2.2-C）
4. `feat: RedisElector token 租约 + LeaderTerm`（2.2-D）
5. `feat: 节点和协调器优雅下线`（2.2-E）
6. `refactor: 统一 wire 协议`（2.3，独立）
7. `feat: 动态地图副本组与副本补齐调度`（3.3-A/B，独立大阶段）
8. `feat: 自动副本提升、旧主回归与 K8s 生命周期适配`（3.3-C/D）
9. `feat: 动态地图分片与负载迁移`（3.4，按需）

每步独立可编译、可回滚。
