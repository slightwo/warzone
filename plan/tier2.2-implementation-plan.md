# Tier 2.2 — 控制面 / 数据面拆分实施计划

> 本文是 Tier 2.2 的独立落地计划，细化 `tier2-remediation-design.md` 中的 2.2-A～2.2-E。
> 目标是将当前网关进程内的控制面职责拆出为独立协调器，并以 Redis 中版本化的拓扑快照作为唯一主权来源。
>
> 本文按阶段（A～E）和阶段内子任务（如 A.1、A.2）拆分；每项都明确目标文件、输入输出、完成条件和边界，便于逐提交实施和评审。

---

## 1. 当前基线与已确认事实

### 1.1 已完成、必须保持不回退的能力

Tier 2.1 已完成并完成本地集群验证：

- 节点通过 `tickLoop` 自行驱动 `world.BackgroundStep()`；
- 节点自行发布 `events:map:<mapID>`；
- 主节点自行保存 Redis checkpoint；
- 副本节点自行拉取 checkpoint；
- 主节点重启后可从 checkpoint 恢复世界；
- 网关重启不应成为世界停止 tick 的原因。

因此，2.2 的任何阶段都**不得**重新引入“协调器远程 tick 世界”或“协调器中转副本快照”的实现。

### 1.2 当前问题

当前 `src/cluster/cluster.go` 中的 `Cluster` 同时承载网关和控制面职责：

- 网关业务：认证、会话、玩家指令转发、地图快照缓存、事件缓存；
- 控制面：`discoveryLoop`、`heartbeatLoop`、`handleNodeFailure`、Boss 生命周期；
- 路由与主权：进程内 `owners` / `replicas` map。

其中最优先的正确性问题是：`discoveryLoop` 直接把节点注册中 `Maps` / `Replicas` 写回内存 owner/replica。若旧主节点在故障切换后带着旧的 `-maps` 参数重新注册，就可能覆盖新主的主权。

### 1.3 目标架构

```text
                        Redis
    ┌─────────────────────┼──────────────────────┐
    │                     │                      │
    │ battle:topology     │ checkpoints          │ events:topology
    │ versioned snapshot  │ map snapshots        │ topology change
    │                     │                      │
┌───▼──────────────┐      │             ┌────────▼───────────┐
│ Coordinator      │──────┘             │ Gateway             │
│ cmd/coordinator  │  写唯一拓扑          │ cmd/server          │
│                  │                     │                     │
│ - discovery      │                     │ - client stream     │
│ - heartbeat      │                     │ - authentication    │
│ - failover       │                     │ - session/routing   │
│ - boss lifecycle │                     │ - cache/event       │
└───┬──────────────┘                     └────────┬───────────┘
    │ control RPC                                 │ data RPC
    │                                             │
    └──────────────────┬──────────────────────────┘
                       ▼
            ┌──────────────────────┐
            │ Node / Map runtime   │
            │                      │
            │ owner: tick/write    │
            │ replica: pull state  │
            │ MapEpoch fencing     │
            └──────────────────────┘
```

### 1.4 总体不变量

实施过程中必须持续满足以下规则：

1. **单一拓扑权威**：`battle:topology` 是 owner、replica 与 map epoch 的唯一权威记录；网关和节点内存仅为缓存。
2. **节点注册不授予主权**：节点注册只表达地址、健康租约、候选能力与 drain 状态；不直接改变 owner。
3. **单一写者**：每张逻辑地图任一时刻仅一个 owner 可以 tick、处理状态写入、保存 checkpoint。
4. **先就绪、后路由**：候选副本必须完成 `Promote` 后，才允许提交为新 owner 并让网关路由流量。
5. **失败不改路由**：任何 Promote、拓扑提交或会话迁移失败，都不能把路由切到未就绪节点。
6. **控制面不承载高频业务**：Move、Attack、Heal、Shop、Snapshot 等数据面请求仍是 gateway → owner node；协调器不成为玩家请求中转。
7. **增量迁移**：每个阶段均应保持可编译、可启动和可回滚，不做跨阶段巨型提交。

---

## 2. 阶段总览与提交边界

| 阶段 | 核心产物 | 是否引入新进程 | 主风险 | 推荐提交 |
|---|---|---:|---|---|
| 2.2-A | `Topology` 持久化、路由缓存刷新、节点注册去主权化 | 否 | 旧注册逻辑覆盖拓扑 | `feat: persist versioned topology` |
| 2.2-B | 独立 `cmd/coordinator`、网关与控制面职责分离 | 是 | 拆分期间路由或会话回归 | `refactor: split coordinator from gateway` |
| 2.2-C | `MapEpoch` fencing、有序故障切换 | 否 | 旧主 / 网络分区双写 | `fix: fence map ownership during failover` |
| 2.2-D | `Elector`、Redis token 租约、LeaderTerm | 可多实例 | 双协调器同时决策 | `feat: add coordinator leader election` |
| 2.2-E | health、ready、drain、最终 checkpoint | 否 | 主动下线造成重复切换 | `feat: support graceful node drain` |

实施顺序固定为 **A → B → C → D → E**。2.3 协议治理和第 3 梯队动态副本调度不属于本计划。

---

## 3. 2.2-A：版本化拓扑基础

### A.0 阶段目标与边界

本阶段将 owner/replica 路由信息从 `Cluster` 内存迁移为 Redis 中单 Key 的完整拓扑快照，并让网关进程订阅和轮询刷新该快照。

本阶段不拆 `cmd/coordinator`，也不要求节点执行 MapEpoch 校验；当前单网关进程临时同时承担初始化与控制决策，但它必须通过统一的拓扑读写接口操作主权。

### A.1 定义拓扑数据契约

**新增文件**：`src/storage/topology.go`

定义以下类型、常量和纯函数：

```go
type Topology struct {
    Version    uint64            `json:"version"`
    LeaderTerm uint64            `json:"leader_term"`
    Owners     map[string]string `json:"owners"`
    Replicas   map[string]string `json:"replicas"`
    MapEpochs  map[string]uint64 `json:"map_epochs"`
    UpdatedAt  time.Time         `json:"updated_at"`
}
```

要求：

- `Version`：每次成功提交拓扑整体递增，初始为 `1`；
- `LeaderTerm`：2.2-D 前保持 `0`，但数据结构和校验必须预留；
- `Owners` / `Replicas`：2.2 期间保持一主一备模型；值为空表示该角色暂未分配；
- `MapEpochs`：每张已纳入拓扑的地图必须存在且大于 `0`；2.2-A 初始化为 `1`，2.2-C 才在主切换时递增并真正参与 fencing；
- `UpdatedAt`：由提交拓扑的控制面填充，采用 UTC；
- 提供 `Clone()`、`Normalize()`、`Validate(knownMapIDs map[string]struct{}) error` 等纯函数，避免调用方直接复用可变 map。

`Validate` 最低校验：

- owner、replica 引用的节点 ID 不为空；
- 同一地图的 owner 与 replica 不相同；
- 未知 map ID 不允许进入拓扑；
- owner 已存在时必须存在正 MapEpoch；
- `Version` 不为 0。

### A.2 实现 Redis 原子拓扑读写

**修改文件**：`src/storage/store.go`  
**新增测试**：`src/storage/topology_test.go`

新增常量：

```text
battle:topology                 # 完整 Topology JSON；单一权威快照
battle:topology:version         # 可选，若后续需要快速观测；不作为权威
```

新增接口：

```go
func (s *Store) LoadTopology() (*Topology, bool, error)
func (s *Store) SaveTopology(topology Topology) error
func (s *Store) CompareAndSaveTopology(expectedVersion uint64, next Topology) error
func (s *Store) PublishTopologyChanged(version uint64) error
```

实现约束：

1. `LoadTopology` 区分“Key 不存在”和“Redis / JSON 出错”；调用方不能把读取异常当作空拓扑处理。
2. `SaveTopology` 只写完整 JSON 到一个 Redis Key，禁止将 owners、replicas、epochs 分散写入多个 Hash。
3. `CompareAndSaveTopology` 用 Lua 校验现存拓扑版本后再 `SET` 新 JSON；无现存值时仅接受 `expectedVersion == 0` 且 `next.Version == 1`。
4. Lua 成功写入后再发布 `events:topology`；发布失败必须记录错误，并依赖后续轮询补偿刷新，不能回滚已提交拓扑。
5. `Topology` 序列化前必须 `Clone + Normalize + Validate`，避免调用方后续修改 map 改写已提交对象的语义。

测试最低覆盖：

- 空 Redis 返回 `found=false, err=nil`；
- 正常保存后完整读回，map 不共享引用；
- 相同版本并发提交中仅一个成功；
- 旧 `expectedVersion` 被拒绝；
- 非法 owner/replica/epoch 被拒绝；
- JSON 损坏返回错误而非空拓扑。

> 测试实现优先在 Store 内部抽出可注入 Redis 命令执行器，避免单元测试依赖开发者本机 Redis；必要时再新增独立集成测试。

### A.3 明确节点注册的降级语义

**修改文件**：

- `src/storage/topology.go` 或 `src/storage/store.go`：更新 `NodeRegistryInfo` 注释；
- `src/cmd/node/main.go`：调整启动日志与字段命名说明；
- `src/node/node.go`：保留 `AddReplicaMap`，补充“本地副本能力不代表路由主权”的注释。

规则：

- 短期保留 `NodeRegistryInfo.Maps` / `Replicas` 及 `-maps` / `-replicas` 参数，以兼容当前部署命令；
- 在 2.2-A 中将它们重新定义为 `DeclaredPrimaryMaps` / `DeclaredReplicaMaps` 的候选能力语义；可先只改注释和变量名，JSON 字段维持兼容；
- 节点注册内容不得覆盖已存在的 `Topology.Owners`、`Topology.Replicas` 或 `Topology.MapEpochs`；
- 只有“拓扑尚不存在”时，控制面才允许根据所有已发现节点的声明能力做一次 bootstrap；bootstrap 成功后写入 Version `1`，以后不再从启动参数重建主权。

Bootstrap 规则：

1. 读取活跃节点注册信息并按 node ID 排序；
2. 只为已知地图建立一主一备；重复 owner 或 owner=replica 必须报错，不采用“最后注册者覆盖”；
3. bootstrap 通过 `CompareAndSaveTopology(0, initial)` 提交，竞争失败则重新加载已由其他实例写入的拓扑；
4. 发现拓扑已存在时，节点新加入只加入可连接节点池，不改变当前主从归属。

### A.4 让 Cluster 通过 Topology 缓存路由

**新增文件**：`src/cluster/topology_cache.go`  
**修改文件**：`src/cluster/cluster.go`

本阶段可保留 `Cluster` 名称以减少重构面，但 owner/replica 的来源必须改为拓扑缓存：

- 为 `Cluster` 增加 `topology storage.Topology`（或 `*storage.Topology`）字段；
- 新增 `applyTopologyLocked(topology storage.Topology)`：深拷贝并同步本地路由缓存；
- 新增 `loadTopology()`：从 Store 读取并应用；
- 新增 `topologySyncLoop()`：以 300～500ms 周期做兜底轮询；
- `Start()` 在启动业务路由循环前先加载拓扑；若拓扑不存在，等待 discovery bootstrap；若读取出错，记录并保留最后一个成功快照；
- `Login`、`SwitchMap`、`sessionNode`、`mapCacheLoop` 只读 topology 缓存，不再把 `owners/replicas` 作为独立权威状态。

迁移策略：

1. 先保留原有 `owners` / `replicas` 字段作为过渡缓存，但只允许 `applyTopologyLocked` 写入；
2. 删除 discovery、recover、failover 中对这两个 map 的直接写入；
3. 所有路由和故障转移代码改为以本地 `Topology` 副本计算下一状态；
4. 2.2-B 稳定后，再删除过渡字段并改为直接访问 `topology.Owners/Replicas`。

### A.5 改造 discovery 与拓扑事件订阅

**修改文件**：

- `src/cluster/cluster.go`：`discoveryLoop`、`eventLoop`；
- `src/storage/store.go`：`SubscribeEvents`。

具体改动：

- `SubscribeEvents` 增加精确订阅 `events:topology`；
- `eventLoop` 收到拓扑事件后调用 `loadTopology()`，只按版本向前应用；
- `discoveryLoop` 仅负责发现可达节点、创建 `NodeGRPCClient`、维持节点池；
- discovery 首次具备足够节点声明能力时调用 `bootstrapTopologyIfAbsent`；
- 已存在 topology 时，节点重新注册只更新客户端和健康状态，绝不覆盖主权；
- 若 topology 指向尚未连通的 node，网关保持“地图暂不可用”而不是回退到节点声明配置。

### A.6 A 阶段验收与回滚

验收用例：

1. 空 Redis 启动：节点全部注册后仅生成一个 Version `1` 拓扑；
2. 重启 gateway：从 `battle:topology` 恢复相同路由，不依赖本进程发现顺序；
3. 模拟旧主重启：其 `-maps` 声明不能覆盖当前 owner；
4. 手工修改 / 发布更高 Version 拓扑后，gateway 在事件或轮询周期内刷新；
5. 拓扑 JSON 损坏或 Redis 短暂失败时，已有网关继续使用最后成功快照并记录错误，不清空路由；
6. `go test ./...`、`go vet ./...`、三节点 + 网关启动与短压测通过。

回滚策略：

- 新逻辑只新增 `battle:topology`，不修改 checkpoint、会话和节点注册现有 Key；
- 回滚代码后旧版本会重新使用内存注册逻辑；
- 若需要重新 bootstrap，显式删除 `battle:topology`，不得在启动流程中自动删除。

---

## 4. 2.2-B：协调器独立进程与网关职责收敛

### B.0 阶段目标与边界

本阶段将控制面的 discovery、heartbeat、拓扑提交、故障转移和 Boss 生命周期从 gateway 进程移动到独立的 `cmd/coordinator`。网关保留客户端连接、认证、会话、事件、快照缓存与数据面转发。

本阶段采用 `AlwaysLeader` 单协调器模式；真实选主留给 2.2-D。

### B.1 拆分节点客户端接口

**新增文件**：`src/cluster/node_client.go`  
**修改文件**：`src/cluster/grpc_client.go`

将现有混合 `NodeClient` 拆为：

```go
type GatewayNodeClient interface {
    NodeID() string
    AddPlayer(context.Context, string, *protocol.UserProfile) error
    RemovePlayer(context.Context, string, string) (protocol.UserProfile, bool, error)
    MovePlayer(context.Context, string, string, string) (string, protocol.UserProfile, bool, error)
    Attack(context.Context, string, string) (string, string, string, protocol.UserProfile, bool, error)
    Heal(context.Context, string, string) (string, protocol.UserProfile, bool, error)
    BuyItem(context.Context, string, string, string) (string, protocol.UserProfile, bool, error)
    AttackBoss(context.Context, string, string) (string, protocol.UserProfile, bool, error)
    Profile(context.Context, string, string) (protocol.UserProfile, bool, error)
    RewardPlayer(context.Context, string, string, int, int) (protocol.UserProfile, bool, error)
    Snapshot(context.Context, string) (protocol.MapView, error)
}

type CoordinatorNodeClient interface {
    NodeID() string
    Ping(context.Context) error
    View() protocol.NodeView
    Checkpoint(context.Context, string) (protocol.MapCheckpoint, error)
    Promote(string, world.MapConfig) error
    Close() error
}
```

约束：

- `NodeGRPCClient` 同时实现两个接口；
- `Start` / `Stop` 不能继续作为远端节点生命周期 RPC 的假实现存在于控制接口中；节点进程由部署系统管理；
- 当前 `healthy` 本地标记可先留在协调器侧连接包装中，后续以节点注册 lease / health 统一；
- 该子任务只拆接口，不改变业务行为。

### B.2 新建 coordinator 领域包

**新增目录及文件**：

```text
src/coordinator/
  coordinator.go     # 生命周期、依赖装配、Start/Close
  discovery.go       # 节点发现与连接池维护
  heartbeat.go       # 应用层 Ping 与故障触发
  topology.go        # bootstrap、加载、CompareAndSaveTopology
  failover.go        # 副本选择、Promote、会话迁移
  boss.go            # Boss 初始化与复活生命周期
```

职责约束：

- coordinator 持有 `Store`、`CoordinatorNodeClient` 池、已知 map 配置；
- coordinator 是唯一允许调用 `CompareAndSaveTopology` 的业务组件；
- discovery 仅更新“节点可用列表”，不得隐式改变地图 owner；
- failover 的所有拓扑变更必须走 `topology.go` 的单一提交方法；
- Boss 的初始化和复活逻辑从 `cluster.Cluster` 迁入 `boss.go`；网关仅 `LoadGlobalBoss` 读取。

### B.3 收敛 Cluster 为网关业务门面

**拆分文件**：

```text
src/cluster/
  cluster.go          # 过渡期 Gateway 门面；最终移除控制面方法
  routing.go          # Topology -> GatewayNodeClient 连接与 sessionNode
  snapshot_cache.go   # mapCacheLoop 与 SnapshotFor
  events.go           # eventLoop、用户/地图/全局事件缓存
  session.go          # Login、Logout、SwitchMap、会话读取与缓存
```

**修改文件**：`src/cluster/cluster.go`

迁移要求：

- 从 `Cluster.Start()` 移除 `discoveryLoop`、`heartbeatLoop`；保留拓扑刷新、地图缓存与事件订阅；
- 从 gateway 删除 `handleNodeFailure`、`recoverNode`、`failNode` 与 Boss 生命周期；
- gateway 根据 `Topology.Owners` 创建或复用 `GatewayNodeClient`，按 Version 清理失效连接；
- `GlobalSession` 继续由 Redis 作为权威，`localSessions` 只作加速缓存；
- `ExecuteAdmin` 在本阶段可保留只读 status，故障 / 恢复命令迁到 coordinator 管理接口后再恢复。

### B.4 新增 coordinator 进程入口与启动顺序

**新增文件**：`src/cmd/coordinator/main.go`  
**修改文件**：`src/cmd/server/main.go`、`tostart.txt`

启动顺序调整为：

```text
node(s) → coordinator → gateway → client
```

要求：

- `cmd/coordinator` 创建 `storage.Store`、`coordinator.Coordinator` 并启动控制循环；
- `cmd/server` 只创建 gateway 门面，不再启动控制面循环；
- coordinator 未就绪或 topology 缺失时，gateway 对用户明确返回“路由尚未就绪”，不得猜测 owner；
- 为便于本地开发，gateway 可轮询等待 topology，但生产模式不应无限阻塞启动；
- 用进程信号分别关闭 gateway 和 coordinator，验证 gateway 关闭不会停止节点 tick。

### B.5 B 阶段验收与回滚

验收用例：

1. 关闭 gateway 后，节点 checkpoint Version 持续递增；
2. 重启 gateway 后，能从 Redis topology 恢复路由并继续登录、移动；
3. 关闭 coordinator 后，既有 gateway 仍能使用最后拓扑提供已有 owner 的数据面服务；但不进行新的故障切换；
4. coordinator 启动后能发现已运行节点并恢复 health 检查；
5. 所有 Move / Attack / Snapshot 数据面请求均不经过 coordinator；
6. `go test ./...`、三节点集群短压测和跨地图切换通过。

回滚策略：

- coordinator 是新增进程，回滚时可恢复 gateway 内的兼容控制循环；
- 在删除旧循环前，先通过 feature flag 或显式 `BATTLEWORLD_CONTROL_MODE=embedded|external` 运行双模式验证；
- 双模式期间必须保证同一时刻只能有一个控制面写 Topology，避免双写。

---

## 5. 2.2-C：MapEpoch fencing 与故障转移有序提交

### C.0 阶段目标与边界

本阶段解决“旧主网络恢复 / 延迟观察到新拓扑后继续 tick 或写 checkpoint”的双写风险。`MapEpoch` 从 A 阶段的预留字段变为节点、网关、存储共同执行的 fencing token。

### C.1 定义地图主权上下文

**新增文件**：`src/topology/authority.go` 或 `src/node/authority.go`  
**修改文件**：`src/storage/topology.go`

定义：

```go
type MapAuthority struct {
    MapID string
    Owner string
    Epoch uint64
}
```

规则：

- 只有 `Topology.Owners[mapID] == nodeID` 且 `Topology.MapEpochs[mapID] == epoch` 的节点可以写；
- 初次 bootstrap 赋予 Epoch `1`；
- 每次 owner 节点变化必须递增 Epoch；仅补换 replica 不递增 Epoch；
- epoch 不允许回退，Topology Version 也不允许回退。

### C.2 为节点 RPC 显式携带主权 epoch

**修改文件**：

```text
src/pb/battle.proto
src/pb/battle.pb.go
src/pb/battle_grpc.pb.go
src/cluster/grpc_client.go
src/node/grpc_server.go
```

要求：

- 对所有会改变地图状态的 NodeService 请求增加 `map_epoch`：AddPlayer、RemovePlayer、MovePlayer、Attack、Heal、BuyItem、AttackBoss、RewardPlayer、Promote；
- Snapshot / Counts / Profile 可先作为只读请求不带 epoch；
- gateway 每次根据当前 Topology 取 owner + epoch 后发起写 RPC；
- node gRPC handler 在调用 `NodeService` 前校验 map owner 和 epoch；校验失败返回明确的 `FailedPrecondition`；
- 不使用隐式 gRPC metadata 作为最终协议；若为过渡兼容使用 metadata，必须在同一阶段移除并改为显式字段。

### C.3 节点侧主权缓存与写入门禁

**新增文件**：

```text
src/node/authority.go        # Topology 拉取、缓存、Owner/Epoch 校验
src/node/authority_test.go
```

**修改文件**：`src/node/node.go`、`src/cmd/node/main.go`

要求：

- `NodeService.Start()` 启动 topology refresh loop；订阅拓扑事件并以轮询兜底；
- `tickLoop` 仅对本节点当前 owner 的地图执行 `BackgroundStep` 与 checkpoint；
- 所有状态写入 API 在 world 调用前执行 `RequireAuthority(mapID, epoch)`；
- 节点丢失 topology 连接时采用 fail-closed：已有 owner 权限只能在有限宽限期内使用，超过宽限期后停止 tick 和写 checkpoint；
- 副本节点继续拉 checkpoint，但不能处理玩家写入或启动 tick；
- 初始 `-maps` 只表示可实例化能力；尚未取得 Topology owner 时不得对外承诺主服务。

### C.4 将 checkpoint 写入与 fence 原子绑定

**修改文件**：`src/storage/store.go`  
**新增测试**：`src/storage/fence_test.go`

新增 Redis 结构：

```text
battle:map:fence:<mapID>      # owner node ID + epoch；由拓扑提交同步更新
```

实现要求：

1. coordinator 提交新 topology 时用 Lua 在同一个 Redis 原子操作中写入 topology JSON 和被变更地图的 fence；
2. 节点保存 checkpoint 时调用 `SaveCheckpointIfOwner(cp, nodeID, epoch)`；Lua 先校验 fence 的 owner/epoch，再执行 `HSET checkpoints`；
3. 旧 owner 即使本地尚未刷新 topology，也无法写入新 epoch 的 checkpoint；
4. 保留普通 `SaveCheckpoint` 仅供迁移期或显式无主权的初始化路径，C 阶段结束后主节点 tick 不得调用它。

### C.5 按固定状态机改写故障转移

**修改文件**：`src/coordinator/failover.go`、`src/coordinator/topology.go`

对单张地图的故障转移固定为：

```text
读取当前 Topology (version=N, epoch=E)
→ 校验当前 owner 确实失效
→ 选择健康副本
→ 校验副本 checkpoint 可用且版本满足最低要求
→ 对副本执行 Promote(mapID, E+1)
→ CompareAndSaveTopology(N, owner=replica, epoch=E+1)
→ 原子更新该地图受影响的 GlobalSession
→ 发布 events:topology
→ 网关刷新路由
```

失败规则：

- `Promote` 失败：Topology、fence、session 均不改；
- Topology CAS 失败：停止本次切换，重新加载并重新评估；不得再次盲目 Promote；
- 会话更新失败：记录需修复会话，网关基于 topology 重试路由；不可回滚已成功提交的新 owner；
- 同一失效节点涉及多张地图时，每张地图独立处理、独立提交，避免一张失败阻塞其他地图。

### C.6 C 阶段验收

- kill owner 后仅一个副本成功提升；
- 原 owner 恢复并沿用旧 `-maps` 时，不能 tick、不能处理写请求、不能写 checkpoint；
- 旧 epoch 的写 RPC 被 node 拒绝；
- 人工构造旧节点 checkpoint 写入请求被 Redis fence 拒绝；
- Promote 失败时 topology Version、owner 和 session 不发生错误跳转；
- checkpoint owner / epoch 与 topology 可观测且一致。

---

## 6. 2.2-D：协调器高可用与 LeaderTerm

### D.0 阶段目标与边界

本阶段允许多个 coordinator 实例运行，但任一时刻仅 leader 执行 discovery、heartbeat、failover 与 Boss 生命周期。该阶段不改变 gateway → node 数据面路径。

Lab4 的 leader-only 结构可参考；其 Redis 实现中“GET 后无条件 SETEX 续租”不能照搬，因为旧 leader 有覆盖新 lease 的风险。

### D.1 抽象 Elector 接口与单实例实现

**新增目录及文件**：

```text
src/elector/
  elector.go          # Elector 接口
  always_leader.go    # 单实例实现
  always_leader_test.go
```

接口：

```go
type Elector interface {
    Campaign(ctx context.Context) (leaderCtx context.Context, err error)
    IsLeader() bool
    Term() uint64
    Resign() error
}
```

`AlwaysLeader` 用于先验证 coordinator 进程拆分，不依赖 Redis 租约。

### D.2 实现 Redis token 租约

**新增文件**：

```text
src/elector/redis.go
src/elector/redis_scripts.go
src/elector/redis_test.go
```

实现要求：

- 每个 coordinator 实例生成随机、不可复用 token；
- 抢锁：`SET key token NX PX ttl`；
- 续租：Lua 仅当 value 等于当前 token 时 `PEXPIRE`；
- 释放：Lua 仅当 value 等于当前 token 时 `DEL`；
- 租约丢失立刻取消 leader context；
- 当选新 leader 时分配递增 `LeaderTerm`；term 不能由本地内存生成，必须来自 Redis 持久化计数或 topology CAS；
- 任一 Redis 错误都应使实例退为 non-leader，禁止继续提交拓扑。

### D.3 将控制循环绑定到 leader context

**修改文件**：

```text
src/coordinator/coordinator.go
src/coordinator/discovery.go
src/coordinator/heartbeat.go
src/coordinator/failover.go
src/coordinator/boss.go
src/cmd/coordinator/main.go
```

要求：

- 仅 leader 启动或保留 discovery、heartbeat、failover、Boss 复活循环；
- leader context 被取消时，停止新决策；正在执行的故障转移在 CAS 前必须再次检查 leader 状态与 term；
- topology 提交要求 `next.LeaderTerm == elector.Term()`；
- gateway 和 node 不需要选主，只读取最终 topology。

### D.4 D 阶段验收

- 两个 coordinator 并发运行时，只有一个产生 topology 变更；
- leader 被终止后，另一个实例接管并使用更高 `LeaderTerm`；
- 旧 leader 恢复网络后，无法续租或以旧 term 覆盖 topology；
- gateway 在 leader 切换期间继续使用最后有效 topology；
- 选主日志、当前 leader ID、term、lease 剩余时间可观测。

---

## 7. 2.2-E：优雅下线、健康检查与 drain

### E.0 阶段目标与边界

本阶段借鉴 Lab4 的 `/healthz`、`/readyz`、`/drain` 语义，保证主动发布或缩容不等同于节点故障，并避免 drain 节点仍接收新路由或继续覆盖 checkpoint。

### E.1 抽取通用生命周期组件

**新增文件**：`src/lifecycle/http.go`  
**新增测试**：`src/lifecycle/http_test.go`

提供可复用的 HTTP handler：

- `/healthz`：进程存活即返回 200，同时暴露 draining 与活跃连接数；
- `/readyz`：draining 或拓扑未就绪时返回 503；
- `/drain`：幂等触发 drain 回调，允许预停止等待。

不依赖 Kubernetes API；Kubernetes 仅作为未来调用这些端点的部署环境。

### E.2 节点 drain 与最终 checkpoint

**新增文件**：`src/node/drain.go`  
**修改文件**：`src/node/node.go`、`src/cmd/node/main.go`、`src/storage/topology.go`

顺序：

```text
标记 NodeRegistryInfo.Draining=true
→ 读取 / 等待 coordinator 从候选或路由中摘除该节点
→ 若仍为 owner，协调器先安排受控迁移或明确不可 drain
→ 节点停止接收新写请求
→ 保存最终 fenced checkpoint
→ 停止 tick 和 gRPC 服务
```

要求：

- drain 状态为注册信息的一部分，但不直接修改 owner；
- gateway 不向 draining node 新建路由；
- node 未完成主权移交前不能直接退出，避免把主动下线误判为意外故障后重复 Promote；
- 需要定义 drain 超时和强制退出行为，并以日志暴露未迁移地图。

### E.3 coordinator 与 gateway drain

**新增 / 修改文件**：

```text
src/coordinator/drain.go
src/cmd/coordinator/main.go
src/cluster/drain.go
src/cmd/server/main.go
```

- coordinator drain：停止参选、停止故障决策、完成当前 topology 提交后退出；
- gateway drain：停止接受新客户端，等待已有 stream 自然结束或超时；
- gateway 关闭不影响 node tick；
- 若采用多 coordinator，先 `Resign` 再停止 HTTP / gRPC 服务。

### E.4 E 阶段验收

- 调用 node `/drain` 后，gateway 不再把新用户路由到该节点；
- drain 主节点完成最终 checkpoint 和受控迁移后退出，不出现重复 Promote；
- gateway drain 后无新连接、已有连接在超时内结束；
- coordinator drain 后能由其他 leader 接管；
- `/healthz`、`/readyz`、`/drain` 返回码与状态符合部署探针语义。

---

## 8. 跨阶段测试矩阵

| 测试层次 | A | B | C | D | E |
|---|---:|---:|---:|---:|---:|
| Topology codec / 校验单测 | 必须 | 保持 | 扩展 epoch | 扩展 term | 保持 |
| Redis CAS / Lua 集成测试 | 必须 | 保持 | 必须 fence | 必须 lease | 保持 |
| Gateway 路由冒烟 | 必须 | 必须 | 必须 | 必须 | 必须 |
| 三节点 checkpoint 恢复 | 保持 | 必须 | 必须 | 必须 | 必须 |
| owner 故障转移 | 观察 | 基础 | 必须严格验证 | 双协调器验证 | drain 验证 |
| 旧主回归 | 不回抢 | 不回抢 | 不可写 / 不可 tick | 不可覆盖 term | 作为候选恢复 |
| 压测 | 短压测 | 短压测 | 短压测 | 双协调器短压测 | drain 期间短压测 |

建议新增最小自动化集：

```text
src/storage/topology_test.go
src/cluster/topology_test.go
src/coordinator/failover_test.go
src/node/authority_test.go
src/elector/redis_test.go
src/lifecycle/http_test.go
```

对于需要 Redis 的测试，优先使用可注入 command executor 或启动临时 Redis；不得依赖开发者本机长期运行的数据实例。

---

## 9. 部署与兼容迁移顺序

### 9.1 从当前版本升级到 2.2-A

1. 部署新 node 代码，但仍使用现有 `-maps/-replicas` 参数声明能力；
2. 启动包含拓扑逻辑的单一 gateway / embedded control；
3. 若 `battle:topology` 不存在，由已发现注册信息 bootstrap Version `1`；
4. 记录 topology 内容、Version 和 owner/replica；
5. 重启 gateway 验证路由从 topology 恢复；
6. 验证旧 owner 重启不会改写 topology。

### 9.2 从 2.2-A 升级到 2.2-B

1. 启动独立 coordinator，但先不启动 gateway 内嵌控制循环；
2. coordinator 加载既有 topology 并开始节点健康检查；
3. gateway 仅订阅 / 轮询 topology；
4. 验证节点 tick 与 checkpoint 不依赖 gateway；
5. 稳定后删除 embedded control 模式。

### 9.3 从 2.2-B 升级到 2.2-C

1. 先发布能读取 `MapEpochs` 但不强制拒绝旧 epoch 的兼容节点；
2. 完成 topology fence Key、写 RPC epoch 字段和 checkpoint Lua guard；
3. coordinator 开始提交新 epoch；
4. 打开强制 `RequireAuthority`；
5. 通过旧主回归测试后移除兼容分支。

---

## 10. 明确不纳入 Tier 2.2 的工作

以下事项必须在后续独立阶段处理，避免扩大本轮变更：

- 端到端 protobuf / JSON 统一与 `protocol/adapter.go` 重写（2.3）；
- 动态副本数量、容量调度、自动补副本和重平衡（3.3）；
- 同地图 active-active 写入或 CRDT 状态合并；
- 地图分片、跨 shard 负载迁移（3.4）；
- 以事件流 / WAL 替代当前全量 checkpoint 的 RPO 优化；
- PostgreSQL / Redis 物理高可用、跨机房容灾；
- Lab4 的 Kubernetes Pod deletion cost、ConfigMap lease 等 K8s 专属实现。

---

## 11. 推荐的实施起点

下一项实际编码工作固定为 **2.2-A.1 + A.2**：

1. 新增 `src/storage/topology.go`，完成 `Topology`、clone、normalize、validate；
2. 在 `src/storage/store.go` 增加单 Key 的 load/save/CAS API；
3. 先为该存储契约补测试；
4. 测试通过后再进入 A.3～A.5 的 Cluster 路由接入。

这样可以先把“拓扑是一个可验证、可持久化、可版本比较的数据契约”稳定下来，再改 discovery、failover 和进程边界，避免一开始就在多个 goroutine 和多个进程之间迁移隐式状态。
