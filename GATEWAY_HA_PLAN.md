# Battleworld — Gateway 高可用与容量扩展计划

> 本文记录 Gateway 接入层的高可用与容量改造方案，目标是消除单实例 Gateway 的连接单点，并降低高连接数下的状态同步成本。
>
> **现状边界**：Coordinator 已独立为 `cmd/coordinator`，负责选主、节点发现、健康检查、拓扑与故障转移；Node 由自身 tick loop 驱动世界并保存 fenced checkpoint。本文不改变地图 owner 的单写者模型，也不把高频玩家请求转发给 Coordinator。

---

## 1. 当前基线

### 1.1 已具备的基础

- Gateway（`cmd/server`）通过 gRPC `GatewayService.GameStream` 提供玩家双向长连接。
- Gateway 从 Redis 读取版本化 Topology，并按 owner 建立到 Node 的数据面 gRPC 连接。
- Coordinator 与 Node 已是独立进程；Gateway 重启不会停止 Node 的世界 tick，也不会阻止 Coordinator 继续进行故障转移。
- Gateway 已支持 `/healthz`、`/readyz`、`/drain`；`drain` 会停止 gRPC 接入并让现有流优雅结束。
- `mapCacheLoop` 按地图拉取 Node 快照，`SnapshotFor` 从本地地图缓存组装玩家状态，避免每位玩家推送都触发一次 Node Snapshot RPC。
- 每条游戏流的状态帧只保留最新待发送快照，慢客户端不会无限积压状态帧；认证结果、命令确认与错误仍可靠排队。

### 1.2 仍存在的问题

1. **单接入实例**：Gateway 固定监听 `127.0.0.1:9310`，本地启动脚本只拉起一个 Gateway。该进程故障会断开全部客户端流，新玩家也无法登录。
2. **异常退出会遗留会话**：`global_sessions` 使用 Redis Hash 保存，没有 TTL 或 Gateway 归属。Gateway 正常断流会 Logout；异常退出时无法清理，会让玩家被错误判为仍在线。
3. **状态构建按用户放大**：每条已认证流默认每 100ms 调用一次 `SnapshotFor(username)`，即每秒约 10 次。尽管地图从缓存读取，但完整 `WorldState` 的拼装、protobuf 转换与发送仍按连接数线性增长。
4. **全量状态重复传输**：常态推送包含当前地图的 terrain、全部玩家、NPC、宝物及其它摘要。同地图多个客户端会接收大量重复内容；热门地图的总传输量会随地图人数近似平方增长。
5. **缺少接入层治理与观测**：没有显式的连接上限、限流、慢客户端指标、流量指标或基于路由就绪状态的 readiness 判定。

### 1.3 目标架构

```text
客户端
  │  gRPC 长连接
  ▼
L4 / gRPC Load Balancer
  ├──────────── Gateway-1 ────┐
  ├──────────── Gateway-2 ────┼── 数据面 RPC ──> 当前地图 Owner Node
  └──────────── Gateway-N ────┘
                 │
                 ├── Redis: Topology、Session Lease、事件
                 └── PostgreSQL: 用户与持久化 Profile

Coordinator ── 仅负责拓扑、选主、健康检查、故障转移 ──> Redis / Node 控制 RPC
```

不变量：

- Gateway 可以横向扩容，但**每张地图任一时刻仍只有一个 owner Node 执行 tick 与状态写入**。
- 任一 Gateway 仅是连接接入、认证、路由、缓存与推送实例；它不拥有地图主权。
- Gateway 故障可以导致客户端短暂断连，但不应导致地图停止运行、主权丢失或玩家无法在重连后恢复。
- 拓扑、MapEpoch fencing 与 Node 的写入校验保持现有语义。

---

## 2. 实施顺序总览

| 阶段 | 主题 | 解决问题 | 依赖 |
|---|---|---|---|
| A | 接入多副本与就绪治理 | Gateway 单点、滚动发布不可控 | 无 |
| B | 会话租约与断线恢复 | 异常宕机后的幽灵在线 | A 前后均可，建议紧随 A |
| C | 可观测性与过载保护 | 不知容量边界、过载失控 | A |
| D | Gateway 侧增量状态同步 | 全量状态导致的 CPU/带宽瓶颈 | B 推荐完成；可独立实现 |
| E | 压测、故障演练与容量验收 | 验证高可用与性能目标 | A-D 分阶段执行 |

推荐顺序：**A → B → C → D → E**。其中 D 是相对独立的性能专项，可在 A 完成后并行设计与实施；它不依赖多 Gateway 才能产生收益。

---

## 3. 阶段 A：Gateway 多副本与就绪治理

### 3.1 目标

将 Gateway 从固定单实例接入点改造成可部署多个副本的无主接入层；由外部负载均衡器将新 gRPC 连接分配到健康实例。

### 3.2 主要改动

1. **监听配置化**
   - Gateway 监听地址不能继续固定在 `protocol.GatewayAddr`。
   - 增加启动参数或运行时配置，例如 `-addr 0.0.0.0:9310`；保留当前地址作为本地开发默认值。
   - `start.sh` 维持单 Gateway 本地模式，但可用环境变量或参数启动多个 Gateway 实例。

2. **外部负载均衡**
   - 在部署环境使用 L4 TCP 或支持 HTTP/2/gRPC 的 L7 Load Balancer。
   - 负载均衡器只向 `/readyz` 正常的 Gateway 分配**新**连接。
   - gRPC 双向流是长连接；已有连接天然固定在建立时选中的 Gateway，不做流内迁移。

3. **补强 readiness**
   - `/healthz`：仅说明进程可响应。
   - `/readyz`：必须同时满足未 drain、Redis 可访问、Topology 已加载且本实例已建立所需 owner Node 数据面连接。
   - 在 Topology 刷新失败且超过可接受窗口时，实例应进入 not-ready，避免继续承接新登录流量。

4. **完善 drain 语义**
   - `POST /drain` 将 Gateway 标记为 not-ready，让负载均衡器停止分配新连接。
   - 等待既有流自然结束，或在配置的 drain deadline 之前向客户端发送可重试通知并关闭流。
   - 进程最终调用 `GracefulStop`，避免立即中断正在发送的可靠控制消息。

### 3.3 验收

- 同时启动至少两个 Gateway，均可从 Redis 同步相同 Topology 并路由到相同的 Node owner。
- 将一个 Gateway drain 后，新的连接只进入其他 ready 实例；该实例已有流不被立即强制中断。
- 强制停止一个 Gateway 后，Coordinator 与 Node 持续运行；客户端重连至另一 Gateway 后可继续游戏。

---

## 4. 阶段 B：Session Lease 与自动恢复

### 4.1 目标

使 Redis 会话具备可过期的所有权，避免 Gateway 非正常退出后遗留永久 `global_sessions` 记录，并支持安全重连。

### 4.2 会话模型

将当前仅含 `username/mapID/nodeID/version` 的会话记录扩展为至少包含：

```text
username       玩家标识
session_id     每次成功登录生成的随机唯一值
gateway_id     当前接入 Gateway 实例标识
map_id         当前地图
node_id        最近一次已知 owner，非写路由权威
version        会话或地图切换版本
expires_at     租约过期时间（用于观测）
```

Redis 存储不得再使用无法设置 field TTL 的单一 Hash 作为唯一会话真相。可选方案：

- 每位玩家单独使用带 TTL 的 Key，例如 `battle:session:<username>`；或
- Hash 保存内容，同时维护独立 TTL lease key；但读取与续租必须原子一致。

推荐首选单 Key + TTL，模型清晰且不会出现 Hash field 无法自动过期的问题。

### 4.3 原子操作

1. **登录获取会话**
   - 使用 Redis Lua 脚本或等价原子操作检查旧会话。
   - 旧会话仍有效且 `session_id` 不同：拒绝重复登录，或按明确策略踢出旧会话；第一阶段建议保持“拒绝重复登录”。
   - 旧会话已过期：原子写入新会话和 TTL。

2. **续租**
   - Gateway 对每个活动会话，或使用聚合的会话心跳机制，在 TTL 的固定比例内续期。
   - 续租必须校验 `session_id` 与 `gateway_id`，不能让旧 Gateway 续活已被接管的新会话。

3. **登出与流结束**
   - 正常 Logout 仅删除与当前 `session_id` 匹配的 Key，防止旧流关闭误删新重连会话。
   - 删除后再执行 Node RemovePlayer 与 Profile 持久化；错误需记录并明确补偿策略。

4. **重连**
   - 客户端携带可短期验证的重连凭据，或在当前账号密码认证模型下重新认证。
   - 重连成功后由新 Gateway 重新建立 Session Lease、按 Topology 连接当前 owner，并返回完整状态快照。
   - 不允许客户端依据旧 `node_id` 直接路由；一律以当前 Topology owner 和 MapEpoch 为准。

### 4.4 验收

- Gateway 被 `SIGKILL` 后，Session Lease 在 TTL 内过期，玩家随后能重新登录。
- 旧连接恢复网络或延迟关闭时，不能删除/续活新连接的 Session。
- 主备切换期间重连总是按最新 Topology 进入新 owner。

---

## 5. 阶段 C：可观测性与过载保护

### 5.1 目标

在扩容前先获得容量基线；在容量耗尽前明确拒绝或降级，避免协程、内存、文件描述符和网络缓冲无界增长。

### 5.2 指标

至少暴露或记录以下指标：

| 类别 | 指标 |
|---|---|
| 连接 | 当前活跃流数、新建/关闭流速率、认证失败数、每实例连接数 |
| 命令 | 各命令 QPS、拒绝数、Node RPC 延迟/错误、按地图命令量 |
| 推送 | 全量/增量状态帧数、丢弃/合并状态帧数、消息字节数、发送耗时 |
| 慢客户端 | 控制队列等待、状态被覆盖次数、长时间阻塞的 `Send` 数量 |
| 依赖 | Redis、PostgreSQL、Node RPC 的可用性和延迟 |
| 运行时 | goroutine 数、堆内存、GC 停顿、文件描述符使用量 |

`lifecycle.Status.ActiveConnections` 应由 Gateway 实际维护，而非始终返回零，以便 readiness、drain 与部署平台观察当前存量连接。

### 5.3 保护策略

1. 配置最大活跃连接数；达到上限时拒绝新流并返回可重试错误。
2. 限制单用户并发流；会话租约成为最终保障，接入层可提前拦截。
3. 对移动、攻击、购买等命令按用户实施令牌桶限流；超限返回稳定的 `COMMAND_REJECTED` 或 `INVALID_REQUEST`，不进入 Node RPC。
4. 配置 gRPC 最大消息大小、keepalive、连接空闲策略和并发流限制；具体值由压测结果确定，不能硬编码为未经验证的生产值。
5. 慢客户端只保留最新**可合并状态**；控制消息受有限队列与超时保护，避免单流长期阻塞消耗无限资源。

### 5.4 验收

- 达到连接/命令阈值时，Gateway 保持健康，拒绝新请求而非出现内存持续增长。
- 能按实例、地图和状态帧类型定位容量热点。
- drain 期间可观察活跃连接逐步下降至零。

---

## 6. 阶段 D：Gateway 侧增量状态同步

### 6.1 独立性与边界

本阶段主要减少 Gateway 的状态构建、序列化和发送成本，**可独立于多副本 Gateway 和 Session Lease 实施**。它不改 Node 的世界规则、Coordinator 的拓扑逻辑、MapEpoch fencing 或 Redis 会话所有权。

第一阶段由 Gateway 比较相邻的地图缓存快照生成 Delta；因此 Node 仍按当前 RPC 返回完整地图 Snapshot。后续如有必要，再让 Node 原生输出事件/Delta。

### 6.2 改造前

```text
每条已认证 GameStream，每 100ms：
  SnapshotFor(username)
    → 从 mapCache 复制当前完整 MapView
    → 装配完整 WorldState
    → 转 protobuf
    → 发送给单个客户端
```

若在线连接数为 N，则约有 `10N` 次/秒玩家状态组装与 protobuf 转换。同地图玩家收到的 terrain、其他玩家、NPC、宝物等绝大部分内容高度重复。

### 6.3 改造后

```text
首次认证 / 重连 / 切图 / 版本缺口 / MapEpoch 变化：
  Gateway → Full WorldState（建立客户端基线）

每次 mapCache 得到更高 mapVersion 的地图快照：
  Gateway（每地图）比较前后快照
    → 生成一个 MapDelta
    → 向订阅该地图的所有连接 fan-out

客户端：
  校验 mapID、mapEpoch、fromVersion
    → 将 upsert/removal 应用到本地地图索引
    → 推进到 toVersion 并刷新 UI
```

常态同步由“每用户完整状态”改为“每地图生成一次变更 + 向该地图连接广播”。每个连接仍收到自己的网络副本，但 Gateway 不再为每名玩家重复构建和转换相同的地图内容。

### 6.4 协议设计

在 `battle.proto` 中保持当前 `WorldState` 作为全量基线；新增以下类型并作为 `ServerEnvelope` 的新 payload：

```text
MapDelta
  map_id
  map_epoch
  from_version
  to_version
  player_upserts / player_removals
  npc_upserts / npc_removals
  treasure_upserts / treasure_removals
  可选 boss 更新
  可选地图事件

ResyncRequest
  客户端已知 map_id、map_epoch、map_version
```

协议规则：

1. 客户端只在本地 `map_id` 匹配时应用 Delta。
2. `map_epoch` 不同、`from_version` 不等于本地版本、或版本倒退时，客户端停止应用该 Delta 并请求全量重同步。
3. 切图、重连、Topology 导致的 owner 变化、服务端检测到订阅者落后时，服务端主动发送完整 `WorldState`。
4. 状态 Delta 是可合并的；可靠的认证结果、命令确认、错误和重同步响应仍走控制消息通道。
5. `MapEpoch` 继续作为主权代际，不应用旧 owner 的状态到新 owner 的客户端基线。

### 6.5 Gateway 内部结构

新增两个明确的数据结构：

1. **地图快照历史/差分器**
   - 按 `mapID` 保存最近已处理的 `MapView` 和 `mapVersion`。
   - 对更高版本快照建立以实体 ID 为键的索引，生成 upsert 与 removal。
   - 版本相同不产生 Delta；版本倒退或 epoch 变化时丢弃旧历史并强制后续全量同步。

2. **地图订阅表**
   - `mapID -> set<streamSender>`，认证成功后订阅当前地图；切图成功后原子地取消旧订阅、加入新订阅。
   - 连接关闭、Logout、发送失败时清理订阅。
   - fan-out 不得持有订阅表锁调用 `stream.Send`；复制当前订阅者列表后再逐一提交状态。

先复用现有 `streamSender` 的“仅保留最新状态”机制；为 Delta 增加等价的版本检查，避免慢客户端收到倒退状态。若 Delta 无法跨版本合并，则慢客户端应标记为 `resync required`，下一次只发送全量基线而非尝试补齐所有历史帧。

### 6.6 客户端改造

终端客户端维护当前地图的本地实体索引：

- `players[username]`
- `npcs[id]`
- `treasures[id]`
- 当前 `mapID`、`mapEpoch`、`mapVersion`

收到全量 `WorldState` 时重建索引；收到 Delta 时按协议校验后更新索引。UI 从本地索引渲染，不能假定每次服务端消息都携带完整地图。

### 6.7 分阶段落地

| 子阶段 | 内容 | 回滚方式 |
|---|---|---|
| D1 | 为当前全量状态记录帧大小、构建耗时、同地图连接数 | 仅观测，无行为变化 |
| D2 | 新增 protobuf Delta 与客户端本地状态模型；保留全量默认路径 | feature flag 关闭 Delta |
| D3 | Gateway 生成 Delta、维护订阅表；小流量/单地图灰度启用 | 切回全量推送 |
| D4 | 启用缺帧重同步、慢客户端 resync | 切回全量推送 |
| D5 | 默认开启 Delta，并保留强制全量调试开关 | feature flag 回退 |

### 6.8 验收

- 登录、重连、切图、节点 failover 后，客户端均能以全量快照建立正确状态。
- 人为丢弃或打乱 Delta 后，客户端检测版本缺口并完成重同步，不显示错误地图状态。
- 同一地图多玩家场景下，Gateway 的每状态帧 CPU、分配与发送字节数显著下降；具体目标由 D1 基线和 E 阶段压测确定。
- 慢客户端不会造成 Delta 队列无界增长，也不会阻塞其他订阅者的发送。

---

## 7. 阶段 E：压测、故障演练与容量验收

### 7.1 压测场景

扩展 `cmd/benchmark` 或建立等价压测客户端，至少覆盖：

1. 单 Gateway 基线：连接、认证、移动、攻击、状态接收。
2. 多 Gateway：连接在不同实例间分布，验证 Topology 与路由一致。
3. 热点地图：大量玩家进入同一地图并高频移动，比较全量与 Delta 推送成本。
4. 慢客户端：限制接收速度或暂停读取，验证状态合并、resync 和资源上限。
5. Gateway 故障：随机终止一个实例，验证其他实例继续接新连接、客户端退避重连与会话恢复。
6. Node owner 故障：与 Gateway 重连叠加，验证客户端只应用新 MapEpoch 的状态。

### 7.2 发布与回滚策略

- 所有协议和推送变化使用 feature flag；先支持“双栈客户端”，再逐步切换默认路径。
- 多 Gateway 部署先小规模灰度；`readyz`、连接数、错误率和重连成功率是扩容/回滚依据。
- Delta 异常时可立即强制使用全量 `WorldState`，不影响 Node/Coordinator 的主权机制。
- Session Lease 上线前需评估已有 `global_sessions` 的迁移或清理方案，避免旧记录造成登录冲突。

### 7.3 最终验收目标

1. 单 Gateway 实例故障不再造成全站不可接入；新流可被其他 ready 实例接收。
2. 已断线客户端可在可配置时间内自动重连并恢复至当前地图状态。
3. Gateway 可通过增加副本分摊长连接与推送负载，且不改变地图单 owner 写入语义。
4. 热门地图下，常态状态同步不再为每位玩家重复发送完整地图；系统可观测地展示 CPU、内存和网络成本下降。
5. 连接、命令、推送、依赖故障与慢客户端均有明确监控、限流和降级行为。
