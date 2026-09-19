# 后续改造实施路线

本文把 [Gateway 高可用计划](GATEWAY_HA_PLAN.md) 和 [World 性能优化计划](WORLD_PERFORMANCE_PLAN.md) 拆成可独立验证的实施步骤。目标是按依赖从简单到复杂推进，而不是一次性引入负载均衡、租约、限流和增量同步。

## 1. 总体原则

1. 每个阶段只解决一个明确问题，完成后必须能独立运行、测试并提交。
2. 不改变现有核心边界：Coordinator 管理 Topology 和故障转移；每张地图只有一个 Owner Node 写入；`MapEpoch` fencing 继续拒绝旧 Owner 写入。
3. 先让 Gateway 接入层可多副本运行，再解决跨副本会话正确性；没有会话租约前，不接入负载均衡器。
4. `session_id` 是 Gateway 与 Node 间的内部 fencing token，不暴露给客户端。Redis 租约、Gateway 本地失效状态和 Node 条件写入共同保证旧流不能在完成接管后继续修改世界。
5. 性能改动必须建立在压测结果上。先记录数据，再限流或减少重复工作；未达到触发条件时停止，不提前实现 `MapDelta` 或位置索引。
6. 每一个新增后台 goroutine 都必须有明确退出条件；每一个 Redis 原子操作及每一种 session fencing 结果都必须有对应单元测试。

## 2. 依赖关系

```text
阶段 0：基线与测试入口
       │
       ▼
阶段 1：Gateway 多实例 + 真实 readiness + 连接计数
       │
       ▼
阶段 2A：TTL 租约 + 登录/退出 + session_id fencing
       │
       ▼
阶段 2B：去除 node_id 会话耦合 + 切图恢复 + 重复登录幂等
       │
       ▼
阶段 3：HAProxy 只转发 ready Gateway 的新连接
       │
       ▼
阶段 4：受控 drain 与故障演练
       │
       ▼
阶段 5：Gateway HA 验收完成
       │
       ▼
阶段 6：性能基线与最小观测
       │
       ├──命令压力明确──────────────────────► 阶段 7：Gateway 轻量命令限流
       │
       ├──快照/编码/推送成本明确────────────► 阶段 8：无变化快照/推送抑制
       │                                               │
       └────相关简单优化后仍有明确瓶颈───────────────┤
                                                       ▼
                                  阶段 9A：共享转换结果，再评估 MapDelta
                                  或阶段 9B：出生点采样，再评估位置索引
```

阶段 1 是后续 HA 的前提；阶段 2A 先建立可过期的会话所有权与旧流 fencing，阶段 2B 再去掉旧会话迁移语义。阶段 3 和阶段 4 都依赖阶段 2B。性能阶段必须在阶段 5 之后开始，避免把接入层不稳定误判为地图性能问题；阶段 7 与阶段 8 是由不同测量结果触发的独立分支，不要求机械地串行实施。

## 3. 阶段 0：建立可重复的基线

### 目标

在改业务逻辑前固定验证入口，确保之后每一步的回归都可判断。

### 最小改动

- 保持 `go test ./...` 作为每一阶段的最低回归检查。
- 保持 `start.sh` 作为本机启动真值来源；需要压测时使用 `cmd/benchmark`。
- 为手工验证记录固定端口和角色：客户端入口、Gateway 后端端口、lifecycle 端口、Node 端口。
- 不修改 Topology、fence、Node drain 或世界锁的语义。

### 验收

- `go test ./...` 通过。
- `./start.sh` 能启动现有单 Gateway 拓扑，客户端能登录、移动、退出。

### 为什么先做

后续步骤会同时修改 Gateway、Redis 和启动脚本。没有固定基线时，很难判断故障来自新逻辑还是本机环境。

---

## 4. 阶段 1：Gateway 多实例、真实 readiness 与连接计数

### 要解决的问题

当前 Gateway 固定监听 `protocol.GatewayAddr`，只能启动一个实例；`/readyz` 只检查 drain 状态，无法作为负载均衡器的可信后端健康信号。

### 前置依赖

- 阶段 0 完成。
- 现有 Gateway 已能够加载 Topology、连接当前 Owner Node、刷新地图缓存。

### 最小改动

#### 4.1 独立实例参数

在 `cmd/gateway` 增加：

- `-addr`：gRPC 监听地址；默认保持 `protocol.GatewayAddr`，保证单机启动方式兼容。
- `-id`：Gateway 实例标识；单机可给一个稳定默认值，本地多实例必须显式传入，例如 `gateway-a`、`gateway-b`。

此阶段 `-id` 只用于日志、lifecycle 状态和后续租约准备，不改变客户端 protobuf，也不写 Redis。

#### 4.2 Gateway 就绪快照

在 `cluster` 中提供只读的 readiness 判断或结构化快照，不让 `cmd/gateway` 直接读取 `Cluster` 私有字段。ready 必须同时满足：

1. Gateway 未 drain；
2. Redis 可用（为 `storage.Store` 增加一个只读 ping/health 方法）；
3. 已加载且校验通过的 Topology；
4. 每张有 Owner 的地图均能找到对应 Node gRPC 客户端；
5. 每张有 Owner 的地图均有与当前 `topology_version + owner_node_id` 一致的最近成功缓存。

首次启动前，第 5 项必须对所有当前 Owner 地图成立。稳态运行时，不因普通刷新失败清空仍与当前 Owner 一致的最后成功缓存；Topology 变更只使受影响地图等待新 Owner 缓存，不应重置无关地图的 ready 状态。此时该 Gateway 可以短暂 not-ready，但不得因为缓存刷新实现而把所有地图或所有副本同时清空。

`/healthz` 只表示进程仍能响应；`/readyz` 使用上述 ready 判断。未 ready 返回 `503`，并在 JSON 状态中保留 `draining` 和 `active_connections`。

#### 4.3 活跃连接计数

在 `GatewayServer.GameStream` 中，仅在认证成功、流真正进入游戏阶段后递增连接数；通过 `defer` 对称递减。认证失败、首帧非法请求不能计入。

连接计数只用于观测和后续 drain，不用于容量限制。

### 不做

- 不启动 HAProxy。
- 不改 `global_sessions`。
- 不改变 `GameStream` 的客户端 protobuf 协议。
- 不在此阶段实现 drain timeout。

### 验收

1. 两个 Gateway 可以直接同时启动：

```bash
go run ./cmd/gateway -id gateway-a -addr 127.0.0.1:9317 -lifecycle-addr 127.0.0.1:9422
go run ./cmd/gateway -id gateway-b -addr 127.0.0.1:9318 -lifecycle-addr 127.0.0.1:9423
```

2. 刚启动、Topology 或当前 Owner 缓存未完成时，`GET /readyz` 返回 `503`。
3. 路由与缓存准备完成后，两个实例各自的 `GET /readyz` 返回 `200`。
4. Topology 更新时，仅受影响地图的缓存重新校验；未受影响地图的有效缓存不会被清空。
5. 客户端直连任意后端端口均可登录；认证成功后状态中的 `active_connections` 增加，断开后恢复。
6. 为 readiness 条件和连接计数补充单元测试。

### 为什么阶段 2A 依赖它

TTL 租约需要写入 `gateway_id`，而 HAProxy 必须依赖可信的 `/readyz`。先建立实例身份和 readiness，后续逻辑不会反复改接口。

---

## 5. 阶段 2A：TTL 会话租约与 Gateway/Node fencing

### 要解决的问题

当前 `global_sessions` 是 Redis Hash，字段不能独立过期。Gateway 崩溃后在线记录永久存在；多个 Gateway 并存时，旧流也可能删除、续活或继续操作新会话。

本阶段只建立会话所有权、登录/退出和命令的 fencing，不同时移除 `node_id` 会话耦合或重写切图恢复流程。

### 前置依赖

- 阶段 1 完成，Gateway 具备稳定 `-id`。
- 先确定租约节奏，建议初值：TTL 为 15 秒，续租间隔为 5 秒，并预留一个小于 TTL 的本地安全边际。

### 最小改动

#### 5.1 调整运行时配置

在 `gateway` 配置增加：

- `session_lease_ttl`，初值 `15s`；
- `session_lease_renew_interval`，初值 `5s`；
- `session_lease_safety_margin`，用于在本地保守判断租约是否仍可发送命令。

配置校验必须保证续租间隔和安全边际均小于 TTL，且续租间隔加安全边际小于 TTL。

#### 5.2 新的存储模型和原子 API

用每用户独立 Key 替代 `global_sessions` Hash：

```text
battle:session:<username>
{
  "username": "...",
  "session_id": "随机且不可预测",
  "gateway_id": "gateway-a",
  "map_id": "green",
  "version": 1
}
TTL = 15s
```

存储层只暴露意图明确的方法，不让 `cluster` 直接拼 Redis 命令：

- `TryCreateSessionLease`：仅 Key 不存在时创建；必须原子执行。
- `LoadSessionLease`：读取并反序列化。
- `RenewSessionLease`：只有 `session_id` 匹配才续 TTL。
- `DeleteSessionLease`：只有 `session_id` 匹配才删除。

上述条件操作使用 Lua 或等价 Redis 原子操作。调用者必须区分成功、`session_id` 不匹配、Key 不存在和存储错误；每种结果都需要测试。地图更新 API 留到阶段 2B，避免在第一个租约步骤同时改动切图事务。

#### 5.3 让 session_id 成为内部 fencing token

不修改客户端 `GameStream` protobuf。认证成功后，`GameStream` 在进程内保存 `username`、`session_id`、`gateway_id` 与保守的本地租约截止时间。成功创建或续租后才推进截止时间；续租发现不匹配、Key 不存在，或本地截止时间已到时，流立即标记为失效、停止状态推送并关闭。暂时性 Redis 错误不能无限延长本地截止时间。

Gateway 与 Node 的内部 RPC 增加 `session_id`，至少传入 `AddOrRestorePlayer`、所有会改变世界状态的命令和 `RemovePlayer`。Node 为在线玩家保存当前 `session_id`：

1. `AddOrRestorePlayer` 成功时以本次 token 认领该玩家；新流在认领完成前不得接收游戏命令。
2. Node 仅执行 token 与当前玩家 token 相同的命令和 `RemovePlayer`；不匹配统一返回会话已失效，绝不修改世界或删除玩家。
3. Gateway 在本地流已失效时也直接拒绝命令，不再发起 Node RPC。

Node 成功认领新 token 是旧/新流的实际控制权切换点。Redis 创建到 Node 认领完成之间存在必要的跨进程调用窗口：新流在此窗口不接收命令，旧流最多只能在其原 token 仍被 Node 认可时完成已到达的操作；认领完成后 Node 必须拒绝任何旧 token。该边界要通过并发测试明确，而不能把 Gateway 侧检查误称为无窗口的全局事务。

#### 5.4 调整登录与退出顺序

**登录：**

1. 验证账号密码；
2. 根据最新 Topology 找到地图 Owner 与 `MapEpoch`；
3. 生成 `session_id`，原子创建租约；
4. 使用同一 `session_id` 调用 Node `AddOrRestorePlayer`，完成 Node 侧认领；
5. Node 调用失败时，只删除匹配本次 `session_id` 的租约；
6. 认领成功后才启动本流续租、接受游戏命令并返回初始状态。

**正常退出或流关闭：** 先停止本流续租和命令接收；随后以 `session_id` 调用 Node 的条件 `RemovePlayer`，再条件删除 Redis 租约。两步任一出现不匹配都只说明所有权已转移，旧流不得继续删除、保存或清理后继会话的数据。

### 不做

- 会话迁移、重连 token、流内迁移；
- 根据 TTL 到期主动扫描并从 Node 删除玩家；
- 把 `session_id` 暴露给客户端；
- 扫描 Redis 全量会话；
- 在本阶段移除会话中的 `node_id` 或改写 Coordinator 会话迁移。

### 验收

1. 同账号在两个 Gateway 上并发登录，只有一个成功。
2. Gateway A 进程被杀死后，等待 TTL 到期，可经 Gateway B 重新登录。
3. Gateway A 的旧流不能删除、续期或覆盖 Gateway B 的租约。
4. Node 完成 B 的 token 认领后，A 的旧 token 命令和 `RemovePlayer` 必须被拒绝，且不能修改 B 的实时实体。
5. 覆盖 Redis 原子 API、登录失败回滚、流失效、Node token fencing 及退出顺序的测试。

### 为什么阶段 2B 依赖它

阶段 2B 要调整 Node 路由、切图和 Coordinator 会话迁移。先把用户所有权和旧流 fencing 独立验证，能够将之后的拓扑变化问题与基础会话正确性问题分开定位。

---

## 6. 阶段 2B：去除 NodeID 会话耦合、切图恢复与重复登录幂等

### 要解决的问题

会话租约不应保存或依赖旧 `node_id`。Coordinator failover 后，命令应当由最新 Topology 路由；切图或重新登录不能创建重复实体，也不能用旧 Profile 覆盖实时状态。

### 前置依赖

- 阶段 2A 完成，Redis 与 Node 均已能按 `session_id` fencing。
- 当前 `AddOrRestorePlayer`、切图和 Coordinator 会话迁移调用点已被列出并具备回归测试入口。

### 最小改动

#### 6.1 移除会话 NodeID 路由

新的租约不保存 `node_id`。每条命令都通过最新 Topology 查询地图 Owner 与 `MapEpoch`；`MapEpoch` 继续是旧 Owner 写入的权威保护。

同步移除或改写：

- `CompareAndSaveTopologyAndMigrateSessions*`；
- Coordinator 的会话迁移调用与测试替身接口；
- 基于会话 `NodeID` 路由的逻辑。

#### 6.2 条件更新租约与切图恢复

增加 `UpdateSessionLeaseMap`：仅当 `session_id` 匹配时更新 `map_id/version`，并保留原剩余 TTL。该操作同样必须原子，并区分成功、不匹配、Key 不存在和存储错误。

同一流内的切图必须串行：切图开始后拒绝该流的第二个切图请求与普通世界命令，直到完成或流失效。

1. 用最新 Topology 找到目标地图 Owner 与 `MapEpoch`；
2. 使用同一 `session_id` 完成 Node 侧切图/认领；
3. 条件更新租约的 `map_id/version`；
4. 成功后解除该流的切图阻塞。

如果第 3 步返回暂时性存储错误，保持切图阻塞并在当前 TTL 内有限重试或重新读取同一 `session_id` 的租约；不得在状态不明时继续接受命令。如果发现 token 不匹配或 Key 不存在，立即使旧流失效。如果 Node 已完成切图而租约最终无法更新，新登录的恢复逻辑必须以当前租约和最新 Topology 调用幂等的 `AddOrRestorePlayer`，返回已存在实体的实时视图，而不是以旧 PostgreSQL Profile 覆盖它。

#### 6.3 保证 Node 侧重复登录幂等

Gateway 崩溃后，旧玩家实体可能仍暂留在 World 中。`AddOrRestorePlayer` 遇到同名在线玩家时，必须在 token fencing 下接管并返回该实体的实时视图，不得创建第二个实体或用旧 PostgreSQL Profile 覆盖它。

本阶段只解决“租约过期后可重新登录、不会产生重复玩家或覆盖实时状态”。不额外实现离线玩家定时清理；若之后观察到长期残留实体，再单独设计清理策略。

### 不做

- 会话迁移、重连 token、流内迁移；
- 根据 TTL 到期主动从 Node 删除玩家；
- 把 session_id 暴露给客户端；
- 扫描 Redis 全量会话。

### 验收

1. Node failover 后，命令总是从最新 Topology 找 Owner，不依赖会话中的旧 NodeID。
2. 同一流切图期间的并发命令被拒绝或等待，不会在旧/新地图交叉写入。
3. Node 侧切图成功而 Redis 更新失败时，旧流停止；重新登录不会创建重复玩家或回退实时位置、生命、物品。
4. Coordinator 不再迁移玩家会话，相关存储 API、调用和替身测试已删除或改写。
5. 覆盖切图成功、Node 失败、租约不匹配、Redis 暂时失败与重复登录的测试。

### 为什么阶段 3 依赖它

HAProxy 会让新连接随机落到不同 Gateway。只有会话不再携带旧 NodeID、旧流已经被 fencing，并且失败后的重新登录可恢复实时实体时，负载均衡才不会放大故障切换中的错误。

---

## 7. 阶段 3：HAProxy 接入多个 Gateway

### 要解决的问题

阶段 1 至 2B 证明 Gateway 能够多实例运行且会话正确，但客户端仍需手工选择后端端口；此阶段提供统一入口并让新连接只进入 ready 副本。

### 前置依赖

- 阶段 1 的 `/readyz` 已覆盖真实依赖；
- 阶段 2A、2B 的 TTL 会话租约、session fencing 和路由改造已完成；
- 两个 Gateway 已可独立运行。

### 最小改动

1. 增加一个本地 HAProxy 配置，前端监听 `127.0.0.1:9310`，后端是：
   - Gateway A：`127.0.0.1:9317`，HTTP 健康检查显式访问 `127.0.0.1:9422/readyz`；
   - Gateway B：`127.0.0.1:9318`，HTTP 健康检查显式访问 `127.0.0.1:9423/readyz`。
2. 数据面保持 TCP 转发，兼容 gRPC 长连接；健康检查使用 HTTP `GET /readyz`，不能向 gRPC 后端端口发送 HTTP 检查。
3. 调整 `start.sh` 和 `stop.sh`：启动并管理 Node、Coordinator、Gateway A、Gateway B、HAProxy。Gateway 一律显式传入 `-addr`，避免默认地址与 HAProxy 的 `9310` 冲突；客户端继续使用 `protocol.GatewayAddr` 的 `9310`。
4. 将 HAProxy 配置和启动命令限制在本地验证范围；不实现服务发现、双 HAProxy 或 VIP。

### 验收

1. 客户端只连接 `127.0.0.1:9310`，可以正常进入游戏。
2. 关闭任一 Gateway 后，新的连接仍能进入另一实例。
3. 两个后端都不 ready 时，入口无法建立新游戏流；日志可说明没有健康后端。
4. 不要求已有 gRPC 流自动迁移。

### 为什么阶段 4 依赖它

Drain 的价值在于“负载均衡器不再分配新连接”。没有真实负载均衡器，只能验证标记变化，不能验证流量切走。

---

## 8. 阶段 4：受控 drain 与 Gateway 故障演练

### 要解决的问题

当前 Gateway `/drain` 会立即调用 `GracefulStop`，不符合“先摘流量、再等待已有流自然结束”的最小语义。

### 前置依赖

- 阶段 1 的活跃连接计数；
- 阶段 3 的 HAProxy readiness 检查。

### 最小改动

1. `POST /drain` 立即设置 `draining=true`，使 `/readyz` 返回 `503`。
2. HAProxy 检测到未 ready 后停止向该 Gateway 分配**新**连接。
3. Gateway 等待 `active_connections` 归零，或等到一个固定的 `gateway.drain_timeout`。
4. 连接归零时调用 `GracefulStop`；超过 timeout 时调用 `Stop` 强制关闭，客户端获得断开并按既有方式重新登录。
5. 删除或替换当前 SIGTERM 直接 `GracefulStop` 的分支。SIGTERM 与 `/drain` 必须进入同一个 drain 状态机，避免绕过摘流、计数与 timeout。

新增 `gateway.drain_timeout` 配置并测试其大于零；本地可先使用 30 秒。

### 验收

1. 对 Gateway A 调用 `/drain` 后，其 `/readyz` 立即为 `503`。
2. 新客户端经 `9310` 只进入 Gateway B。
3. A 上已有流在 timeout 前可继续操作；连接结束后 A 正常停止。
4. timeout 到达时，A 被强制停止；客户端断开后可经 B 重新登录。
5. 对 A 发送 SIGTERM 时，观察到与 `/drain` 相同的摘流、等待和超时行为。
6. 验证时同时观察 `active_connections` 的变化。

---

## 9. 阶段 5：Gateway HA 最终验收与停止点

### 目标

确认“接入层可切换”已经闭环，不继续把范围扩大为无感迁移。

### 验收场景

| 场景 | 预期结果 |
|---|---|
| 两个 Gateway 均 ready | 客户端通过 `9310` 正常登录并游戏 |
| Gateway A 直接崩溃 | A 上流断开；Node、Coordinator、B 保持运行；TTL 到期后可通过 B 重新登录 |
| Gateway A drain | A 不接收新连接；已有流按 drain 规则结束；新连接进入 B |
| Node owner failover | 仍由 Coordinator + Topology + MapEpoch 接管；会话不迁移，后续命令按新 Topology 路由 |
| 旧流晚到的退出/续租 | 不会删除或续活新 session_id 对应的会话 |
| B 完成玩家认领后的 A 旧命令 | Node 以旧 token 拒绝，不修改新会话的玩家实体 |
| 切图中 Redis 更新失败 | 旧流停止；后续登录恢复实时实体，不重复创建或覆盖旧状态 |

### 停止条件

上述场景通过后，Gateway HA 视为完成。不要在本项目继续加入自动无感重连、会话粘滞、流内迁移、双入口负载均衡或完整监控平台。

---

## 10. 阶段 6：性能基线与最小观测

### 要解决的问题

在没有数据前，无法判断瓶颈是 Gateway、Node RPC、World 地图锁，还是重复状态推送。

### 前置依赖

- 阶段 5 完成，接入层行为稳定；
- 已能使用 `cmd/benchmark` 构造热点地图压力。

### 最小改动

仅增加日志或内存计数，不接 Prometheus：

- 按地图、命令类型统计 Gateway 接收量、拒绝量、耗时；
- 记录 Node RPC 耗时与错误数；
- 记录 World 命令和 Tick 的执行耗时；
- 区分由状态泵和命令触发的 `SnapshotFor` 调用次数与累计耗时；
- 记录 `ToWorldState` 转换次数与累计耗时；
- 输出每张地图的玩家数、快照刷新次数、实际发送的 State 帧数与发送累计耗时；
- 预留 `rate_limit_hits`、`state_frames_suppressed` 计数，即使初始值为零。

服务端汇总记录 `count + total + max`，按固定周期（例如 10 秒）打印，避免每请求打日志。客户端端到端 p95 继续由 `cmd/benchmark` 在相同场景下计算，不在服务端提前引入直方图。

### 验收与决策

- 用相同用户数、地图、命令模型和持续时间至少执行两次压测；
- 记录吞吐、客户端 p95 延迟、错误数、地图 tick 耗时、`SnapshotFor`/转换/发送的次数与耗时；
- 只有观测表明高频命令是主要压力时，才进入阶段 7；只有观测表明快照构建、protobuf 转换或状态发送是主要压力时，才进入阶段 8；否则先停止性能改造。

---

## 11. 阶段 7：Gateway 轻量命令限流

### 要解决的问题

单个客户端高频发送移动或攻击，可能占用同一地图串行写入时间，影响其他玩家。

### 前置依赖

- 阶段 6 已确认高频命令是问题的一部分；
- 限流只在 Gateway 内存中生效，允许不同 Gateway 的独立限额，接受这一学习项目级别的近似。

### 最小改动

1. 将限流器作为每个认证后 `GameStream` 的局部对象，仅按 `commandType` 记录最近一次**被接受**的时间。
2. 只限制会改变世界状态的命令；认证、状态推送和 Admin 请求不受影响。
3. 初始阈值：移动 100ms、攻击 300ms、治疗/购买 1 秒；将值放入运行时配置。
4. 超限请求在 Gateway 直接返回稳定错误码，不调用 Node RPC。
5. 限流器随流创建、随流结束；重连后窗口重置。无需全局 `username` map、定期清理 goroutine 或跨 Gateway 协调。

### 验收

- 高频移动请求的 Node RPC 数明显下降；
- 正常操作节奏不受影响；
- 一个用户被限流不会阻塞其他用户；
- 限流命中数出现在阶段 6 的汇总日志中。

---

## 12. 阶段 8：减少无变化快照与重复推送

### 要解决的问题

当前 Gateway 周期性刷新地图并推送完整状态。地图没有变化时，仍可能重复构建、编码和发送相同内容。

### 前置依赖

- 阶段 6 已显示快照构建、编码或网络推送是主要开销；
- 阶段 1 的地图缓存“首次加载完成”状态可复用。

### 最小改动

1. 地图缓存的身份至少由 `topology_version + owner_node_id + map_version` 组成。只有身份不变时才复用缓存；Owner 切换即使恰好得到相同 `map_version`，也必须重新加载，不能把旧 Owner 的视图当成新缓存。
2. `gateway.map_snapshot_refresh_interval` 已是配置项。本阶段只根据阶段 6 数据逐步调大并复测，不新增另一套刷新配置。
3. 为每个游戏流维护上一次**写入成功**的轻量状态指纹。指纹至少包含 `session_version`、`map_epoch`、`topology_version`、地图可见状态版本、Boss/全局状态版本和用户事件序列号。
4. 用户事件目前包含在 `WorldState.Events`，不能被当作独立的可靠控制消息。为入队事件增加单调事件序列号；事件序列号变化时，即使地图版本不变也必须发送状态帧。写失败或被发送队列合并时不得丢弃未确认事件，只有成功写入后才能更新该流的已发送指纹。
5. 抑制判断必须发生在 `ToWorldState` 之前，基于缓存元数据、会话版本和事件序列号完成；否则完整 protobuf 转换成本已经发生，优化没有意义。
6. `MapView.Version` 不是“玩家静止”的充分条件：`BackgroundStep` 的 NPC 移动可能持续推进版本。阶段 8 不做内容哈希，也不把 `NodeCounts` 作为唯一正确性判据；若压测显示 NPC 版本噪声使抑制几乎不命中，记录原因并停止在此优化，后续仅能以专门设计的可见状态版本另立改造。

### 验收

- 静止且无 NPC/全局事件变化的场景中，地图快照构建、protobuf 编码和状态帧数量下降；
- Owner failover、移动、攻击、切图、Boss 状态和用户事件不会因缓存复用或抑制丢失；
- NPC 持续移动场景若无法安全抑制，指标应如实反映“零或很少命中”，不得为了命中率跳过状态；
- 客户端状态排序测试与新增的“无变化不重复推送、事件必达、Owner 变更强制刷新”测试均通过。

---

## 13. 阶段 9：只在压测触发时选择一个深入优化

阶段 9 不是默认工作项。先根据阶段 6 的数据选择一个方向，不要同时实现。

### 9A：先共享转换结果，再评估简化 MapDelta

**触发条件：** 完整地图状态的 protobuf 转换或编码仍是主要瓶颈，且阶段 8 已无明显改善。

**第一步（更便宜的中间方案）：** 对同一 Gateway、同一地图、同一地图身份的公共部分复用已转换的 protobuf 对象或编码结果；玩家私有状态和事件仍逐流构建，绝不能复用或串给其他用户。若现有消息结构无法安全拆出公共部分，不为共享缓存强改协议，记录结果后再决定是否进入 Delta。

**只有第一步仍不足时的最小 Delta 方案：**

- 登录、重连、切图发送完整 `WorldState`；
- 常态只发送实体变更；
- 不保存 Delta 历史，不做补发和复杂重放；
- 客户端发现版本不连续时必须有恢复路径：每固定时间间隔或每固定数量的 Delta 强制发送完整状态，而不是无限等待下一次偶然的完整帧。

### 9B：先采样出生点，再评估位置索引

**触发条件：** World 锁内大部分时间耗费在遍历玩家/NPC/宝物以判断格子占用、查找附近目标或选择 NPC/宝物出生位置。

**第一步（更小的优化）：** 若 profile 确认热点是出生点全量扫描，随机采样 64～128 个候选格子；采样未命中时回退现有完整扫描，保证不改变出生正确性。

**只有第一步仍不足时的最小位置索引方案：**

- 在 `World` 内维护格子到实体的占用索引；
- 登录、移动、死亡、复活、退出、NPC 刷新/死亡、宝物变化时在同一把地图锁下同步更新；
- 保持地图级锁，不增加玩家锁、格子锁或 Zone Actor。

### 验收

- 用与阶段 6 相同的压测场景对比优化前后数据；
- 先验证中间方案是否已达成目标，再决定是否实施 MapDelta 或位置索引；
- 若无明确改善，回滚或停止继续复杂化。

## 14. 明确不进入路线的方案

以下方案不属于当前学习项目的实施范围：

- Zone Actor、地图分区并发、玩家级/格子级多锁；
- 分布式事务、事件溯源；
- 自动无感重连、流内迁移、重连 token；
- 会话粘滞、会话迁移、Delta 历史补发；
- Prometheus 全套监控、服务发现、Kubernetes 编排、双 HAProxy/VIP。

如果未来要实施其中任一项，应先新增单独的设计文档，说明现有方案为什么不够，以及压测或故障数据证明了什么。
