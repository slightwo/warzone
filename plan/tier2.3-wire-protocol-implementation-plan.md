# Tier 2.3 — Proto-first 通信协议治理实施计划

> 本文是 Tier 2.3 的独立实施计划，承接 `tier2-remediation-design.md` 中“统一 wire 协议”的方向。
>
> **结论先行**：选择“真 Protobuf”路线。Client ↔ Gateway 使用强类型 `oneof` 流协议；Gateway ↔ Node 保持动作级 unary gRPC，并规范公共消息、时间与错误语义。Redis 继续使用 JSON 作为持久化/协调实现细节。协议边界保留小型、单向 mapper，但删除当前 `protocol/adapter.go` 这种全局、双向、镜像式适配层。

---

## 1. 背景、目标与边界

### 1.1 当前基线

当前工程已经完成 Tier 2.2 的控制面/数据面拆分，通信相关现状如下：

```text
Terminal client / benchmark / admin
    ↕ GatewayService.GameStream(stream pb.Message)
Gateway
    ↕ NodeService（按动作拆分的 unary protobuf RPC）
Node

Redis：Topology、checkpoint、session、event 等以 JSON 持久化或发布订阅
```

现有问题不在于 gRPC 实际传输 JSON，而在于业务模型与 protobuf wire 模型平行维护：

```text
protocol.* 领域/视图模型（附带 json tag）
      ↕ src/protocol/adapter.go：全局双向逐字段转换
pb.* protobuf 传输模型
```

具体表现：

1. `pb.Message` 是以 `type` 字符串分发的扁平字段包，同时承载登录、注册、移动、攻击、错误、状态和管理命令；字段组合无法由类型系统约束。
2. `protocol.Message` 与 `pb.Message` 镜像存在，`ToProtoMessage` / `FromProtoMessage` 让 Client、Gateway、benchmark 和 admin 都经过重复转换。
3. `WorldState`、`MapView`、`UserProfile`、`MapCheckpoint` 等对象在 `protocol` 与 `pb` 中重复定义；新增字段需要同步修改模型、转换器和调用点。
4. `adapter.go` 当前约 437 行，集中承载 Gateway、Node、Client 等不同行为边界的转换，且多数转换无业务语义，仅是机械字段复制。
5. `GameStream` 对命令确认、异步状态推送和错误消息使用同一 `Message`；没有 `request_id`，benchmark 只能把后续任意状态帧近似当作命令完成。
6. `cmd/admin` 复用玩家游戏流发送单次管理消息；而 Gateway 当前只支持只读 status，二者生命周期和鉴权语义不同。
7. NodeService 已按 `MovePlayer`、`Attack`、`Promote` 等业务动作拆分 RPC，结构总体正确；问题主要是其与 `protocol.*` 的重复映射、`MapEpoch` 公共字段重复，以及 checkpoint 时间使用字符串。

### 1.2 目标

Tier 2.3 完成后必须具备：

1. **唯一的线上 RPC 编码**：所有 gRPC 负载均为 protobuf message；禁止以 protobuf `bytes` 偷渡业务 JSON。
2. **强类型客户端流协议**：Client ↔ Gateway 的请求与服务端下行消息通过 `oneof` 表达互斥语义，不再使用 `type` 字符串万能包。
3. **清晰的流语义**：命令确认、异步状态、事件/通知与错误有不同的 payload、可靠性和关联规则。
4. **请求关联**：所有认证及客户端写命令携带 `request_id`，Gateway 返回对应的确认或错误。
5. **可演进性**：V1、V2 在迁移期并行；字段编号不复用；旧客户端淘汰后再删除 V1。
6. **边界映射收敛**：删除 `src/protocol/adapter.go` 与 `protocol.Message`；`protocol` 不再依赖 `pb`。仅在 Gateway、Node 传输边界保留按职责组织的 mapper。
7. **Node RPC 契约规范化**：复用 `MapAuthority`，使用 `google.protobuf.Timestamp`，并将账户敏感字段与节点玩家状态分离。
8. **不破坏 Tier 2.2 不变量**：Topology、MapEpoch fencing、LeaderTerm、Promote → topology/fence/session 原子提交、drain 语义均保持不变。

### 1.3 非目标

本阶段明确不做：

- 不把 Redis JSON 存储改为 protobuf；Redis 的持久化模型不是 gRPC wire protocol。
- 不让 `pb.*` 直接侵入 `world`、`storage` 的核心业务和持久化模型，以换取“零转换”。
- 不把 NodeService 改为大而全的 `oneof` 流；它继续使用动作级 unary RPC。
- 不在本阶段实现完整 delta snapshot、事件重放/WAL、动态副本组、分片或跨语言 SDK。
- 不改变认证、TLS、凭据管理等 Tier 0 安全工作的优先级；2.3 仅确保新协议不暴露 `password_hash`。
- 不承诺断线后自动重放非幂等写命令。重连后必须重新认证、获取新状态；后续若引入自动重试，再单独增加持久化幂等语义。

---

## 2. 架构决策

### 2.1 选择 A：Proto-first，而不是 protobuf `bytes` + JSON

选择 protobuf 作为所有 RPC 的 wire contract，原因：

- 工程已经使用 gRPC、`protoc`、`protoc-gen-go` 与 `protoc-gen-go-grpc`；NodeService 也已是强类型 RPC。
- `oneof` 可在编译期约束一条流消息只能包含一种业务语义。
- protobuf 的字段编号、`reserved`、兼容规则与生成代码适合服务端和未来多语言客户端演进。
- 改用 `bytes` + JSON 只能把转换工作搬到 JSON 编解码，不会解决命令语义、敏感字段边界和版本兼容问题，且会失去 protobuf 契约收益。

### 2.2 必要 mapper 与多余 adapter 的边界

不以“完全没有转换”为目标。以下转换是合理且保留的：

```text
pb.ClientEnvelope / pb.NodeRequest
            ↓
      application command / domain input
            ↓
    world、cluster、storage 业务逻辑
            ↓
      domain read model / result
            ↓
 pb.ServerEnvelope / pb.NodeResponse
```

它们负责：鉴权输入筛选、敏感字段隔离、`MapEpoch` 主权校验、时间类型转换、错误码映射和 API 版本隔离。

需要删除的是“所有对象均维护无差别双向镜像”的全局 adapter。完成后：

- `protocol.Message` 和 `ToProtoMessage` / `FromProtoMessage` 删除；
- terminal client 直接消费 `pb.ServerEnvelope`、`pb.WorldState`，不再把服务端状态转回 `protocol.WorldState`；
- Gateway 仅保留 `protocol.WorldState -> pb.WorldState` 的单向响应映射；
- Node 边界按真实调用方向保留 `pb <->` 领域参数的 mapper；
- `protocol` 包不导入 `pb`，从而不再成为全局传输耦合点。

建议的目录归属（可在实施时微调，以避免 Go import cycle）：

```text
src/transport/gateway/   # V2 流 handler、domain -> pb 下行映射
src/transport/node/      # Node RPC client/server 的 pb <-> domain 映射
src/protocol/            # 不依赖 pb 的领域/视图定义；最终删除 Message 与 adapter.go
```

### 2.3 API 版本与兼容策略

- 保留当前 `GatewayService.GameStream` 作为 **V1**，直至所有内置消费者迁移完成。
- 在同一 `GatewayService` 中新增 `GameStreamV2`，使一个 Gateway 二进制可同时服务 V1 与 V2。
- 新增独立 `AdminService`，用 unary status RPC 替代在游戏流中发送 `TypeAdmin`。
- Node 侧新增 `NodeServiceV2`，与 V1 并行；先升级 Node，再让 Gateway/Coordinator 改用 V2。
- 不复用 V1 `Message` 的字段编号；V2 使用全新 message 名称与 tag 范围。
- 删除 V1 前，所有 `cmd/client`、`cmd/admin`、`cmd/benchmark`、集成测试和部署脚本必须已切换至 V2。
- 删除字段时遵循 protobuf 兼容规则：不复用历史 tag 或字段名；若保留旧 message 做过渡，删除字段后使用 `reserved` 保留 tag 与名称。

---

## 3. V2 协议契约

### 3.1 Client ↔ Gateway：双向流 envelope

`battle.proto` 中新增 V2 结构；以下为目标形态，具体 tag 可在实现前经一次 proto review 固化：

```proto
message ClientEnvelope {
  // 每个认证/写命令在同一登录会话内唯一且非零；服务端回包原样携带。
  uint64 request_id = 1;

  oneof payload {
    LoginRequest login = 10;
    RegisterRequest register = 11;
    QuickEnterRequest quick_enter = 12;
    LogoutRequest logout = 13;

    MoveCommand move = 20;
    AttackCommand attack = 21;
    AttackBossCommand attack_boss = 22;
    HealCommand heal = 23;
    BuyItemCommand buy_item = 24;
    SwitchMapCommand switch_map = 25;
  }
}

message ServerEnvelope {
  // 对认证/命令回包携带原 request_id；主动状态与通知使用 0。
  uint64 request_id = 1;

  oneof payload {
    Authenticated authenticated = 10;
    CommandResult command_result = 11;
    WorldState state = 20;
    ErrorResponse error = 30;
    ServerNotice notice = 31;
  }
}

service GatewayService {
  rpc GameStream(stream Message) returns (stream Message);       // V1，迁移期保留
  rpc GameStreamV2(stream ClientEnvelope) returns (stream ServerEnvelope);
}
```

约束：

1. 一条 envelope 必须且只能设置一个 `payload`；未设置或设置不符合会话阶段的 payload 返回 `INVALID_ARGUMENT` 类错误。
2. 第一个 V2 上行消息必须为 `login`、`register` 或 `quick_enter`；认证成功前拒绝游戏命令。
3. `logout` 成功后，Gateway 发送对应 `CommandResult`（若 stream 仍可写），随后停止该会话。
4. `request_id` 在同一登录会话内由客户端生成且不可重复。V2-A 仅用于关联，不承诺跨断线的幂等重放。
5. `WorldState` 是服务器主动推送，不对应单一命令，`request_id = 0`。
6. 当前 `SessionVersion`、`TopologyVersion`、`MapEpoch` 继续保留在 `WorldState` 中，作为会话、路由和主权可观测字段。
7. `CommandResult` 表示命令已被 Gateway 接受并完成处理；它不替代后续完整 `WorldState` 推送。
8. `ErrorResponse` 用稳定的业务错误 enum、可展示 message、`retryable` 标记组成。内部 gRPC status 不直接透传给终端客户端。
9. `ServerNotice` 用于非状态类消息；地图/用户事件在 V2-A 可继续位于 `WorldState.events`，事件流重放/独立事件协议不属于本阶段。

### 3.2 命令、状态与背压语义

V2 必须明确区分两种下行类别：

| 类别 | 示例 | 是否可合并/丢弃 | `request_id` |
|---|---|---:|---:|
| 可靠控制消息 | `Authenticated`、`CommandResult`、`ErrorResponse`、关键通知 | 否；发送失败即关闭 stream | 对应请求或 0 |
| 最新状态消息 | `WorldState` 周期推送 | 可以合并；积压时仅保留最新一帧 | 0 |

Gateway 的单 stream 写入仍必须只有一个 goroutine。V2 实现应替换当前“所有生产者写同一个有界 `sendCh`”的方式：

- 可靠消息进入有界控制队列；队列满表示客户端消费过慢，应按明确策略关闭 stream，而不能静默丢弃确认/错误。
- 状态消息使用 latest-wins 槽位或可替换队列：当上一帧尚未发送时，用新快照覆盖旧快照。
- 所有下行消息由唯一 sender goroutine 调用 `stream.Send`。
- `WorldState` 由对象池分配时，在 mapper 已构造 protobuf 响应且 sender 不再引用领域对象后归还；不得把池对象交给异步 goroutine 后提前 `FreeWorldState`。

### 3.3 状态版本与客户端应用规则

V2 client 必须保存最后接受的状态版本，避免重连、状态合并或未来多生产者场景中应用过期快照：

1. `SessionVersion` 更小的状态丢弃。
2. 相同 session 内，`MapEpoch` 更小的状态丢弃。
3. 相同 session 与 epoch 内，`Map.Version` 更小的状态丢弃。
4. `MapEpoch` 变大时允许 `Map.Version` 回退，因为新 owner 从 checkpoint 恢复后地图版本可能不同；此时完整替换本地地图视图。
5. `TopologyVersion` 是路由观测值，不独立作为地图状态新旧判断依据：其他地图的拓扑变化也会提升它。
6. 认证成功、MapEpoch 变化或 stream 重连后，客户端以服务器首个完整 `WorldState` 为权威，不保留旧 stream 的局部状态。

### 3.4 Admin 独立契约

当前 `TypeAdmin` 不进入 V2 游戏流。新增只读管理服务：

```proto
service AdminService {
  rpc GetGatewayStatus(GetGatewayStatusRequest) returns (GetGatewayStatusResponse);
}
```

初始范围仅包含现有 Gateway status；节点故障、恢复、Topology 写入仍由 Coordinator 管理接口负责，不借由 Gateway 回流。后续若增加 Coordinator 管理 API，应使用独立 service、认证和审计，不复用玩家流。

### 3.5 Gateway ↔ Node：保持动作级 RPC，规范公共模型

NodeService 不改为流 envelope。V2 的目标是减少重复和修正语义：

```proto
message MapAuthority {
  string map_id = 1;
  uint64 map_epoch = 2;
}

message PlayerState {
  string username = 1;
  string last_map = 2;
  string last_node = 3;
  int32 x = 4;
  int32 y = 5;
  int32 hp = 6;
  int32 max_hp = 7;
  int32 attack = 8;
  int32 potions = 9;
  int32 treasures = 10;
  int32 kills = 11;
  int32 deaths = 12;
  int32 victories = 13;
  bool alive = 14;
}

message MapCheckpointV2 {
  MapAuthority authority = 1;
  int64 version = 2;
  repeated string terrain = 3;
  repeated PlayerView players = 4;
  repeated NPCView npcs = 5;
  repeated TreasureView treasures = 6;
  google.protobuf.Timestamp checkpoint_at = 7;
}
```

要求：

- 所有地图写 RPC 与 `Promote` 组合 `MapAuthority`，继续由 Node `requireAuthority` 和 Redis fence 双重验证。
- `Profile` / `AddPlayer` 等 Node 数据面使用不含 `password_hash` 的玩家状态类型；账户密码哈希不得出现在 Client 或 Node 数据面 protobuf 中。
- `MapCheckpoint` 使用 `google.protobuf.Timestamp`，删除 RFC3339 string 的解析和忽略解析错误的行为。
- 内部 authority 失配继续映射为 gRPC `FailedPrecondition`；参数非法为 `InvalidArgument`；节点不可用为 `Unavailable`。Gateway 将其转换为终端可理解的 V2 错误。
- V2 Node service 与 V1 并行，禁止在混合版本部署时让 Gateway 假设所有 Node 都支持 V2。

---

## 4. 分阶段实施计划

### 阶段 2.3-0：基线、工具链与契约护栏

**目标**：在修改协议前固定当前行为、生成方式和性能基线，避免把协议重构与未知回归混在一起。

**目标文件**：

```text
src/pb/battle.proto
src/pb/battle.pb.go
src/pb/battle_grpc.pb.go
src/cmd/client/main.go
src/cmd/admin/main.go
src/cmd/benchmark/main.go
src/cmd/server/main.go
src/protocol/adapter.go
```

**工作项**：

1. 为当前 V1 `GameStream` 补行为表征测试：首包认证约束、认证失败、状态推送、命令错误、admin status。
2. 记录 benchmark 基线：吞吐、P50/P95/P99、分配次数、GC、Gateway CPU。当前状态 ticker 实际为 100ms，应将其作为基线参数记录。
3. 为 `protocol` 与 `pb` 的现有映射补关键 round-trip/字段覆盖测试；这些测试用于迁移期间识别字段丢失，而非为旧 adapter 永久背书。
4. 固化 protobuf 生成命令和版本。当前生成头记录 `protoc v7.36.1`、`protoc-gen-go v1.36.11`；对应本机编译器命令输出为 `libprotoc 36.1`，二者是同一 Protobuf 发布版本的不同展示形式。CI/开发环境必须同时提供 `protoc-gen-go` 与 `protoc-gen-go-grpc`。
5. 在 `src/` 模块目录执行生成和一致性检查：

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pb/battle.proto
git diff --exit-code -- pb/battle.pb.go pb/battle_grpc.pb.go
```

6. 将 protobuf 兼容性规则纳入 review checklist：只新增字段；不改 tag 类型/含义；不复用删除 tag；删除字段需 reserve tag 与名称。

**完成条件**：

- `go test ./...`、`go vet ./...`、`go build ./...` 全部通过；
- 生成代码可复现且不会出现未提交差异；
- 有可比较的 V1 benchmark / pprof 基线；
- 任何实现 V2 的提交都能与 V1 行为回归对比。

### 2.3-0 执行记录（2026-09-09）

本阶段已完成以下基线与护栏：

1. `src/cmd/server/main_test.go` 通过 bufconn + fake Gateway backend 覆盖 V1 `GameStream` 的首包非认证拒绝、认证失败后关闭流、认证成功后的状态推送、命令错误后保持流可用，以及 admin 单请求流。为此将 Gateway handler 依赖收敛为 `gatewayBackend` 接口，并将生产状态推送间隔显式固定为默认 `100ms`；生产默认行为不变。
2. `src/protocol/adapter_test.go` 覆盖 V1 `Message/WorldState` 和 `MapCheckpoint` 的 protobuf marshal/unmarshal + adapter 往返，重点保护 `SessionVersion`、`TopologyVersion`、`MapEpoch` 与 checkpoint 时间字段。
3. `src/pb/generate.sh` 固化生成命令和版本校验：`libprotoc 36.1`、`protoc-gen-go v1.36.11`、`protoc-gen-go-grpc 1.5.1`。脚本会自动把 `$(go env GOPATH)/bin` 加入 PATH，`--check` 会在生成后验证 `battle.pb.go`、`battle_grpc.pb.go` 无差异。
4. 本地三节点 V1 基准：20 并发用户、15 秒、状态 ticker `100ms`。最终采样结果为 2547 个成功请求、0 错误、169.77 QPS、平均 66.49ms、P50 99.18ms、P95 99.96ms、P99 100.12ms。该延迟分位受全量状态帧 100ms 推送周期限制，且当前 V1 benchmark 以任意后续状态帧近似命令完成时间；2.3-C 迁移 request_id 后应重新采样真实命令确认 RTT。
5. 同一采样窗口 Gateway 的运行时差分为：21 次 GC、45.18MiB 总分配、620549 次 malloc、605914 次 free。CPU profile 的主要热点是 QuickEnter 注册路径的 bcrypt/blowfish（约 53% 累计 CPU），不能将其归因于 wire 协议。分配 profile 差分为 55.35MB，`protocol.ToProtoMapView` 直接/累计约 5MB/11MB、`protocol.ToProtoMessage` 累计约 18.5MB；这为后续收敛镜像 adapter 提供了方向，但不能单独作为性能收益承诺。
6. 已通过 `./pb/generate.sh --check`、`go test ./...`、`go vet ./...`、`go build ./...` 以及 `go test -race ./cmd/server`。基准采集完成后已停止本地 Node、Coordinator 与 Gateway，未保留运行进程。

---

### 阶段 2.3-A：定义 V2 Gateway protobuf 契约

**目标**：新增强类型 V2 协议，不改动 V1 行为，不迁移调用方。

**修改文件**：

```text
src/pb/battle.proto
src/pb/battle.pb.go
src/pb/battle_grpc.pb.go
src/cmd/server/main.go
```

**工作项**：

1. 新增 `ClientEnvelope`、`ServerEnvelope`、认证请求、动作 command、`CommandResult`、`ErrorResponse`、`ServerNotice` 等 V2 message。
2. 在现有 `GatewayService` 新增 `GameStreamV2`；V1 `GameStream` 不删除、不改 tag。
3. 新增 `AdminService.GetGatewayStatus` protobuf 契约，但尚不要求 `cmd/admin` 迁移。
4. 定义稳定错误 enum，例如：`INVALID_REQUEST`、`AUTH_FAILED`、`ROUTE_NOT_READY`、`STALE_ROUTE`、`COMMAND_REJECTED`、`INTERNAL`。禁止把 Go error string 作为客户端判断依据。
5. 定义 `request_id`、首包认证、状态 request_id 为 0、状态版本应用规则和可靠/可合并下行消息的注释，注释随 proto 生成给调用方。
6. 为每种 `oneof` case 增加构造/解析测试；空 payload、认证前命令、未知 enum 应稳定失败。

**完成条件**：

- Gateway 可编译且仍能服务 V1；
- 生成的 Go 类型正确包含 V2 `oneof` wrapper；
- V2 message 不包含 `type`、`action` 等泛化分发字段；
- V2 终端 API 中不存在 `password_hash`；
- V1 与 V2 的 proto tag 互不复用、服务名和 RPC 名明确可共存。

**建议提交**：`feat(protocol): add typed gateway stream v2 contract`

---

### 阶段 2.3-B：实现 V2 Gateway 流与下行背压语义

**目标**：Gateway 同时支持 V1/V2；V2 使用 oneof、请求关联和单 writer / 状态合并机制。

**修改文件**：

```text
src/cmd/server/main.go
src/cluster/cluster.go
src/transport/gateway/（新增，按实际 import 关系落位）
src/protocol/message.go
src/protocol/adapter.go
```

**工作项**：

1. 实现 `GatewayServer.GameStreamV2`：
   - 读取 `ClientEnvelope`；
   - 首包执行 Login/Register/QuickEnter；
   - 将 V2 command 显式分发到现有 `Cluster` 业务方法；
   - 回写带原 `request_id` 的 `Authenticated`、`CommandResult` 或 `ErrorResponse`；
   - 主动状态使用 `ServerEnvelope.state` 和 `request_id = 0`。
2. 引入 V2 专用 sender：可靠控制队列 + latest-state 槽位 + 单一 `stream.Send` goroutine。
3. 只在 Gateway 边界保留 `protocol.WorldState -> pb.WorldState` 单向映射；不得为 V2 client 引入 `pb.WorldState -> protocol.WorldState` 的反向映射。
4. V2 mapper 必须在 protobuf 响应构造完成后归还 `WorldState` 池对象，且不得与 sender goroutine 并发访问池对象。
5. 将 Node authority 错误映射为终端 `STALE_ROUTE` 或可重试路由错误；对于可能已执行的非幂等命令，不得静默自动重发。
6. 当前 V1 `GameStream` 保持原实现和回归测试，便于同一 Gateway 进程滚动升级。

**完成条件**：

- 单 Gateway 可同时接受 V1 和 V2 客户端；
- V2 每一条认证/命令均得到同 request_id 的确认或错误；
- 慢客户端下，状态帧可被合并但认证、命令确认和错误不丢失；
- `MapEpoch` 切换后 V2 客户端收到完整状态且不会接受更旧 epoch 状态；
- V1 测试全部保持通过。

**建议提交**：`feat(gateway): serve typed game stream v2`

---

### 阶段 2.3-C：迁移 Client、Admin 与 Benchmark 消费者

**目标**：仓内所有 Gateway 消费者切换为 V2，消除它们对 `protocol.Message` 和 message adapter 的依赖。

**修改文件**：

```text
src/cmd/client/main.go
src/cmd/admin/main.go
src/cmd/admin/main_test.go
src/cmd/benchmark/main.go
src/cmd/server/main.go
```

**工作项**：

1. `cmd/client`：
   - 使用 `GameStreamV2`；
   - 直接构造 `pb.ClientEnvelope` 的 `oneof` payload；
   - 直接消费 `pb.ServerEnvelope`、`pb.WorldState`，UI 不再保存 `protocol.WorldState` 副本；
   - 使用 protobuf getter，减少对 Go generated struct 公开字段布局的依赖；
   - 为每个用户命令生成非零 request_id，并将确认/错误与本地输入对应；
   - 修正连接目标传递：认证连接必须使用用户选择的 `addr`，不能忽略传入参数而固定连接默认地址。
2. `cmd/admin`：迁移到 `AdminService.GetGatewayStatus`；不再打开玩家双向流，不再发送 `TypeAdmin`。
3. `cmd/benchmark`：
   - 使用 V2 登录和动作 command；
   - 使用 request_id 计算真正的命令确认 RTT，而不是以任意状态帧近似；
   - 单独统计 `CommandResult`、业务错误和状态推送延迟。
4. 用 feature flag 或显式命令行参数保留 V1/V2 选择，直至 Gateway 部署完成；默认切换 V2 前必须完成兼容性冒烟。
5. 增加 CLI 集成测试：V2 登录、移动、状态应用、命令错误、admin status、stream 关闭。

**完成条件**：

- 正式 Client、admin、benchmark 均不调用 `ToProtoMessage` / `FromProtoMessage`；
- admin status 不再通过玩家游戏流执行；
- benchmark 基于 request_id 输出准确命令延迟；
- V2 client 的状态排序规则已由单测覆盖；
- 客户端断线时不自动重放未确认写命令。

**建议提交**：`refactor(client): migrate gateway consumers to stream v2`

---

### 阶段 2.3-D：规范 Node V2 契约并拆分传输 mapper

**目标**：保持 NodeService 动作级设计，清理内部重复传输模型的高风险部分，使 mapper 按 Gateway 与 Node 边界归属。

**修改文件**：

```text
src/pb/battle.proto
src/pb/battle.pb.go
src/pb/battle_grpc.pb.go
src/cluster/node_client.go
src/cluster/grpc_client.go
src/node/grpc_server.go
src/node/node.go
src/transport/node/（新增，按实际 import 关系落位）
src/protocol/adapter.go
```

**工作项**：

1. 新增 `NodeServiceV2` 与 V2 request/response；保留 V1 NodeService，避免混合 Node 版本期间中断 Gateway/Coordinator。
2. 提取 `MapAuthority`，所有地图写操作和 `Promote` 都携带它；Node 仍在 world 修改前执行 authority 缓存与 Redis fence 双校验。
3. 将 `pb.UserProfile` 的 Node 数据面用途替换为不含 `password_hash` 的 `PlayerState` / 受限玩家输入。账户持久化模型仍留在 storage/account 边界。
4. 将 checkpoint 时间改为 `google.protobuf.Timestamp`；解析失败必须作为参数错误返回，禁止继续忽略时间解析错误。
5. 迁移 `NodeGRPCClient`、`NodeGRPCServer`、GatewayNodeClient、CoordinatorNodeClient 至 V2；先升级 Node（同时服务 V1/V2），再升级 Gateway 与 Coordinator 使用 V2。
6. 将现有 adapter 按边界拆开：
   - Gateway：仅保留 domain read model → V2 response 的单向 mapper；
   - Node：只保留 Node RPC 必需的 pb ↔ domain mapping；
   - 删除不再有调用方的反向 mapper。
7. 删除 `protocol` 对 `pb` 的 import；`protocol/adapter.go` 不再新增函数，迁移完成后删除该文件。

**完成条件**：

- Node V2 能完成 Add/Remove/Move/Attack/Heal/Buy/Snapshot/Checkpoint/Promote 等既有 Gateway/Coordinator 调用路径；
- V1、V2 Node 在兼容窗口内均可运行；
- 旧 epoch 写入仍得到 `FailedPrecondition`，Redis fence 测试不回归；
- Node 数据面 protobuf 中没有密码哈希；
- checkpoint 使用 protobuf Timestamp；
- `protocol` 包不导入 `pb`，全局 `adapter.go` 已删除或只剩待删除的零调用代码。

**建议提交**：`refactor(node): normalize typed node rpc v2`

---

### 阶段 2.3-E：淘汰 V1、删除镜像 adapter 并完成性能验收

**目标**：在迁移门槛满足后删除旧万能包和过渡代码，完成协议治理闭环。

**删除/修改文件**：

```text
src/pb/battle.proto
src/pb/battle.pb.go
src/pb/battle_grpc.pb.go
src/protocol/adapter.go
src/cmd/server/main.go
src/cmd/client/main.go
src/cmd/admin/main.go
src/cmd/benchmark/main.go
相关测试与部署脚本
```

**删除前门槛**：

1. 所有生产/本地部署 Gateway 均已支持 V2；
2. 所有 Client、admin、benchmark 已使用 V2；
3. 所有 Node、Coordinator、Gateway 已完成 Node V2 迁移；
4. 已完成至少一个完整发布周期的 V1 无流量观测；
5. 已有 V2 端到端故障切换、drain、重连、慢客户端和压力测试结果。

**工作项**：

1. 删除 `GatewayService.GameStream`、`pb.Message`、`protocol.Message`、仅服务 V1 游戏流的 `Type*` 字符串分发常量，以及 `ToProtoMessage` / `FromProtoMessage`；保留 `protocol/message.go` 中仍作为领域视图使用的类型定义。
2. 删除 V1 NodeService 和其生成/过渡映射；若需保留历史 proto message，显式标记 deprecated 并 reserve 已删除字段 tag/name，禁止复用。
3. 删除 `src/protocol/adapter.go`；确认其功能已由小型边界 mapper 覆盖，而不是把映射复制到业务层。
4. 清理无用的 JSON tag、无用的双向结构和仅为 V1 存在的测试。
5. 使用 `pprof`、benchstat 或等效方式比较 2.3-0 基线与 V2：
   - 端到端命令 P50/P95/P99；
   - Gateway 分配数、GC 暂停、CPU；
   - 全量状态推送吞吐与慢消费者断开率；
   - Node RPC 延迟；
   - protobuf 载荷大小（可选）。
6. 若全量状态帧仍是主要瓶颈，单列后续工作评估增量快照/订阅频率；不得在清理 V1 的提交中混入完整同步模型重写。

**完成条件**：

- 全仓没有 `pb.Message`、`protocol.Message`、`ToProtoMessage`、`FromProtoMessage` 和 `src/protocol/adapter.go`；
- Client ↔ Gateway 完全使用 V2 `oneof`；Gateway ↔ Node 完全使用 NodeServiceV2；
- 所有必要转换均位于命名明确的 Gateway/Node 传输边界，且无 `pb` 对 world/storage 的渗透；
- 2.2 拓扑/epoch/failover/drain 端到端测试保持通过；
- V2 相较基线不存在不可接受的性能退化，且 state 合并策略在慢客户端测试中有效。

**建议提交**：`refactor(protocol): retire legacy wire messages and global adapter`

---

## 5. 发布顺序与回滚

### 5.1 推荐发布顺序

```text
1. 2.3-0：记录基线和生成护栏
2. 2.3-A：Gateway 二进制支持 V1 + V2 契约
3. 2.3-B：Gateway 二进制支持 V1 + V2 运行时
4. 2.3-C：迁移 Client / Admin / Benchmark 到 V2
5. 2.3-D：先升级 Node（V1 + V2），再升级 Gateway / Coordinator 使用 Node V2
6. 观测 V1 无流量、完成故障演练
7. 2.3-E：删除 V1 与全局 adapter
```

### 5.2 回滚原则

- 在 2.3-E 前，Gateway 和 Node 都保留 V1；客户端可切回 V1，因此协议级回滚不需要回滚 Topology 或 checkpoint 数据。
- V2 因实现错误回滚时，停止 V2 client 流量并回退到仍在运行的 V1 RPC；不得改写 `MapEpoch`、fence 或 session 数据来“修复协议”。
- Node V2 rollout 失败时，Gateway/Coordinator 保持 V1 Node client；新 Node 同时提供 V1，不影响既有工作流。
- 仅在确认无 V1 消费者后删除 V1；删除后回滚需重新发布含 V1 的二进制，因此 2.3-E 必须独立提交和独立发布。

---

## 6. 测试矩阵

| 场景 | 2.3-0 | 2.3-A | 2.3-B | 2.3-C | 2.3-D | 2.3-E |
|---|---:|---:|---:|---:|---:|---:|
| protobuf 生成物一致性 | 必须 | 必须 | 保持 | 保持 | 必须 | 必须 |
| proto oneof/非法 payload 单测 |  | 必须 | 保持 | 保持 | 保持 | 保持 |
| V1 Gateway 回归 | 必须 | 必须 | 必须 | 必须 | 必须 | 删除前最后一次 |
| V2 登录/命令/request_id |  | 契约级 | 必须 | 必须 | 保持 | 必须 |
| 可靠消息与状态合并 |  |  | 必须 | 必须 | 保持 | 必须 |
| Client 状态版本排序 |  |  | 必须 | 必须 | 保持 | 必须 |
| Admin status unary |  | 契约级 | 可选 | 必须 | 保持 | 必须 |
| Node authority / fence | 保持 | 保持 | 保持 | 保持 | 必须 | 必须 |
| Checkpoint Timestamp |  |  |  |  | 必须 | 必须 |
| owner failover / 旧主拒写 | 保持 | 保持 | 保持 | 保持 | 必须 | 必须 |
| gateway/node drain | 保持 | 保持 | 必须 | 必须 | 必须 | 必须 |
| benchmark / pprof 对比 | 基线 |  | 初测 | 必须 | 必须 | 最终验收 |

最小命令：

```bash
cd src
go test ./...
go vet ./...
go build ./...
```

需要 Redis 的拓扑、fence、failover 测试必须继续使用可控的临时/注入式依赖，不能依赖开发者常驻 Redis 数据。

---

## 7. 风险与约束

1. **生成代码漂移**：`battle.pb.go`、`battle_grpc.pb.go` 必须只由 `protoc` 生成；禁止手改。生成工具版本应在 CI 固定。
2. **oneof Go 可读性**：Go 生成的 oneof 使用 wrapper 类型，构造和 switch 略繁琐；这是换取协议合法性和演进性的可接受复杂度，应通过 helper/测试减轻，而不是退回字符串 type 分发。
3. **慢消费者**：当前 100ms 全量状态推送会放大队列积压；V2 必须先建立 state coalescing，不能用无限缓冲掩盖问题。
4. **状态池生命周期**：protobuf 响应与 domain `WorldState` 不共享可变底层数据；转换完成与释放顺序必须经过 race test/压力测试验证。
5. **敏感字段泄漏**：迁移中禁止把 `UserProfile.PasswordHash` 复制到 V2 client 或 Node data-plane message；需为账户存储和玩家状态使用不同模型。
6. **兼容窗口过长**：V1/V2 双栈会暂时增加维护成本。每阶段应有明确消费方迁移门槛，避免无限期保留旧协议。
7. **不要用协议重构掩盖业务问题**：MapEpoch、fence、Promote、会话迁移的正确性由现有 2.2 测试保证；通信重构不应改变这些控制面语义。

---

## 8. 最终验收清单

Tier 2.3 完成必须同时满足：

- [ ] Client ↔ Gateway 的正式协议为 protobuf `oneof` V2，不存在 `type` 字符串万能消息。
- [ ] `request_id` 能准确关联认证/命令与确认/错误；状态推送独立且 `request_id=0`。
- [ ] `cmd/client`、`cmd/admin`、`cmd/benchmark` 不依赖 `protocol.Message` 或 message adapter。
- [ ] admin status 使用独立 unary AdminService，不占用玩家 stream。
- [ ] Node 使用动作级 NodeServiceV2、`MapAuthority`、protobuf Timestamp 和无密码哈希的玩家数据模型。
- [ ] `src/protocol/adapter.go` 已删除，`protocol` 不依赖 `pb`。
- [ ] 转换仅存在于 Gateway / Node 传输边界，并按真实方向保留。
- [ ] V1 已在完整兼容窗口后删除，旧 tag/name 未被违规复用。
- [ ] `go test ./...`、`go vet ./...`、`go build ./...` 通过。
- [ ] V2 通过 Client 命令、状态同步、Node fencing、owner failover、drain、慢消费者、重连和基准测试。
- [ ] 与 2.3-0 基线相比，性能没有不可接受退化；若状态全量推送仍为瓶颈，已形成独立的增量同步后续项。

---

## 9. 推荐的第一个编码提交

在进入业务实现前，先完成 **2.3-0 + 2.3-A 的契约部分**，提交边界为：

```text
feat(protocol): add typed gateway stream v2 contract
```

该提交只做以下事项：新增 V2 protobuf message/RPC、生成代码、proto 契约单测与生成一致性检查；不修改 V1 handler、不迁移 Client、不删除 adapter。这样可以在最小风险下先评审 V2 的消息语义、tag 和演进边界，再进入运行时和消费者迁移。
