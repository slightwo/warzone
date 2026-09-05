# Battleworld — 修复优先级与实施计划

> 本文档与 [DESIGN_REVIEW.md](DESIGN_REVIEW.md) 配套。DESIGN_REVIEW.md 负责「问题是什么」，
> 本文档负责「按什么顺序修、为什么这个顺序」。
>
> 排序综合三个维度：**影响（严重度）× 成本（修复难度）× 逻辑依赖（先后顺序）**，
> 不简单照搬「按严重度从高到低」。

---

## 一处对 DESIGN_REVIEW.md 的修正

§1.3 的表述「`NodeGRPCClient` 含 no-op 桩」不够精确，且会误导修复方向：

- `battle.proto` 里 `NodeService` **根本没有** `InstallPrimaryMap` / `RestorePrimaryMap` 这两个 RPC，
  所以那两行空实现不是「没写」，而是「无从写」。
- 影响面其实没那么大：节点在 [cmd/node/main.go](src/cmd/node/main.go#L49-L65) 启动时会**本地自行加载主地图**。
- 真正断掉的是「故障恢复后从 Redis checkpoint 重建世界」这条路径：节点重启只会 `world.NewWorld(cfg)`
  起一张空图。注意协调器的 `nodeDiscoveryLoop` **确实会** `LoadCheckpoint`（[cluster.go:739](src/cluster/cluster.go#L739)），
  但随后的 `client.RestorePrimaryMap` 是 no-op 桩（[grpc_client.go:205](src/cluster/grpc_client.go#L205)），
  恢复被静默丢弃——checkpoint 从未真正回到世界状态。

**正确修法**：让节点自己从 Redis 拉 checkpoint（而不是补两个 RPC，那反而又回到「协调器推动」的老路）。

---

## 修复梯队总览

| 梯队 | 定位 | 内容 |
|------|------|------|
| 0 | 立刻做：独立、便宜、高确定性 | 安全三件套、KEYS→SCAN、清理残留、错误可见化 |
| 1 | 铺路：中成本，为架构重构做准备 | 节点自拉 checkpoint、对象池修复 |
| 2 | 架构重构：高成本、高价值、严格先后 | 节点自治 → 拆分控制面 → 统一协议 |
| 3 | 深化：非阻塞，按需 | 事件重放、Version 语义、动态分片 |

---

## 第 0 梯队 — 立刻做（清障）

独立、便宜、高确定性，且大部分是 🔴 硬伤。

| 序 | 问题 | 影响 | 成本 | 关键位置 |
|----|------|------|------|---------|
| 0.1 | §2.2 安全三件套：bcrypt/argon2 替换裸 SHA-256、凭据改 env/config、gRPC 加 TLS | 🔴 高 | 低 | `hashPassword` [store.go:259](src/storage/store.go#L259)、DSN [store.go:100](src/storage/store.go#L100)、`insecure.NewCredentials()` [grpc_client.go:27](src/cluster/grpc_client.go#L27) |
| 0.2 | §2.3 `KEYS` → `SCAN` | 🟠 中 | 极低 | [store.go:279](src/storage/store.go#L279) |
| 0.3 | §3.3 清理教学残留（`studentTodoNotice`、`[debug]`、`// to understand`、`TODO(Labc…)`） | 🟡 低 | 极低 | 全仓库 |
| 0.4 | §3.4 处理被吞错误 | 🟠 中 | 低 | `CP, _ :=` [cluster.go:948](src/cluster/cluster.go#L948)、`Promote` 丢弃返回值 [cluster.go:994](src/cluster/cluster.go#L994) |

**为什么 0.4 也放进「立刻做」**：它是第 2 梯队故障转移重构的前置——不先让错误可见，
后续重构无法验证正确性。

---

## 第 1 梯队 — 铺路（中成本）

| 序 | 问题 | 影响 | 成本 | 说明 |
|----|------|------|------|------|
| 1.1 | §1.3 节点启动时从 Redis **自拉 checkpoint 恢复**（不是补 RPC） | 🔴 高（故障恢复正确性） | 中 | 在 [cmd/node/main.go](src/cmd/node/main.go) 初始化处加「有 checkpoint 就 `RestorePrimaryMap`，否则 `InstallPrimaryMap`」。**也是 §1.2 的前置**。注意 `NodeClient` 接口的 `InstallPrimaryMap`/`RestorePrimaryMap` **并非死方法**，而是被 [cluster.go:740/742](src/cluster/cluster.go#L740-L742) 调用的空转桩；节点自拉后这条 push 恢复路径变冗余，删接口方法＋调用点应归入 2.2 |
| 1.2 | §3.2 `WorldState` 池泄漏/欠重置 | 🟡 低-中 | 低 | `SnapshotFor` 早退路径加 `defer FreeWorldState`，`FreeWorldState` 补标量字段清零 |

---

## 第 2 梯队 — 架构重构（高成本、严格先后）

| 序 | 问题 | 影响 | 成本 | 依赖 |
|----|------|------|------|------|
| 2.1 | §1.2 节点自治：`BackgroundStep()` 移入节点自身 tick loop | 🔴 高 | 高 | **必须先做**——不先让节点自 tick，协调器的 `backgroundLoop` 就是它目前唯一驱动世界的方式 |
| 2.2 | §1.1 拆分控制面/数据面：协调器独立进程 + 网关降级为无状态代理 | 🔴 高 | 很高 | **必须在 2.1 之后**——节点自治后，`backgroundLoop`/`checkpointLoop`/`heartbeatLoop` 才能从网关进程摘出 |
| 2.3 | §2.1 统一 wire 协议 | 🔴 高 | 很高（~430 行 adapter 重写） | 独立可做，但**建议排在 2.2 之后**，否则架构一变协议又得改一次 |

---

## 第 3 梯队 — 深化/增强（非阻塞）

| 序 | 问题 | 影响 | 成本 |
|----|------|------|------|
| 3.1 | §2.4 事件系统加重放/持久化 | 🟠 中 | 中-高 |
| 3.2 | §3.1 统一 `Version` 语义（或删掉死字段） | 🟡 低 | 中（需想清楚语义） |
| 3.3 | §1.4 动态分片/负载迁移 | 🟠 中 | 很高（demo 阶段可不做） |

---

## 依赖关系图

```
第 0 梯队（全部独立、可并行）
   │
   ├── 0.4 错误可见化 ──┐
   └── 0.1/0.2/0.3 ─────┘（互不依赖）
        │
第 1 梯队
   └── 1.1 节点自拉 checkpoint ──> 是 2.1 的前置
   └── 1.2 对象池修复（独立）
        │
第 2 梯队（严格串行）
   2.1 节点自治 ──> 2.2 拆分控制面 ──> 2.3 统一协议
        │
第 3 梯队（随时可做，不阻塞任何事）
```

---

## 一句话结论

> 先清障（第 0 梯队全是独立、便宜、🔴 安全的硬伤）→ 铺路（节点自拉 checkpoint）→
> 再动架构（先节点自 tick，后拆控制面，协议统一压轴）。

与 [DESIGN_REVIEW.md](DESIGN_REVIEW.md) 末尾「按严重度排序」的最大差异：把安全、KEYS、错误处理
这些「便宜且独立」的 🔴 提前了，而不是让它们排在昂贵架构重构之后——它们不阻塞任何事，
但每多留一天都是面试里第一个被问爆的点，而架构重构本身需要先完成第 1 梯队的「错误可见化」
与「节点自恢复」才能安全推进。
