# 第 1 梯队 — 修改方案设计

> 对应 [REMEDIATION_PLAN.md](../REMEDIATION_PLAN.md) 第 1 梯队（1.1 节点自拉 checkpoint、
> 1.2 对象池修复）。本文档是「怎么做」的具体设计，供实施前评审。
>
> 现状定位基于分支 `tier1-remediation` 的代码快照核实。

---

## 总览与实施顺序

两项互相独立，可并行。但 1.2 成本极低、确定性高，建议先做；1.1 是第 2 梯队「节点自治」的前置。

| 序 | 内容 | 影响 | 成本 | 建议顺序 |
|----|------|------|------|---------|
| 1.2 | WorldState 对象池泄漏 / 欠重置 | 🟡 低-中 | 极低 | 先做 |
| 1.1 | 节点启动自拉 checkpoint 恢复 | 🔴 高（故障恢复正确性） | 中 | 后做 |

---

## 1.2 WorldState 对象池修复

### 现状（已核实）

`protocol.WorldState` 走 `sync.Pool`（[message.go:254](../src/protocol/message.go#L254)）。两个问题：

**问题 A：早退路径泄漏**。`SnapshotFor`（[cluster.go:458](../src/cluster/cluster.go#L458)）在
`AllocWorldState()` 之后有两条 `return nil, err` 路径没有归还对象：

| 位置 | 触发条件 | 可达性 |
|------|---------|--------|
| [cluster.go:517](../src/cluster/cluster.go#L517) `if node == nil` | 承载节点不可用 | 防御性，几乎不可达（`sessionNode` 两个分支在返回前都已校验 `node != nil`） |
| [cluster.go:530](../src/cluster/cluster.go#L530) `if !mapOk \|\| cachedCurrent.View == nil` | 地图缓存尚未填充 | **真实泄漏点**（节点刚上线、`mapCacheLoop` 未跑完的窗口期） |

> 补充核实：`FreeWorldState` 的三处调用（[server main.go:74/94/174](../src/cmd/server/main.go#L74)）
> 都在同步 `stream.Send` 返回之后，此时对象不再被引用，归还池子是安全的。调用时机本身无问题。

**问题 B：`FreeWorldState` 重置不彻底**（[message.go:264](../src/protocol/message.go#L264)）。它只截断了
切片（`Maps/Nodes/Events/Map.Terrain/Map.Players/Map.NPCs/Map.Treasures/Boss.Sites`），没有清零标量字段：
`Self`（`PlayerView` 全标量）、`Map` 的 `ID/Name/NodeID/Width/Height/Version`、`Boss` 的
`Name/HP/MaxHP/Alive/LastHit/RespawnIn/AttackGap/Version`、以及 `SessionVersion`。

> 实际影响有限：`SnapshotFor` 成功路径会全量覆盖 `Self/Map/Boss/SessionVersion`，因此陈旧标量
> 目前不会被读到。这是防御性修复——未来若新增「部分填充」的路径，欠重置就会把陈旧数据泄漏给下游。

### 方案

1. 两个早退路径补 `protocol.FreeWorldState(ws)`（[cluster.go:517](../src/cluster/cluster.go#L517)、
   [cluster.go:530](../src/cluster/cluster.go#L530)）。
2. `FreeWorldState` 在截断切片后补标量清零，**保留切片底层数组复用**（不能用 `*ws = WorldState{}`
   整体覆盖，那会丢弃底层数组，让 `sync.Pool` 失去意义）：

```go
func FreeWorldState(ws *WorldState) {
    if ws == nil {
        return
    }
    // 截断切片，复用底层数组，帮助 GC 清理切片内对象引用
    ws.Maps = ws.Maps[:0]
    ws.Nodes = ws.Nodes[:0]
    ws.Events = ws.Events[:0]
    ws.Map.Terrain = ws.Map.Terrain[:0]
    ws.Map.Players = ws.Map.Players[:0]
    ws.Map.NPCs = ws.Map.NPCs[:0]
    ws.Map.Treasures = ws.Map.Treasures[:0]
    ws.Boss.Sites = ws.Boss.Sites[:0]
    // 清零标量字段，避免复用时携带陈旧数据
    ws.Self = protocol.PlayerView{} // PlayerView 全标量，无切片，可整体覆盖
    ws.Map.ID, ws.Map.Name, ws.Map.NodeID = "", "", ""
    ws.Map.Width, ws.Map.Height = 0, 0
    ws.Map.Version = 0
    ws.Boss.Name, ws.Boss.LastHit = "", ""
    ws.Boss.HP, ws.Boss.MaxHP = 0, 0
    ws.Boss.Alive = false
    ws.Boss.RespawnIn, ws.Boss.AttackGap = 0, 0
    ws.Boss.Version = 0
    ws.SessionVersion = 0
    worldStatePool.Put(ws)
}
```

### 验收

- `go build ./...` 通过。
- 说明：泄漏发生在「地图缓存未就绪」窗口期，修复后每个 `地图状态正在同步中` 错误不再泄漏对象。

---

## 1.1 节点启动自拉 checkpoint

### 现状（已核实）

- 节点启动时（[node main.go:49-65](../src/cmd/node/main.go#L49)）对每张主地图只调 `InstallPrimaryMap(cfg)`
  起一张空图，**从不从 Redis 恢复 checkpoint**。
- 协调器 `discoveryLoop`（[cluster.go:732-735](../src/cluster/cluster.go#L732)）确实会 `LoadCheckpoint`
  再调 `client.RestorePrimaryMap(cfg, *cp)`，但 `NodeGRPCClient.RestorePrimaryMap` 是**空转桩**
  （[grpc_client.go:200](../src/cluster/grpc_client.go#L200)），恢复被静默丢弃——这正是 DESIGN_REVIEW §1.3
  描述的「节点重启只会起空图，checkpoint 从未真正回到世界状态」。
- 好消息：`NodeService.RestorePrimaryMap`（[node.go:135](../src/node/node.go#L135)）是真实实现，内部
  `RestoreCheckpoint`（[world.go:550](../src/world/world.go#L550)）能完整恢复 terrain/players/npcs/treasures
  与 version，且带 `cp.Version == 0` 的保护。
- `LoadCheckpoint`（[store.go:246](../src/storage/store.go#L246)）返回 `(*MapCheckpoint, bool)`。

### 方案

让节点**自己**从 Redis 拉 checkpoint，而不是补 RPC（补 RPC 又回到「协调器推动」的老路）。
改动仅在 [node main.go](../src/cmd/node/main.go) 主地图加载循环：

```go
for _, cfg := range available {
    if cfg.ID == mapID {
        if store != nil {
            if cp, ok := store.LoadCheckpoint(mapID); ok && cp.Version > 0 {
                ns.RestorePrimaryMap(cfg, cp)
                log.Printf("节点 [%s] 从 checkpoint 恢复地图 %s (version %d)", nodeID, mapID, cp.Version)
            } else {
                ns.InstallPrimaryMap(cfg)
                log.Printf("节点 [%s] 本地成功加载地图: %s", nodeID, mapID)
            }
        } else {
            // store 初始化失败（PG 连不上）时仍继续，退化为起空图
            ns.InstallPrimaryMap(cfg)
            log.Printf("节点 [%s] 本地加载地图 %s（store 不可用，起空图）", nodeID, mapID)
        }
        hostedMaps = append(hostedMaps, mapID)
        break
    }
}
```

要点：
- `store` 可能为 `nil`（[node main.go:38-40](../src/cmd/node/main.go#L38) PG 连不上时仅告警、仍继续），必须判空。
- `cp.Version > 0` 显式检查，与 `RestoreCheckpoint` 内部保护一致；`RestorePrimaryMap` 自身带
  「地图不存在则 `NewWorld`」逻辑（[node.go:139-144](../src/node/node.go#L139)），重复调用也安全。
- 副本（`-replicas`）节点启动时不实例化 World，副本数据由协调器 `checkpointLoop` 通过 `StoreReplica` RPC
  推送（[node.go:367](../src/node/node.go#L367)），**无需自拉**，本轮不动。

### 边界与取舍

1. **协调器 push 路径变冗余**：节点自拉后，`discoveryLoop` 里的空转桩 `client.RestorePrimaryMap` 成了纯冗余。
   但删除接口方法 + 调用点属于 2.2（拆分控制面），**本轮不改**。空转桩 no-op 也不会覆盖自拉结果，二者自洽。
2. **checkpoint 可能是旧快照**：`checkpointLoop` 每 700ms 写一次，节点自拉到的是「最近一次」而非「故障瞬间」。
   「旧快照 > 空图」，符合故障恢复语义，可接受。
3. **副本恢复窗口**：副本节点重启后 `replicaSnapshots` 为空，若主节点在下一个 checkpointLoop 周期前故障，
   `Promote` 会失败（此错误已在 0.4 可见化）。这是已知边界，归入 2.2 处理。

### 验收

- `go build ./...` 通过。
- 节点重启后，主地图从 Redis checkpoint 恢复（世界状态、玩家、NPC 不丢）。
- 首次启动（无 checkpoint）仍起空图，行为不变。
- `store` 不可用时不 panic，降级为起空图。

---

## 涉及文件清单

| 文件 | 改动 |
|------|------|
| [src/cmd/node/main.go](../src/cmd/node/main.go) | 1.1 主地图加载处自拉 checkpoint |
| [src/cluster/cluster.go](../src/cluster/cluster.go) | 1.2 `SnapshotFor` 早退路径补 Free |
| [src/protocol/message.go](../src/protocol/message.go) | 1.2 `FreeWorldState` 补标量清零 |

---

## 风险与开放问题

1. **checkpoint 旧快照**：自拉恢复拿到的是 ~700ms 前的状态，非故障瞬间精确值。demo/面试口径：故障恢复语义下可接受。
2. **副本恢复窗口**：副本节点重启后到下一次 checkpointLoop 之间有「空副本」窗口，若此时主节点故障则 Promote 失败。归入 2.2。
3. **对象池泄漏是低频的**（仅地图缓存窗口期触发），但修复便宜、零风险，顺手清掉。

---

## 建议的提交切分（可选）

1. `fix: 修复 WorldState 对象池泄漏与欠重置`（1.2）
2. `feat: 节点启动自拉 checkpoint 恢复世界状态`（1.1）

每步独立可编译、可回滚，便于 code review。
