# Battleworld — Design Review & Known Weaknesses

Internal review notes for interview preparation. The project has a **sound layered skeleton**
(gateway / coordinator / node / storage, hot-cold separation, primary-replica failover), but a number
of decisions are either unfinished or would not hold up in production. This document is an honest,
structured catalog of those weaknesses, grouped by altitude and ordered by severity.

---

## Severity legend

| Mark | Meaning |
|------|---------|
| 🔴 | Architecture-level or correctness risk |
| 🟠 | Design decision worth revisiting |
| 🟡 | Detail / polish / technical debt |

---

## 1. Architecture

### 1.1 🔴 The gateway is a control-plane + data-plane + simulation single point

`cluster.Cluster` is started **inside the gateway process** and runs five background loops
(`src/cluster/cluster.go` `Cluster.Start()`):

- `discoveryLoop` — node discovery
- `heartbeatLoop` — failure detection
- `mapCacheLoop` — snapshot caching
- `checkpointLoop` — replica replication
- `backgroundLoop` — world simulation

Consequences of the gateway crashing are far worse than "players disconnect":

1. **The world freezes** — `backgroundLoop` is what drives NPC movement/attacks/spawns by RPC'ing
   every node's `BackgroundStep()`.
2. **Failure detection stops** — no `heartbeatLoop` means a dead node is never failed over.
3. **Replica replication stops** — no `checkpointLoop` means replicas go stale.

**Fix:** split the coordinator into its own process with leader election (Redis lock / etcd), so the
gateway becomes a stateless connection proxy. See §1.2 for the coupled issue.

### 1.2 🔴 Nodes are not autonomous — the world depends on the coordinator to tick

`node.NodeService.Start()` only launches `flushLoop` (flushing hot data to Redis). Nodes have **no
self-ticking simulation loop** — NPC movement, combat, and spawning are all driven remotely by the
coordinator's `backgroundLoop` every 700 ms.

This is the opposite of how real game servers work: each scene server should tick itself, and the
coordinator should only route. In the current design, a node that loses its coordinator does not
keep the world running.

**Fix:** move `BackgroundStep()` into the node's own tick loop; let the coordinator only aggregate
events.

### 1.3 🟠 `NodeClient` interface leaks a "local in-process" assumption

`NodeClient` (`src/cluster/cluster.go`) was designed as an abstraction over a node client, but its
only implementation `NodeGRPCClient` contains **no-op stubs**:

```go
func (c *NodeGRPCClient) Start() error                     { return nil }
func (c *NodeGRPCClient) InstallPrimaryMap(cfg world.MapConfig) {}
func (c *NodeGRPCClient) RestorePrimaryMap(cfg world.MapConfig, cp protocol.MapCheckpoint) {}
```

As a result, the "restore from checkpoint on node discovery" path in `discoveryLoop` is a silent
no-op — a freshly discovered node is never actually restored from Redis. The interface did not evolve
when the implementation switched from in-process to network.

**Fix:** either make these real RPCs, or remove them from the interface.

### 1.4 🟠 Static 1:1 primary-replica, no dynamic sharding

Primary/replica topology is hard-coded via startup flags (`-maps`, `-replicas`). There is no dynamic
rebalancing and no migration based on player load. Hotspots are inevitable at scale. Acceptable for a
demo, but not production HA.

---

## 2. Technical decisions

### 2.1 🔴 Three serialization schemes coexist; gRPC carries JSON semantics

The codebase uses three codecs simultaneously:

| Channel | Encoding |
|---------|----------|
| node ↔ gateway gRPC | protobuf (`src/pb/battle.proto`) |
| gateway ↔ terminal client | JSON (`src/protocol/message.go` `Conn.Send`) |
| Redis hot data | `goccy/go-json` (`src/storage/store.go`) |

Worse, the `GameStream` bidirectional stream's `Message` is a flat bag of `string` fields plus a
nested `WorldState` — semantically "JSON over gRPC". It gets neither protobuf's strong typing nor its
compact binary representation, and it makes the `.proto` definition almost decorative.

The cost is concrete: `src/protocol/adapter.go` is **~430 lines of mechanical `ToProtoXxx` /
`FromProtoXxx` hand-written conversions**, and the conversions are already drifting out of sync with
each other.

**Fix:** pick one wire contract. Either define proper typed gRPC messages, or drop protobuf and use
JSON end-to-end. The hybrid is the worst of both.

### 2.2 🔴 Security: unsalted SHA-256, hard-coded credentials, plaintext gRPC

- Passwords are hashed with **bare SHA-256, no salt** (`src/storage/store.go` `hashPassword`) —
  trivially rainbow-table attackable; production requires bcrypt/argon2.
- DB address and password are **hard-coded in source** (`storeAddr`, DSN password in
  `NewStore`), and live in git history.
- Inter-node gRPC uses **plaintext** (`insecure.NewCredentials()` in `src/cluster/grpc_client.go`).

### 2.3 🟠 Redis `KEYS` for node discovery

`GetActiveNodes` scans with `rdb.Keys("battle:node:registry:*")`. `KEYS` blocks Redis and is a
production incident at scale. Use `SCAN` or maintain an index set.

### 2.4 🟠 Half-baked event system: in-memory, no replay

Two event paths coexist and are conflated:

- `pushEvent` / `broadcastGlobalEvent` publish to Redis Pub/Sub.
- `eventLoop` subscribes and buffers into **in-process memory** (`globalEvents`, `mapEvents`,
  `userEvents`), which `SnapshotFor` then reads.

So events are process-local state: they are lost on gateway restart, and there is no replay. The
`// to understand` comments on those fields suggest the author was unsure about this too.

---

## 3. Details

### 3.1 🟡 Version fields have no consistent semantics and are effectively dead

`SessionVersion` / `Version` mean different things in different places:

- `Login` hard-codes `session.Version = 1`.
- `SwitchMap` increments it.
- `HotSession.SessionVersion` is hard-coded to `0` with a comment saying "no need to maintain it".
- `MapView.Version`, `MapCheckpoint.Version`, `BossState.Version` are unrelated counters.

None of them are used for conflict/consistency detection — they are counters without a consumer.
This is a trap question ("what is `Version` for?") that deserves a confident answer.

### 3.2 🟡 `WorldState` object pool leaks and under-resets

`SnapshotFor` allocates from the pool (`protocol.AllocWorldState()`) but several early `return error`
paths return without `FreeWorldState`. Additionally, `FreeWorldState` resets the slices but **not the
scalar fields of `Self` / `Map`**, so reused objects can carry stale data.

### 3.3 🟡 Teaching artifacts left in the code

- `studentTodoNotice sync.Map` global variable.
- `logStudentTODO` / `studentTODOError` call sites.
- `// to understand`, `// TODO(Labc.mu.Unlock3-2)` half-finished comments.
- Numerous `fmt.Println("[debug]...")` statements.

These read as assignment leftovers and should be removed before showing the repo.

### 3.4 🟡 Errors are silently swallowed

- `checkpointLoop`: `CP, _ := owner.Checkpoint(...)` — a failed checkpoint silently falls back to a
  stale snapshot during failover.
- `Promote`'s error is ignored in `handleNodeFailure` — if the replica never received that map's
  snapshot, the promotion fails and the map is left temporarily unowned.
- Many `_ = c.store.SaveHotSession(...)`, `_ = conn.WriteJSON(...)` with no error handling.

---

## 4. Priority matrix

| Severity | Issue | Layer |
|----------|-------|-------|
| 🔴 | Gateway = control + data + simulation single point | Architecture |
| 🔴 | Nodes not autonomous; world ticked remotely | Architecture |
| 🔴 | Three codecs; JSON semantics over gRPC | Decision |
| 🔴 | Unsalted SHA-256, hard-coded creds, plaintext gRPC | Security |
| 🟠 | `NodeClient` no-op stubs / abstraction leak | Decision |
| 🟠 | Redis `KEYS` for discovery | Decision |
| 🟠 | In-memory events, no replay | Decision |
| 🟠 | Static 1:1 primary-replica | Architecture |
| 🟡 | Version fields semantically dead | Detail |
| 🟡 | `WorldState` pool leak / under-reset | Detail |
| 🟡 | Teaching artifacts | Detail |
| 🟡 | Swallowed errors | Detail |

---

## Recommended remediation order

1. Split control plane from data plane (leader-elected coordinator; gateway becomes proxy).
2. Make nodes self-ticking; drop coordinator-driven simulation.
3. Unify the wire protocol to one codec.
4. Replace SHA-256 with bcrypt/argon2; move credentials to env/config; enable TLS.
5. Replace `KEYS` with `SCAN`; add session TTL/lease + takeover.
6. Clean up teaching artifacts and swallowed errors.
