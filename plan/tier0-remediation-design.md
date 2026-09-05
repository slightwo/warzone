# 第 0 梯队 — 修改方案设计

> 对应 [REMEDIATION_PLAN.md](../REMEDIATION_PLAN.md) 第 0 梯队（0.1 安全三件套、0.2 KEYS→SCAN、
> 0.3 清理教学残留、0.4 处理被吞错误）。本文档是「怎么做」的具体设计，供实施前评审。
>
> 现状定位基于分支 `tier1-remediation` 的代码快照核实。

---

## 总览与实施顺序

四项互相独立，可并行，但建议按「依赖最少 → 依赖最多」排：

| 序 | 内容 | 独立度 | 建议顺序 |
|----|------|--------|---------|
| 0.3 | 清理教学残留 | 纯删除/改注释，零依赖 | 先做，清障 |
| 0.2 | KEYS→SCAN | 单函数重写 | 先做 |
| 0.4 | 处理被吞错误 | 只加 error 处理与日志，不改架构 | 中 |
| 0.1 | 安全改动 | 涉及密码迁移、配置引入 | 最后 |

> 0.1 内部分两步：bcrypt（最独立）→ 凭据 env 化（引入 config 包）。TLS 已按评审意见废弃（见 0.1c）。

---

## 0.3 清理教学残留

### 现状（已核实）

| 类别 | 位置 | 说明 |
|------|------|------|
| 死变量 | [cluster.go:89](../src/cluster/cluster.go#L89) `var studentTodoNotice sync.Map` | 全仓库无任何引用，纯死代码，直接删 |
| 半成品注释 | [cluster.go:382](../src/cluster/cluster.go#L382) `// TODO(Labc.mu.Unlock()3-2):` | 删 |
| 不确定注释 | `// to understand` [cluster.go:77/100](../src/cluster/cluster.go#L77)、[node.go:77](../src/node/node.go#L77)；`// to under` [cluster.go:243/476/624/1158](../src/cluster/cluster.go#L243)、[store.go:337](../src/storage/store.go#L337) | 改为有意义的注释或直接删 |
| 活跃 debug 打印 | [node.go:250/260/264/271/284/289/296/319](../src/node/node.go#L250)、[grpc_client.go:126](../src/cluster/grpc_client.go#L126) | 删（或改标准 log） |
| 注释掉的 debug 打印 | [cluster.go:364/367/370](../src/cluster/cluster.go#L364)、[node.go:171](../src/node/node.go#L171)、[grpc_server.go:47/98](../src/node/grpc_server.go#L47)、[grpc_client.go:124/129](../src/cluster/grpc_client.go#L124)、[cluster.go:1168/1174](../src/cluster/cluster.go#L1168) | 删（连同紧邻的注释块） |

> 注：`logStudentTODO` / `studentTODOError` 在当前快照中**只剩注释里的调用**（[cluster.go:463](../src/cluster/cluster.go#L463)、[cluster.go:1025](../src/cluster/cluster.go#L1025)），没有函数定义，无需额外删除定义，只删注释。

### 方案

1. 删 `studentTodoNotice` 声明（若 `sync` 仍被其它代码用到则保留 import，否则清理 import）。
2. 删除所有 `// to understand` / `// to under` / `// TODO(Labc…)` 注释；若其修饰的代码确实晦涩，补一句真实含义的中文注释，不保留「不确定」语气。
3. 删除活跃 `[debug]` 打印。其中 [node.go](../src/node/node.go) attackBoss 的几条打印在正常路径也有信息价值，改为 `log.Printf`（保持 `log` 包的既有风格，见 [node.go](../src/node/node.go) 顶部已 import `log`），错误分支的改为 `log.Printf("...: %v", err)`。
4. 删除所有被注释掉的 debug 打印及其所在注释行。

### 验收

- `rg "studentTodoNotice|to understand|to under|TODO\(Lab|\[debug\]" src/` 无结果（除 `// to under` 若已改名）。
- `go build ./...` 通过。

---

## 0.2 KEYS → SCAN

### 现状

[store.go:279-295](../src/storage/store.go#L279) `GetActiveNodes` 用 `s.rdb.Keys(ctx, "battle:node:registry:*")`。

### 方案

改用 `Scan` 迭代器：

```go
func (s *Store) GetActiveNodes() ([]NodeRegistryInfo, error) {
    iter := s.rdb.Scan(s.ctx, 0, "battle:node:registry:*", 100).Iterator()
    var nodes []NodeRegistryInfo
    for iter.Next(s.ctx) {
        data, err := s.rdb.Get(s.ctx, iter.Val()).Bytes()
        if err != nil {
            continue // 键可能在 SCAN 与 GET 之间过期，跳过
        }
        var info NodeRegistryInfo
        if json.Unmarshal(data, &info) == nil {
            nodes = append(nodes, info)
        }
    }
    return nodes, iter.Err()
}
```

要点：
- 分页参数 `100`（每批 `COUNT`）对注册表规模足够。
- 原实现「GET 失败/Unmarshal 失败静默跳过」，行为保持一致。
- `iter.Err()` 作为最终错误返回，替代原来的 `err` 返回路径。

### 验收

- `go build ./...` 通过。
- 可选：临时单元测试注入 mock 验证 SCAN 被调用（当前无测试基建，暂不强制）。

---

## 0.4 处理被吞错误

### 现状（已核实，按严重度）

| 位置 | 问题 | 后果 |
|------|------|------|
| [cluster.go:948](../src/cluster/cluster.go#L948) `CP, _ := owner.Checkpoint(...)` | checkpointLoop 吞错 | 快照失败仍用零值/旧 CP 覆盖 Redis，故障恢复拿到坏状态 |
| [cluster.go:994](../src/cluster/cluster.go#L994) `replicasNode.Promote(...)` | handleNodeFailure 吞错 | 副本提升失败无感知，地图短暂失主 |
| [cluster.go:157](../src/cluster/cluster.go#L157) `CP, _ := owner.Checkpoint(...)` | mapCacheLoop 吞错 | 同上，缓存快照失败静默 |
| [cluster.go:1006](../src/cluster/cluster.go#L1006) `sessions, _ := GetAllGlobalSessions()` | 吞错 | 故障重路由漏处理部分会话 |
| [cluster.go:706](../src/cluster/cluster.go#L706) `_ = client.Start()` | 吞错 | 节点客户端启动失败无感知 |
| [grpc_client.go:225](../src/cluster/grpc_client.go#L225) `_, _ = StoreReplica(...)` | 吞错 | 副本同步失败无感知 |
| 多处 `_ = c.store.SaveXxx/PublishEvent` | 吞错 | 热数据/事件丢失无日志 |

### 方案

原则：**第 0 梯队只做「错误可见化」，不改控制流/重试策略**——重试、回退、告警属于第 2 梯队重构。

1. **checkpointLoop / mapCacheLoop**（两处 `CP, _ := Checkpoint`）：
   ```go
   CP, err := owner.Checkpoint(context.Background(), mapID)
   if err != nil {
       log.Printf("[checkpoint] 抓取地图 %s 快照失败: %v", mapID, err)
       continue
   }
   if err := c.store.SaveCheckpoint(CP); err != nil {
       log.Printf("[checkpoint] 保存地图 %s 快照失败: %v", mapID, err)
   }
   ```
   （`continue` 已是「跳过故障节点」语义的延伸，不改变架构。）
2. **Promote**（[cluster.go:994](../src/cluster/cluster.go#L994)）：
   ```go
   if err := replicasNode.Promote(mapID, c.configs[mapID]); err != nil {
       log.Printf("[failover] 副本提升 %s 失败: %v", mapID, err)
       continue
   }
   ```
   （失败时 `continue`，不更新 owners/replicas 映射，避免把失败当成功。）
3. **GetAllGlobalSessions**（[cluster.go:1006](../src/cluster/cluster.go#L1006)）：失败时 `log.Printf` 并 `continue`/直接返回，不遍历空列表。
4. **client.Start()**（[cluster.go:706](../src/cluster/cluster.go#L706)）：失败时 `log.Printf`。
5. **StoreReplica**（[grpc_client.go:225](../src/cluster/grpc_client.go#L225)）：**本轮不改签名**（已定）。该方法签名无 error 返回，内部 `_, _ = StoreReplica(...)` 吞错，调用点（[cluster.go:954](../src/cluster/cluster.go#L954)、[cluster.go:163](../src/cluster/cluster.go#L163)）也无 error 可接。→ **整条副本同步链路的可见化暂缓，归入第 2 梯队重构**（届时一并改 `NodeClient` 接口与 node 实现）。
6. **`_ = c.store.SaveXxx` / `PublishEvent`**：这些多为「尽力而为」的辅助写路径。第 0 梯队**只统一改为 `if err := ...; err != nil { log.Printf(...) }`**，不引入重试。为控制改动面，可先只处理 DESIGN_REVIEW §3.4 点名的三处（Checkpoint×2、Promote），其余 `_ = SaveXxx/PublishEvent` 记录为后续统一项。

### 取舍说明

`StoreReplica` 签名改造牵动接口与 node 实现，**已决定不改签名**，整条副本同步链路的可见化归入第 2 梯队。其余「返回 err 但被丢弃」的调用点统一就地加 `log.Printf`，不改控制流。

### 验收

- 上述调用点不再有裸 `_ =` / `CP, _ :=` 丢弃错误。
- 触发一次节点故障（手动 kill 节点进程）观察日志有 failover 相关输出。

---

## 0.1 安全三件套

### 0.1a 密码哈希：SHA-256 → bcrypt

**现状**：[store.go:259-262](../src/storage/store.go#L259) `hashPassword` 裸 SHA-256、无盐。

**选型**：`golang.org/x/crypto/bcrypt`（go.mod 已有 `x/crypto v0.47.0`，成本因子用 `bcrypt.DefaultCost`=10，可上调 12）。不选 argon2id 的理由：需自行管理 salt/params/编解码，样板多，demo 阶段 bcrypt 足够且更不易用错；argon2id 记入备选。

**改动**：

```go
// hashPassword 生成 bcrypt 哈希；失败时返回 error。
func hashPassword(password string) (string, error) {
    b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
    return string(b), err
}

// verifyPassword 校验密码。兼容旧 SHA-256 哈希（以 "$2" 前缀区分 bcrypt）。
func verifyPassword(hash, password string) bool {
    if strings.HasPrefix(hash, "$2") {
        return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
    }
    // 旧版无盐 SHA-256 兼容路径
    sum := sha256.Sum256([]byte(password))
    return hash == hex.EncodeToString(sum[:])
}
```

调用点联动：
- [store.go:156](../src/storage/store.go#L156) `Register`：`hashPassword` 改为返回 `(string, error)`，需处理 err。
- [store.go:187](../src/storage/store.go#L187) `Authenticate`：`user.PasswordHash != hashPassword(password)` 改为 `!verifyPassword(user.PasswordHash, password)`。

**迁移策略（关键决策）**：现有 PG 里是 SHA-256 哈希。切换后旧用户无法用 bcrypt 匹配。方案：`verifyPassword` 兼容旧格式（如上），并在旧哈希校验成功时**就地升级重哈希**（`SaveProfile` 已有 `Omit("PasswordHash")` 机制，可复用）。若 demo 阶段无真实存量用户，也可选择「清库重注册」，但保留兼容路径成本极低、面试也更好讲。

### 0.1b 凭据 env 化

**现状**：
- [store.go:21](../src/storage/store.go#L21) `var storeAddr = "172.31.50.252"` 硬编码 IP。
- [store.go:100](../src/storage/store.go#L100) DSN 明文密码 `'Wu050601&&'`、user `postgres`、dbname `battleworld`。
- [store.go:124](../src/storage/store.go#L124) Redis 地址拼 `storeAddr:6379`，密码硬编码 `""`。
- 已有 `LAB3_DATA_ROOT` 的 env 读取先例（[server main.go:231](../src/cmd/server/main.go#L231)）。

**方案**：新增 `src/config/config.go`，集中读取环境变量并提供默认值（默认值沿用现值，保证不传 env 也能跑，便于本地开发）：

```go
package config

func EnvOr(key, def string) string {
    if v := strings.TrimSpace(os.Getenv(key)); v != "" {
        return v
    }
    return def
}

func DBHost() string     { return EnvOr("BW_DB_HOST", "172.31.50.252") }
func DBUser() string     { return EnvOr("BW_DB_USER", "postgres") }
func DBPassword() string { return EnvOr("BW_DB_PASSWORD", "") }   // 默认空，强制显式提供
func DBName() string     { return EnvOr("BW_DB_NAME", "battleworld") }
func DBPort() string     { return EnvOr("BW_DB_PORT", "5432") }
func RedisAddr() string  { return EnvOr("BW_REDIS_ADDR", "172.31.50.252:6379") }
func RedisPassword() string { return EnvOr("BW_REDIS_PASSWORD", "") }
```

`NewStore`（[store.go:97-140](../src/storage/store.go#L97)）改为从 `config` 取值拼 DSN/Redis options，删除 `storeAddr` 全局变量。

**注意**：
- DB 密码默认值**设为空**而非现值——避免把秘密继续写死在代码里，同时逼迫部署时显式注入。开发环境用 `tostart.txt` 或 `.env` 说明。
- 密码仍在 git 历史里（`Wu050601&&`），**必须**同时轮换数据库真实密码，并考虑 `git filter-repo` 清理历史（记录为后续项，不在本梯队代码改动内）。

### 0.1c gRPC 加 TLS —— 已废弃

> **评审结论（已废弃）**：第 0 梯队不引入 gRPC TLS。理由：TLS 属于「安全加固」，与第 0 梯队「清理/纠错/可见化」的目标不一致；且 node↔gateway 与 gateway↔终端 client 两条信任边界必须同时处理才不破坏现有明文链路（否则终端 client/benchmark 用明文连强制 TLS 的网关会直接断连）。此项连同 mTLS 一并归入第 2 梯队控制面重构时设计。

### 0.1 验收

- `hashPassword` 返回 bcrypt 哈希（`$2a$...`），`Authenticate` 能通过新旧两种哈希校验。
- 不设 `BW_DB_PASSWORD` 时启动报清晰错误；设置后能连库（本地联调验证）。
- `rg "sha256.Sum256|Wu050601" src/` 在相关安全代码处无残留（旧兼容路径里的 `sha256` 例外，需注释说明）。

---

## 涉及文件清单

| 文件 | 改动 |
|------|------|
| [src/storage/store.go](../src/storage/store.go) | 0.1a hashPassword/verifyPassword、0.1b 配置注入、0.2 SCAN、0.3 `//to under` |
| [src/config/config.go](../src/config/config.go)（新增） | 0.1b env 读取 |
| [src/cluster/grpc_client.go](../src/cluster/grpc_client.go) | 0.3 `[debug]`、0.4 StoreReplica(可选) |
| [src/cluster/cluster.go](../src/cluster/cluster.go) | 0.3 教学残留、0.4 被吞错误 |
| [src/node/node.go](../src/node/node.go) | 0.3 `[debug]`/`//to understand` |
| [src/node/grpc_server.go](../src/node/grpc_server.go) | 0.3 注释掉的 debug |
| [src/start.ps1](../src/start.ps1)（新增） | 一键启动脚本 |

---

## 风险与开放问题

1. **DB 密码轮换**：env 化只是代码层，真实密码已在 git 历史泄露，需运维侧轮换。
2. **bcrypt 性能**：登录/注册路径加 bcrypt 增加 ~50-100ms，对 demo 无影响；压测脚本（benchmark）若大量注册需注意（本梯队不改）。
3. **`StoreReplica` 签名改造** → **已决定**：不改签名，副本同步链路可见化归入第 2 梯队。
4. **gRPC TLS** → **已决定**：本梯队废弃，连同 mTLS 归入第 2 梯队控制面重构时设计。

---

## 建议的提交切分（可选）

1. `chore: 清理教学残留`（0.3）
2. `refactor: GetActiveNodes 改用 SCAN`（0.2）
3. `fix: 可见化被吞错误`（0.4）
4. `security: bcrypt 替换裸 SHA-256`（0.1a）
5. `security: 凭据改 env 注入`（0.1b）

每步独立可编译、可回滚，便于 code review。
