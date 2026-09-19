# Gateway 高可用计划

## 简化原则

本计划的目标是消除 Gateway 单点，并保留“客户端经负载均衡器接入多个 Gateway”的架构；不追求生产级全套容量平台。

- Gateway 只负责接入、认证、路由、缓存与推送，不拥有地图主权。
- 每张地图仍由一个 Owner Node 执行 tick 与状态写入；Topology、`MapEpoch` fencing 和 Node 故障转移保持不变。
- gRPC 双向流建立后固定在一个 Gateway；Gateway 故障时客户端重新连接并登录，不做流内迁移或自动无感恢复。
- 不为性能优化提前引入 `MapDelta`、复杂限流、完整指标体系或复杂会话迁移；这些内容由 [WORLD_PERFORMANCE_PLAN.md](WORLD_PERFORMANCE_PLAN.md) 按压测结果决定。

## 当前问题

1. Gateway 固定监听 `127.0.0.1:9310`，本地只能启动一个实例。
2. `/readyz` 目前只判断是否 drain，无法表示 Redis、Topology、Node 路由和地图缓存是否就绪。
3. `global_sessions` 使用 Redis Hash，不能按用户设置 TTL；Gateway 异常退出会留下永久在线记录。
4. 终端客户端连接断开后只提示错误并退出；恢复方式是重新连接并登录。

## 目标架构

```text
客户端
  │ gRPC 长连接（固定连接到一个 Gateway）
  ▼
HAProxy / 外部 L4 负载均衡器
  ├── Gateway A ──┐
  └── Gateway B ──┼── gRPC 数据面 ──> 当前地图 Owner Node
                  │
                  └── Redis：Topology、会话租约、事件

Coordinator：选主、节点健康、Topology、Node 故障转移
```

负载均衡器只向 ready Gateway 分配**新**连接。Gateway A 宕机时，A 上的游戏流会断开，但 Node 与 Coordinator 继续运行；客户端重新通过负载均衡器建立连接后会进入 Gateway B。

> 本地 HAProxy 本身仍是入口单点，用于验证 Gateway 多副本与故障切换。真正的入口高可用应交给云负载均衡器或独立基础设施，不在本项目中自行实现。

## 实施步骤

### 1. Gateway 多副本与真实 readiness

1. 为 `cmd/server` 增加：
   - `-addr`：本实例 gRPC 监听地址；
   - `-id`：Gateway 实例标识。
2. 保留 `protocol.GatewayAddr` 作为客户端默认入口或本地开发默认值；多个 Gateway 使用不同的后端端口，例如 `9317`、`9318`。
3. 实际维护每个 Gateway 的 `active_connections`。
4. `/healthz` 只表示进程可响应；`/readyz` 仅在以下条件同时满足时返回成功：
   - 未处于 drain；
   - Redis 可访问；
   - 已加载有效 Topology；
   - 所有当前地图 owner 的数据面 gRPC 客户端已建立；
   - 当前拓扑对应的地图缓存已完成首次加载。

### 2. 使用 TTL 会话租约替换全局 Hash 会话

将 `global_sessions` 替换为每用户一个 Redis Key：

```text
battle:session:<username>
  session_id   每次登录生成的随机值
  gateway_id   当前 Gateway 实例标识
  map_id       当前地图
  version      会话/切图版本
  TTL          初始建议 15 秒
```

最小规则：

1. 登录时用原子“仅 Key 不存在才创建”的操作获取租约；建议每 5 秒续租一次。
2. 获得租约后再将玩家加入 Node；失败时仅删除匹配当前 `session_id` 的租约。
3. 续租、切图、登出和流关闭都必须校验 `session_id`，旧 Gateway 或旧流不能续活、覆盖或删除新会话。
4. 流关闭前只有仍拥有当前租约的连接才可以移除 Node 中的玩家；玩家重新登录时，`AddOrRestorePlayer` 必须是幂等的，不能用旧持久化 Profile 覆盖尚在世界内的实时状态。
5. Gateway 异常退出后停止续租；租约到期后玩家可重新登录。当前客户端只需重新输入账号密码，不新增重连 Token。
6. 会话不再保存或迁移 `node_id`：所有命令始终按照最新 Topology 查询 owner 与 `MapEpoch`。因此 Coordinator 故障转移只更新 Topology/fence，不再遍历和迁移会话。

### 3. 通过 HAProxy 接入多个 Gateway

本地启动两个 Gateway 和一个 HAProxy：

```text
客户端入口（HAProxy）：127.0.0.1:9310
Gateway A：127.0.0.1:9317，lifecycle：127.0.0.1:9422
Gateway B：127.0.0.1:9318，lifecycle：127.0.0.1:9423
```

HAProxy 使用 TCP 转发 gRPC 长连接，并通过各 Gateway lifecycle 端口的 `GET /readyz` 做健康检查。后端仅保留 ready 实例；不需要会话粘滞、流迁移或服务发现。

`start.sh` 相应改为启动 Gateway A、Gateway B 与 HAProxy；客户端始终连接 `9310`。

### 4. 保留简单 drain 和故障验证

`POST /drain` 的最小语义：

1. 将 Gateway 标记为 draining，使 `/readyz` 立即返回非成功状态；
2. HAProxy 不再向该副本分配新连接；
3. 已有流继续运行，直到主动结束或到达固定 drain timeout；
4. 进程使用 `GracefulStop` 停止。

完成后只验证以下场景：

1. 两个 Gateway 同时启动，客户端经 `9310` 可以正常进入游戏。
2. 停止一个 Gateway 后，Coordinator、Node 和另一 Gateway 持续运行；等待租约过期后，客户端能重新登录。
3. 对一个 Gateway 调用 `/drain` 后，后续新连接只会进入另一 Gateway。

## 当前不做

- 自动无感重连、流内迁移、重连 Token
- Session `node_id` 的持久化与 Coordinator 侧会话迁移
- 完整 Prometheus 指标、连接配额、复杂限流与慢客户端治理
- `MapDelta`、订阅表、Delta 历史补发和双栈协议
- 容器编排、服务发现、双 HAProxy/VIP

这些能力只有在当前方案完成并经过压测后，才按实际问题单独评估。
