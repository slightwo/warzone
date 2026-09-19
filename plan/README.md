# 后续计划索引

## 简化原则

本项目以学习和理解分布式游戏架构为目标。后续改造遵循以下原则：

1. 只解决已经明确的问题，并保留每一步可独立验证、可单独提交。
2. 优先维护现有核心不变量：Coordinator 管理拓扑与故障转移；每张地图只有一个 Owner Node 写入；`MapEpoch` fencing 继续阻止旧 owner 写入。
3. 先选择容易理解的方案；只有压测证明必要时，才引入更细的并发控制、复杂协议或完整观测体系。
4. Gateway 高可用只保证接入层可切换：已有 gRPC 长连接不做流内迁移，断开后的客户端重新连接并登录即可恢复。

## 文档与边界

| 文档 | 解决的问题 | 当前优先级 |
|---|---|---|
| [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md) | 两份计划的阶段依赖、最小改动、验收条件与停止点 | 最高；所有实施以此顺序推进 |
| [GATEWAY_HA_PLAN.md](GATEWAY_HA_PLAN.md) | Gateway 单点、异常退出后的幽灵会话、经负载均衡接入多个 Gateway | 高 |
| [WORLD_PERFORMANCE_PLAN.md](WORLD_PERFORMANCE_PLAN.md) | 热点地图的无效命令、重复快照和重复状态推送 | 中；先完成 Gateway 高可用 |

## 推荐实施顺序

详细的阶段依赖、每步最小改动、验收条件与停止点见 [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md)。概括顺序如下：

1. Gateway 多实例、真实 readiness、会话 TTL 租约、HAProxy 与 drain 验收。
2. Gateway HA 稳定后，先建立性能基线与最小观测。
3. 仅在数据证明需要时，实施轻量限流和无变化状态抑制。
4. 只有压测仍存在明确瓶颈时，再选择 `MapDelta` 或位置索引之一。

当前不实施 Zone Actor、细粒度多锁、分布式事务、事件溯源、自动无感重连和 Delta 历史补发等高复杂度方案。
