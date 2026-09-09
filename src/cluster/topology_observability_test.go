package cluster

import (
	"testing"
	"time"

	"battleworld/protocol"
	"battleworld/storage"
)

// TestSnapshotForExposesTopologyVersionAndMapEpoch 验证网关在 SnapshotFor 中
// 把当前已提交拓扑的 Version 与会话所属地图的 MapEpoch 写入 WorldState，
// 使客户端可据此判断路由是否变更以及当前主权 fencing token。
func TestSnapshotForExposesTopologyVersionAndMapEpoch(t *testing.T) {
	const username = "observer"
	const mapID = "green"
	const ownerID = "node-a"

	cluster := &Cluster{
		nodes: map[string]gatewayNodeClient{
			ownerID: &fakeGatewayNodeClient{id: ownerID},
		},
		nodeAddrs:   map[string]string{ownerID: "127.0.0.1:9311"},
		nodeTargets: map[string]string{ownerID: "127.0.0.1:9311"},
		configs:     testMapConfigs(mapID),
		owners:      map[string]string{mapID: ownerID},
		replicas:    map[string]string{},
		topology: storage.Topology{
			Version:   3,
			Owners:    map[string]string{mapID: ownerID},
			Replicas:  map[string]string{},
			MapEpochs: map[string]uint64{mapID: 7},
			UpdatedAt: time.Now().UTC(),
		},
		topologyLoaded: true,
		mapCache: map[string]MapCacheData{
			mapID: {Brief: protocol.MapBrief{ID: mapID, NodeID: ownerID}, View: &protocol.MapView{ID: mapID, NodeID: ownerID}},
		},
		mapEvents:  map[string][]string{},
		userEvents: map[string][]string{},
	}
	cluster.localSessions.Store(username, &storage.GlobalSession{
		Username: username,
		MapID:    mapID,
		NodeID:   ownerID,
		Version:  42,
	})

	ws, err := cluster.SnapshotFor(username)
	if err != nil {
		t.Fatalf("SnapshotFor 失败: %v", err)
	}
	if ws.TopologyVersion != 3 {
		t.Fatalf("TopologyVersion = %d, want 3", ws.TopologyVersion)
	}
	if ws.MapEpoch != 7 {
		t.Fatalf("MapEpoch = %d, want 7", ws.MapEpoch)
	}
	if ws.SessionVersion != 42 {
		t.Fatalf("SessionVersion = %d, want 42", ws.SessionVersion)
	}

	// 通过 adapter 往返后字段仍应保留，确保客户端收到的 protobuf 消息不丢字段。
	pb := protocol.ToProtoWorldState(ws)
	round := protocol.FromProtoWorldState(pb)
	if round.TopologyVersion != 3 || round.MapEpoch != 7 {
		t.Fatalf("adapter 往返后 TopologyVersion=%d MapEpoch=%d", round.TopologyVersion, round.MapEpoch)
	}

	// 复用 pool 后新分配的对象不应携带上一次的拓扑字段。
	protocol.FreeWorldState(ws)
	fresh := protocol.AllocWorldState()
	if fresh.TopologyVersion != 0 || fresh.MapEpoch != 0 {
		t.Fatalf("pool 复用后 TopologyVersion=%d MapEpoch=%d，未清零", fresh.TopologyVersion, fresh.MapEpoch)
	}
	protocol.FreeWorldState(fresh)
}
