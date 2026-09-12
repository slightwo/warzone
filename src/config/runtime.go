package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

const RuntimeConfigPath = "config/runtime.yaml"

// RuntimeConfig 收敛影响服务时延、租约与缓存新鲜度的运行时参数。
// 配置在进程启动时加载；运行期间修改文件不会改变已经运行的定时器。
type RuntimeConfig struct {
	Version        int                  `yaml:"version"`
	LeaderElection LeaderElectionConfig `yaml:"leader_election"`
	NodeRegistry   NodeRegistryConfig   `yaml:"node_registry"`
	Coordinator    CoordinatorConfig    `yaml:"coordinator"`
	Node           NodeConfig           `yaml:"node"`
	Gateway        GatewayConfig        `yaml:"gateway"`
	HTTP           HTTPConfig           `yaml:"http"`
}

type LeaderElectionConfig struct {
	LeaseTTL              Duration `yaml:"lease_ttl"`
	RenewInterval         Duration `yaml:"renew_interval"`
	CampaignRetryInterval Duration `yaml:"campaign_retry_interval"`
}

type NodeRegistryConfig struct {
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	LeaseTTL          Duration `yaml:"lease_ttl"`
}

type CoordinatorConfig struct {
	DiscoveryInterval     Duration `yaml:"discovery_interval"`
	HealthCheckInterval   Duration `yaml:"health_check_interval"`
	NodePingTimeout       Duration `yaml:"node_ping_timeout"`
	LeaderRetryInterval   Duration `yaml:"leader_retry_interval"`
	BossReconcileInterval Duration `yaml:"boss_reconcile_interval"`
}

type NodeConfig struct {
	TopologyRefreshInterval Duration `yaml:"topology_refresh_interval"`
	TopologyAuthorityGrace  Duration `yaml:"topology_authority_grace"`
	WorldTickInterval       Duration `yaml:"world_tick_interval"`
	HotSessionFlushInterval Duration `yaml:"hot_session_flush_interval"`
	DrainPollInterval       Duration `yaml:"drain_poll_interval"`
	DrainTimeout            Duration `yaml:"drain_timeout"`
}

type GatewayConfig struct {
	TopologyRefreshInterval    Duration `yaml:"topology_refresh_interval"`
	MapSnapshotRefreshInterval Duration `yaml:"map_snapshot_refresh_interval"`
	StreamStateInterval        Duration `yaml:"stream_state_interval"`
	NodeRPCTimeout             Duration `yaml:"node_rpc_timeout"`
}

type HTTPConfig struct {
	LifecycleReadHeaderTimeout Duration `yaml:"lifecycle_read_header_timeout"`
	PprofEnabled               bool     `yaml:"pprof_enabled"`
	PprofAddr                  string   `yaml:"pprof_addr"`
}

// Duration 支持 YAML 中以 Go duration 字符串表示时长，例如 "500ms" 或 "2s"。
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
		return fmt.Errorf("时长必须是字符串，例如 500ms 或 2s")
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("解析时长 %q: %w", value.Value, err)
	}
	d.Duration = parsed
	return nil
}

// DefaultRuntime 返回与改造前硬编码值相同的有效默认配置。它也让单元测试无需依赖工作目录。
func DefaultRuntime() RuntimeConfig {
	return RuntimeConfig{
		Version: 1,
		LeaderElection: LeaderElectionConfig{
			LeaseTTL:              duration(3 * time.Second),
			RenewInterval:         duration(time.Second),
			CampaignRetryInterval: duration(200 * time.Millisecond),
		},
		NodeRegistry: NodeRegistryConfig{
			HeartbeatInterval: duration(3 * time.Second),
			LeaseTTL:          duration(5 * time.Second),
		},
		Coordinator: CoordinatorConfig{
			DiscoveryInterval:     duration(time.Second),
			HealthCheckInterval:   duration(time.Second),
			NodePingTimeout:       duration(500 * time.Millisecond),
			LeaderRetryInterval:   duration(250 * time.Millisecond),
			BossReconcileInterval: duration(time.Second),
		},
		Node: NodeConfig{
			TopologyRefreshInterval: duration(500 * time.Millisecond),
			TopologyAuthorityGrace:  duration(2 * time.Second),
			WorldTickInterval:       duration(700 * time.Millisecond),
			HotSessionFlushInterval: duration(10 * time.Second),
			DrainPollInterval:       duration(100 * time.Millisecond),
			DrainTimeout:            duration(30 * time.Second),
		},
		Gateway: GatewayConfig{
			TopologyRefreshInterval:    duration(500 * time.Millisecond),
			MapSnapshotRefreshInterval: duration(150 * time.Millisecond),
			StreamStateInterval:        duration(100 * time.Millisecond),
			NodeRPCTimeout:             duration(500 * time.Millisecond),
		},
		HTTP: HTTPConfig{
			LifecycleReadHeaderTimeout: duration(5 * time.Second),
			PprofEnabled:               true,
			PprofAddr:                  "localhost:6060",
		},
	}
}

func duration(value time.Duration) Duration { return Duration{Duration: value} }

// LoadRuntime 自动读取当前工作目录下的 config/runtime.yaml。文件缺失时回退到内置默认值；
// 文件存在但无法读取、解析或校验时必须让进程拒绝启动，避免静默采用错误时序。
func LoadRuntime() (RuntimeConfig, error) {
	return LoadRuntimeFile(RuntimeConfigPath)
}

// LoadRuntimeFile 允许测试使用独立临时文件；生产代码应使用 LoadRuntime。
func LoadRuntimeFile(path string) (RuntimeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return DefaultRuntime(), nil
		}
		return RuntimeConfig{}, fmt.Errorf("读取运行时配置 %s: %w", filepath.Clean(path), err)
	}

	cfg := DefaultRuntime()
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return RuntimeConfig{}, fmt.Errorf("解析运行时配置 %s: %w", filepath.Clean(path), err)
	}
	if err := cfg.Validate(); err != nil {
		return RuntimeConfig{}, fmt.Errorf("校验运行时配置 %s: %w", filepath.Clean(path), err)
	}
	return cfg, nil
}

func (c RuntimeConfig) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("不支持的 version=%d，仅支持 1", c.Version)
	}
	for name, value := range map[string]time.Duration{
		"leader_election.lease_ttl":               c.LeaderElection.LeaseTTL.Duration,
		"leader_election.renew_interval":          c.LeaderElection.RenewInterval.Duration,
		"leader_election.campaign_retry_interval": c.LeaderElection.CampaignRetryInterval.Duration,
		"node_registry.heartbeat_interval":        c.NodeRegistry.HeartbeatInterval.Duration,
		"node_registry.lease_ttl":                 c.NodeRegistry.LeaseTTL.Duration,
		"coordinator.discovery_interval":          c.Coordinator.DiscoveryInterval.Duration,
		"coordinator.health_check_interval":       c.Coordinator.HealthCheckInterval.Duration,
		"coordinator.node_ping_timeout":           c.Coordinator.NodePingTimeout.Duration,
		"coordinator.leader_retry_interval":       c.Coordinator.LeaderRetryInterval.Duration,
		"coordinator.boss_reconcile_interval":     c.Coordinator.BossReconcileInterval.Duration,
		"node.topology_refresh_interval":          c.Node.TopologyRefreshInterval.Duration,
		"node.topology_authority_grace":           c.Node.TopologyAuthorityGrace.Duration,
		"node.world_tick_interval":                c.Node.WorldTickInterval.Duration,
		"node.hot_session_flush_interval":         c.Node.HotSessionFlushInterval.Duration,
		"node.drain_poll_interval":                c.Node.DrainPollInterval.Duration,
		"node.drain_timeout":                      c.Node.DrainTimeout.Duration,
		"gateway.topology_refresh_interval":       c.Gateway.TopologyRefreshInterval.Duration,
		"gateway.map_snapshot_refresh_interval":   c.Gateway.MapSnapshotRefreshInterval.Duration,
		"gateway.stream_state_interval":           c.Gateway.StreamStateInterval.Duration,
		"gateway.node_rpc_timeout":                c.Gateway.NodeRPCTimeout.Duration,
		"http.lifecycle_read_header_timeout":      c.HTTP.LifecycleReadHeaderTimeout.Duration,
	} {
		if value <= 0 {
			return fmt.Errorf("%s 必须大于 0，当前为 %s", name, value)
		}
	}
	if c.LeaderElection.RenewInterval.Duration >= c.LeaderElection.LeaseTTL.Duration {
		return errors.New("leader_election.renew_interval 必须小于 leader_election.lease_ttl")
	}
	if c.NodeRegistry.HeartbeatInterval.Duration >= c.NodeRegistry.LeaseTTL.Duration {
		return errors.New("node_registry.heartbeat_interval 必须小于 node_registry.lease_ttl")
	}
	if c.Coordinator.NodePingTimeout.Duration >= c.Coordinator.HealthCheckInterval.Duration {
		return errors.New("coordinator.node_ping_timeout 必须小于 coordinator.health_check_interval")
	}
	if c.Node.TopologyAuthorityGrace.Duration < 2*c.Node.TopologyRefreshInterval.Duration {
		return errors.New("node.topology_authority_grace 至少应为 node.topology_refresh_interval 的两倍")
	}
	if c.HTTP.PprofEnabled && c.HTTP.PprofAddr == "" {
		return errors.New("http.pprof_enabled 为 true 时 http.pprof_addr 不能为空")
	}
	return nil
}
