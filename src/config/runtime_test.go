package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultRuntimeIsValid(t *testing.T) {
	if err := DefaultRuntime().Validate(); err != nil {
		t.Fatalf("默认运行时配置必须通过校验，实际: %v", err)
	}
}

func TestLoadRuntimeFileMissingFallsBackToDefaults(t *testing.T) {
	cfg, err := LoadRuntimeFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("配置文件缺失时应回退默认值，实际: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("回退后的默认配置必须有效: %v", err)
	}
	if cfg.Gateway.NodeRPCTimeout.Duration != 500*time.Millisecond {
		t.Fatalf("默认 NodeRPCTimeout 不匹配: %s", cfg.Gateway.NodeRPCTimeout)
	}
}

func TestLoadRuntimeFileOverridesValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runtime.yaml")
	content := []byte(`version: 1
leader_election:
  lease_ttl: 6s
  renew_interval: 2s
  campaign_retry_interval: 300ms
node_registry:
  heartbeat_interval: 4s
  lease_ttl: 9s
coordinator:
  discovery_interval: 800ms
  health_check_interval: 800ms
  node_ping_timeout: 200ms
  leader_retry_interval: 150ms
  boss_reconcile_interval: 2s
node:
  topology_refresh_interval: 250ms
  topology_authority_grace: 800ms
  world_tick_interval: 600ms
  hot_session_flush_interval: 8s
  drain_poll_interval: 50ms
  drain_timeout: 20s
gateway:
  topology_refresh_interval: 400ms
  map_snapshot_refresh_interval: 120ms
  stream_state_interval: 80ms
  node_rpc_timeout: 300ms
http:
  lifecycle_read_header_timeout: 4s
  pprof_enabled: false
  pprof_addr: ""
`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("写入临时配置: %v", err)
	}

	cfg, err := LoadRuntimeFile(path)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	if cfg.LeaderElection.LeaseTTL.Duration != 6*time.Second {
		t.Fatalf("lease_ttl 未覆盖: %s", cfg.LeaderElection.LeaseTTL)
	}
	if cfg.Gateway.NodeRPCTimeout.Duration != 300*time.Millisecond {
		t.Fatalf("node_rpc_timeout 未覆盖: %s", cfg.Gateway.NodeRPCTimeout)
	}
	if cfg.HTTP.PprofEnabled {
		t.Fatal("pprof_enabled 未覆盖为 false")
	}
}

func TestLoadRuntimeFileRejectsUnknownField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runtime.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nunknown_section: {}\n"), 0o644); err != nil {
		t.Fatalf("写入临时配置: %v", err)
	}
	if _, err := LoadRuntimeFile(path); err == nil {
		t.Fatal("未知字段必须导致加载失败")
	}
}

func TestValidateRejectsRenewNotLessThanLease(t *testing.T) {
	cfg := DefaultRuntime()
	cfg.LeaderElection.RenewInterval = cfg.LeaderElection.LeaseTTL
	if err := cfg.Validate(); err == nil {
		t.Fatal("renew_interval 等于 lease_ttl 时必须校验失败")
	}
}

func TestValidateRejectsAuthorityGraceTooSmall(t *testing.T) {
	cfg := DefaultRuntime()
	cfg.Node.TopologyAuthorityGrace = cfg.Node.TopologyRefreshInterval
	if err := cfg.Validate(); err == nil {
		t.Fatal("authority grace 小于两倍刷新周期时必须校验失败")
	}
}
