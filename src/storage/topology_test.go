package storage

import (
	"context"
	"errors"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	"battleworld/protocol"

	"github.com/redis/go-redis/v9"
)

func TestRedisOnlyStoreRejectsSQLOperations(t *testing.T) {
	store := newTopologyTestStore(t)
	if err := store.Register("user", "password"); !errors.Is(err, ErrDatabaseUnavailable) {
		t.Fatalf("Redis-only Register error = %v, want ErrDatabaseUnavailable", err)
	}
	if _, err := store.Authenticate("user", "password"); !errors.Is(err, ErrDatabaseUnavailable) {
		t.Fatalf("Redis-only Authenticate error = %v, want ErrDatabaseUnavailable", err)
	}
	if _, err := store.LoadProfile("user"); !errors.Is(err, ErrDatabaseUnavailable) {
		t.Fatalf("Redis-only LoadProfile error = %v, want ErrDatabaseUnavailable", err)
	}
	if err := store.SaveProfile(protocol.UserProfile{Username: "user"}); !errors.Is(err, ErrDatabaseUnavailable) {
		t.Fatalf("Redis-only SaveProfile error = %v, want ErrDatabaseUnavailable", err)
	}
}

func TestTopologyCloneAndValidate(t *testing.T) {
	topology := validTopology()
	knownMaps := map[string]struct{}{"green": {}}
	if err := topology.Validate(knownMaps); err != nil {
		t.Fatalf("valid topology rejected: %v", err)
	}

	clone := topology.Clone()
	topology.Owners["green"] = "node-c"
	topology.MapEpochs["green"] = 2

	if got := clone.Owners["green"]; got != "node-a" {
		t.Fatalf("clone owner mutated through source: got %q", got)
	}
	if got := clone.MapEpochs["green"]; got != 1 {
		t.Fatalf("clone epoch mutated through source: got %d", got)
	}
}

func TestTopologyValidateRejectsInvalidState(t *testing.T) {
	tests := []struct {
		name     string
		topology Topology
		known    map[string]struct{}
	}{
		{
			name: "zero version",
			topology: Topology{
				Owners:    map[string]string{"green": "node-a"},
				MapEpochs: map[string]uint64{"green": 1},
				UpdatedAt: time.Now(),
			},
		},
		{
			name:     "unknown map",
			topology: validTopology(),
			known:    map[string]struct{}{"cave": {}},
		},
		{
			name: "missing epoch",
			topology: Topology{
				Version:   1,
				Owners:    map[string]string{"green": "node-a"},
				UpdatedAt: time.Now(),
			},
		},
		{
			name: "blank node id",
			topology: Topology{
				Version:   1,
				Owners:    map[string]string{"green": " "},
				MapEpochs: map[string]uint64{"green": 1},
				UpdatedAt: time.Now(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.topology.Validate(test.known); err == nil {
				t.Fatal("invalid topology accepted")
			}
		})
	}
}

func TestTopologyAllowsUnassignedOwner(t *testing.T) {
	topology := validTopology()
	topology.Owners["green"] = ""
	if err := topology.Validate(map[string]struct{}{"green": {}}); err != nil {
		t.Fatalf("topology with unassigned owner rejected: %v", err)
	}
}

func TestTopologyNormalizeDoesNotAssignSubmissionTime(t *testing.T) {
	topology := Topology{}
	topology.Normalize()
	if !topology.UpdatedAt.IsZero() {
		t.Fatalf("normalize assigned updated_at: %v", topology.UpdatedAt)
	}
	if topology.Owners == nil || topology.MapEpochs == nil {
		t.Fatal("normalize did not initialize topology maps")
	}
}

func TestTopologyStoreCompareAndSave(t *testing.T) {
	store := newTopologyTestStore(t)

	if topology, found, err := store.LoadTopology(); err != nil || found || topology != nil {
		t.Fatalf("empty topology load = topology=%v found=%t err=%v, want nil false nil", topology, found, err)
	}

	initial := validTopology()
	if err := store.SaveTopology(initial); err != nil {
		t.Fatalf("save topology: %v", err)
	}
	loaded, found, err := store.LoadTopology()
	if err != nil || !found {
		t.Fatalf("load saved topology: found=%t err=%v", found, err)
	}
	loaded.Owners["green"] = "mutated-reader"
	loaded, found, err = store.LoadTopology()
	if err != nil || !found || loaded.Owners["green"] != "node-a" {
		t.Fatalf("stored topology shared mutable map: found=%t topology=%+v err=%v", found, loaded, err)
	}
	if err := store.rdb.Del(store.ctx, TopologyRedisKey).Err(); err != nil {
		t.Fatalf("clear saved topology: %v", err)
	}

	if err := store.CompareAndSaveTopology(0, initial); err != nil {
		t.Fatalf("create topology: %v", err)
	}
	if err := store.CompareAndSaveTopology(0, initial); !errors.Is(err, ErrTopologyVersionConflict) {
		t.Fatalf("second create error = %v, want version conflict", err)
	}

	loaded, found, err = store.LoadTopology()
	if err != nil {
		t.Fatalf("load committed topology: %v", err)
	}
	if !found || loaded.Version != 1 || loaded.Owners["green"] != "node-a" {
		t.Fatalf("unexpected loaded topology: found=%t topology=%+v", found, loaded)
	}

	next := loaded.Clone()
	next.Version = 2
	next.Owners["green"] = "node-b"
	next.MapEpochs["green"] = 2
	next.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopology(1, next); err != nil {
		t.Fatalf("advance topology: %v", err)
	}
	if err := store.CompareAndSaveTopology(1, next); !errors.Is(err, ErrTopologyVersionConflict) {
		t.Fatalf("stale update error = %v, want version conflict", err)
	}

	loaded, found, err = store.LoadTopology()
	if err != nil {
		t.Fatalf("load advanced topology: %v", err)
	}
	if !found || loaded.Version != 2 || loaded.Owners["green"] != "node-b" {
		t.Fatalf("unexpected advanced topology: found=%t topology=%+v", found, loaded)
	}
}

func TestTopologyStoreConcurrentCreateOnlyOneSucceeds(t *testing.T) {
	store := newTopologyTestStore(t)
	const writers = 8

	start := make(chan struct{})
	errs := make(chan error, writers)
	var workers sync.WaitGroup
	for range writers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			errs <- store.CompareAndSaveTopology(0, validTopology())
		}()
	}
	close(start)
	workers.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrTopologyVersionConflict):
			conflicts++
		default:
			t.Fatalf("concurrent create returned unexpected error: %v", err)
		}
	}
	if successes != 1 || conflicts != writers-1 {
		t.Fatalf("concurrent create successes=%d conflicts=%d, want 1 and %d", successes, conflicts, writers-1)
	}
}

func TestLeaderTermFencesStaleCoordinatorTopologyCommit(t *testing.T) {
	store := newTopologyTestStore(t)
	if err := store.rdb.Set(store.ctx, CoordinatorLeaderTermRedisKey, 3, 0).Err(); err != nil {
		t.Fatalf("seed leader term: %v", err)
	}
	initial := validTopology()
	initial.LeaderTerm = 3
	if err := store.CompareAndSaveTopologyForLeader(0, initial, 3); err != nil {
		t.Fatalf("commit term 3 topology: %v", err)
	}

	if err := store.rdb.Set(store.ctx, CoordinatorLeaderTermRedisKey, 4, 0).Err(); err != nil {
		t.Fatalf("advance durable leader term: %v", err)
	}
	stale := initial.Clone()
	stale.Version = 2
	stale.LeaderTerm = 3
	stale.Owners["green"] = "node-c"
	stale.MapEpochs["green"] = 2
	stale.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopologyForLeader(1, stale, 3); !errors.Is(err, ErrLeaderTermInvariant) {
		t.Fatalf("stale leader commit error = %v, want ErrLeaderTermInvariant", err)
	}
	loaded, found, err := store.LoadTopology()
	if err != nil || !found || loaded.Version != 1 || loaded.LeaderTerm != 3 || loaded.Owners["green"] != "node-a" || loaded.MapEpochs["green"] != 1 {
		t.Fatalf("stale leader modified topology: topology=%+v found=%t err=%v", loaded, found, err)
	}

	current := initial.Clone()
	current.Version = 2
	current.LeaderTerm = 4
	current.Owners["green"] = "node-c"
	current.MapEpochs["green"] = 2
	current.UpdatedAt = time.Now().UTC()
	if err := store.CompareAndSaveTopologyForLeader(1, current, 4); err != nil {
		t.Fatalf("current leader commit: %v", err)
	}
}

func TestLoadTopologyRejectsCorruptData(t *testing.T) {
	store := newTopologyTestStore(t)
	if err := store.rdb.Set(store.ctx, TopologyRedisKey, "not-json", 0).Err(); err != nil {
		t.Fatalf("seed corrupt topology: %v", err)
	}

	_, found, err := store.LoadTopology()
	if found {
		t.Fatal("corrupt topology reported as found")
	}
	if !errors.Is(err, ErrTopologyCorrupt) {
		t.Fatalf("load corrupt topology error = %v, want ErrTopologyCorrupt", err)
	}

	if err := store.CompareAndSaveTopology(1, Topology{
		Version:   2,
		Owners:    map[string]string{"green": "node-b"},
		MapEpochs: map[string]uint64{"green": 1},
		UpdatedAt: time.Now().UTC(),
	}); !errors.Is(err, ErrTopologyCorrupt) {
		t.Fatalf("CAS corrupt topology error = %v, want ErrTopologyCorrupt", err)
	}
}

func validTopology() Topology {
	return Topology{
		Version:    1,
		LeaderTerm: 0,
		Owners:     map[string]string{"green": "node-a"},
		MapEpochs:  map[string]uint64{"green": 1},
		UpdatedAt:  time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC),
	}
}

func newTopologyTestStore(t *testing.T) *Store {
	t.Helper()
	redisServer, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is required for topology storage integration tests")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve Redis port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release Redis port: %v", err)
	}

	process := exec.Command(redisServer,
		"--bind", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--save", "",
		"--appendonly", "no",
	)
	if err := process.Start(); err != nil {
		t.Fatalf("start Redis: %v", err)
	}
	t.Cleanup(func() {
		if process.Process != nil {
			_ = process.Process.Kill()
		}
		_ = process.Wait()
	})

	client := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))})
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := client.Ping(context.Background()).Err(); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("temporary Redis did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatalf("创建 Redis-only Store: %v", err)
	}
	return store
}
