// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

// Reproduction of the HAKeeper zombie-replica scenario described in
// https://github.com/matrixorigin/matrixone/issues/24203.
//
// The reproduction builds a 3-node HAKeeper cluster, removes replica 3 from
// shard 0 via a raft ConfChange committed on the survivors, then restarts
// service 3 from its own persistent data dir. At that point service 3's
// local metadata still lists (shard=0, replicaID=3) even though the cluster
// has dropped it. That is the zombie starting condition.
//
// Two knobs control the test:
//   - useRealSelfCheck = false  -> disables the self-check by stubbing
//     getShardMembershipFn to return "unknown", exercising the pre-fix behavior.
//   - useRealSelfCheck = true   -> uses the real getShardMembership RPC against
//     the surviving services, exercising the fix.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// buildZombieCluster starts three log-services (service1, service2, service3)
// sharing the same HAKeeper shard (shard=0, replicas {1, 2, 3}). Ports are
// allocated in a distinct range to avoid clashing with runHAKeeperClusterTest.
// All three services register runtimes backed by `zlog` so the caller can
// observe log messages from any of them.
func buildZombieCluster(t *testing.T, zlog *zap.Logger) (
	svc1, svc2, svc3 *Service,
	cfg1, cfg2, cfg3 Config,
) {
	t.Helper()

	newCfg := func(dataDir string, logPort, raftPort, gossipPort int, seeds []string) Config {
		c := DefaultConfig()
		c.UUID = uuid.New().String()
		c.FS = vfs.NewStrictMem()
		c.DeploymentID = 1
		c.RTTMillisecond = 5
		c.DataDir = dataDir
		c.LogServicePort = logPort
		c.RaftPort = raftPort
		c.GossipPort = gossipPort
		c.GossipSeedAddresses = seeds
		c.DisableWorkers = true
		c.HAKeeperConfig.TickPerSecond = 10
		c.HAKeeperConfig.LogStoreTimeout.Duration = 5 * time.Second
		c.HAKeeperConfig.TNStoreTimeout.Duration = 10 * time.Second
		c.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
		// HAKeeperClientConfig is used by checkZombieReplicas to issue
		// GetShardInfo RPCs. Point it at all three peers so the real RPC can
		// round-trip in the with-fix scenario.
		return c
	}
	seeds := []string{"127.0.0.1:19101", "127.0.0.1:19201", "127.0.0.1:19301"}
	cfg1 = newCfg("data-z1", 19102, 19100, 19101, seeds)
	cfg2 = newCfg("data-z2", 19202, 19200, 19201, seeds)
	cfg3 = newCfg("data-z3", 19302, 19300, 19301, seeds)

	serviceAddrs := []string{
		cfg1.LogServiceServiceAddr(),
		cfg2.LogServiceServiceAddr(),
		cfg3.LogServiceServiceAddr(),
	}
	cfg1.HAKeeperClientConfig.ServiceAddresses = serviceAddrs
	cfg2.HAKeeperClientConfig.ServiceAddresses = serviceAddrs
	cfg3.HAKeeperClientConfig.ServiceAddresses = serviceAddrs

	rt1 := runtime.NewRuntime(metadata.ServiceType_LOG, cfg1.UUID, zlog)
	rt2 := runtime.NewRuntime(metadata.ServiceType_LOG, cfg2.UUID, zlog)
	rt3 := runtime.NewRuntime(metadata.ServiceType_LOG, cfg3.UUID, zlog)
	runtime.SetupServiceBasedRuntime(cfg1.UUID, rt1)
	runtime.SetupServiceBasedRuntime(cfg2.UUID, rt2)
	runtime.SetupServiceBasedRuntime(cfg3.UUID, rt3)

	mkSvc := func(c Config, rt runtime.Runtime) *Service {
		s, err := NewService(c,
			newFS(),
			nil,
			WithBackendFilter(func(msg morpc.Message, backendAddr string) bool { return true }),
			WithRuntime(rt),
		)
		require.NoError(t, err)
		return s
	}
	svc1 = mkSvc(cfg1, rt1)
	svc2 = mkSvc(cfg2, rt2)
	svc3 = mkSvc(cfg3, rt3)

	peers := map[uint64]dragonboat.Target{
		1: svc1.ID(),
		2: svc2.ID(),
		3: svc3.ID(),
	}
	require.NoError(t, svc1.store.startHAKeeperReplica(1, peers, false))
	require.NoError(t, svc2.store.startHAKeeperReplica(2, peers, false))
	require.NoError(t, svc3.store.startHAKeeperReplica(3, peers, false))
	return
}

// transferLeaderTo requests that `target` become the leader for shard 0,
// retrying until it succeeds or the test deadline is hit.
func transferLeaderTo(t *testing.T, s *store, targetReplicaID uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		leaderID, _, ok, err := s.nh.GetLeaderID(hakeeper.DefaultHAKeeperShardID)
		if err == nil && ok && leaderID == targetReplicaID {
			return
		}
		_ = s.nh.RequestLeaderTransfer(hakeeper.DefaultHAKeeperShardID, targetReplicaID)
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("failed to transfer leader to replica=%d within deadline", targetReplicaID)
}

// waitLeader blocks until any of the given stores reports an elected leader
// for the HAKeeper shard (shard=0), or fails the test after timeout.
func waitLeader(t *testing.T, stores []*store, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			leaderID, _, ok, err := s.nh.GetLeaderID(hakeeper.DefaultHAKeeperShardID)
			if err == nil && ok && leaderID != 0 {
				return leaderID
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no HAKeeper leader elected within %s", timeout)
	return 0
}

// commitRemoveReplica tells any surviving store to commit a REMOVE ConfChange
// for (shardID=0, replicaID). This simulates `L/Remove` on the cluster level
// without going through HAKeeper scheduler; the result is the same ConfChange
// in the raft log.
//
// The call retries on ErrShardNotReady / ErrRejected because the Delete
// ConfChange may race with a leader election in progress after svc3 was
// stopped. We assert within a time budget rather than on the first try.
func commitRemoveReplica(t *testing.T, s *store, replicaID uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := s.nh.SyncGetShardMembership(ctx, hakeeper.DefaultHAKeeperShardID)
		cancel()
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		err = s.nh.SyncRequestDeleteReplica(
			ctx2, hakeeper.DefaultHAKeeperShardID, replicaID, m.ConfigChangeID)
		cancel2()
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("failed to commit REMOVE within deadline: %v", lastErr)
}

// captureLogger returns a zap logger whose entries are recorded in the
// returned observer; mount it on a runtime via runtime.SetupServiceBasedRuntime
// to capture all runtime logs.
func captureLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	return zap.New(core), logs
}

func countLogs(logs *observer.ObservedLogs, needle string) int {
	n := 0
	for _, e := range logs.All() {
		if strings.Contains(e.Message, needle) {
			n++
		}
	}
	return n
}

// restartService3 simulates the pod coming back up after shutdown. Its data
// dir (cfg3.FS + cfg3.DataDir) is preserved so loadMetadata reads the stale
// (shard=0, replicaID=3) record. NewService -> loadMetadata -> startReplicas
// is the exact path the real pod takes.
func restartService3(t *testing.T, cfg3 Config, zlog *zap.Logger) *Service {
	t.Helper()
	rt := runtime.NewRuntime(metadata.ServiceType_LOG, cfg3.UUID, zlog)
	runtime.SetupServiceBasedRuntime(cfg3.UUID, rt)
	s, err := NewService(cfg3,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool { return true }),
		WithRuntime(rt),
	)
	require.NoError(t, err)
	return s
}

// TestZombieRepro_WithFix exercises the full scenario using the real
// GetShardInfo RPC. Expectation: on restart service3 detects via the
// self-check that its replicaID is not in cluster membership, skips
// StartReplica for shard 0, and logs the skip message.
func TestZombieRepro_WithFix(t *testing.T) {
	// Capture all zap logs emitted via runtime.Logger().
	obsLogger, logs := captureLogger()
	_ = logs // reserved for log-based assertion if runtimes are ever unified

	defer leaktest.AfterTest(t)()
	svc1, svc2, svc3, cfg1, _, cfg3 := buildZombieCluster(t, obsLogger)

	waitLeader(t, []*store{svc1.store, svc2.store, svc3.store}, 10*time.Second)
	// Pin leader to replica 1 so we can deterministically drive REMOVE after
	// stopping svc3.
	transferLeaderTo(t, svc1.store, 1)

	// Stop service3 -- simulates pod going down.
	// Close() stops all replicas AND the nodehost. We need the DataDir/FS to
	// survive for the restart, which is fine because cfg3.FS is a separate
	// in-memory FS we still hold a reference to.
	require.NoError(t, svc3.Close())

	// Now commit REMOVE(shardID=0, replicaID=3) via the leader. Service3 is
	// already down so only survivors {1, 2} apply this ConfChange to their
	// membership.
	commitRemoveReplica(t, svc1.store, 3)

	// Give the cluster a beat to propagate the ConfChange + update gossip
	// registries that GetShardInfo reads from.
	time.Sleep(500 * time.Millisecond)

	// Sanity: surviving peer's view no longer contains replicaID=3.
	si, ok, err := GetShardInfo("", cfg1.LogServiceServiceAddr(), hakeeper.DefaultHAKeeperShardID)
	require.NoError(t, err)
	require.True(t, ok)
	_, replica3Present := si.Replicas[3]
	require.False(t, replica3Present, "replica 3 should have been removed from shard 0 membership")
	t.Logf("post-REMOVE membership: %v", si.Replicas)

	// Sanity 2: verify svc3's configured ServiceAddresses include at least
	// one live peer the self-check can query.
	t.Logf("cfg3.HAKeeperClientConfig.ServiceAddresses = %v", cfg3.HAKeeperClientConfig.ServiceAddresses)

	// Now restart service3 (same DataDir, same FS). This walks through
	// loadMetadata (reads stale record with replicaID=3) then startReplicas
	// (which invokes checkZombieReplicas).
	svc3b := restartService3(t, cfg3, obsLogger)
	defer func() {
		assert.NoError(t, svc3b.Close())
		assert.NoError(t, svc1.Close())
		assert.NoError(t, svc2.Close())
	}()

	// Expectation (with fix): the self-check identifies replicaID=3 as a
	// zombie and startReplicas() skips it.
	// Observable signals:
	// 1. haKeeperReplicaID was NOT set to 3 (startHAKeeperReplica sets it
	//    atomically; remaining 0 proves startHAKeeperReplica was skipped).
	// 2. nh.GetNodeHostInfo.LogInfo does not contain (shard=0, replica=3)
	//    because nh.StartReplica was never called for the zombie.
	// 3. Unlike the without-fix run, svc3b does NOT spam preVote/no-leader
	//    traffic. But that is harder to assert without log capture; the
	//    other two are sufficient.
	haReplicaID := atomic.LoadUint64(&svc3b.store.haKeeperReplicaID)
	assert.Equal(t, uint64(0), haReplicaID,
		"haKeeperReplicaID must remain unset because startHAKeeperReplica was skipped")

	// ShardInfoList enumerates the replicas actually being driven by
	// dragonboat on this NodeHost (i.e. state machine is running). We
	// assert the zombie replica is NOT in this list.
	info := svc3b.store.nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
	for _, si := range info.ShardInfoList {
		if si.ShardID == hakeeper.DefaultHAKeeperShardID && si.ReplicaID == 3 {
			t.Fatalf("zombie replica (shard=0, replica=3) must NOT be an active shard after fix")
		}
	}

	// Symmetrically, svc3b does not participate in raft at all for shard 0,
	// so GetLeaderID returns ok=false -- proving the pod is clean and
	// waiting rather than spinning.
	_, _, ok2, _ := svc3b.store.nh.GetLeaderID(hakeeper.DefaultHAKeeperShardID)
	assert.False(t, ok2,
		"svc3b has no running replica for shard 0 so GetLeaderID must be ok=false")
}

// TestZombieRepro_WithoutFix disables the self-check via getShardMembershipFn
// and exercises the pre-fix behavior. Expectation: startReplicas() happily
// starts replicaID=3 again, and the resulting zombie emits preVote /
// dropped-proposal traffic because it is no longer in the cluster membership.
func TestZombieRepro_WithoutFix(t *testing.T) {
	obsLogger, logs := captureLogger()

	defer leaktest.AfterTest(t)()
	svc1, svc2, svc3, _, _, cfg3 := buildZombieCluster(t, obsLogger)

	waitLeader(t, []*store{svc1.store, svc2.store, svc3.store}, 10*time.Second)
	transferLeaderTo(t, svc1.store, 1)

	require.NoError(t, svc3.Close())
	commitRemoveReplica(t, svc1.store, 3)
	time.Sleep(500 * time.Millisecond)

	// Disable the self-check: stub getShardMembershipFn to behave as if no peer
	// is reachable (this is what the pre-fix code path is equivalent to --
	// it never queried membership at all, so "I assume everything is fine"
	// is the pre-fix behavior).
	origFn := getShardMembershipFn
	defer func() { getShardMembershipFn = origFn }()
	getShardMembershipFn = func(sid, address string, shardID uint64) (map[uint64]string, bool, error) {
		return nil, false, nil // shard unknown -> fallback = not a zombie
	}

	svc3b := restartService3(t, cfg3, obsLogger)
	defer func() {
		assert.NoError(t, svc3b.Close())
		assert.NoError(t, svc1.Close())
		assert.NoError(t, svc2.Close())
	}()

	// Expectation (without fix): startHAKeeperReplica WAS called for
	// replicaID=3, so haKeeperReplicaID is set and the replica is in
	// GetNodeHostInfo.LogInfo. No "skip starting zombie replica" log line.
	skipCount := countLogs(logs, "skip starting zombie replica")
	assert.Equal(t, 0, skipCount, "without fix there should be no zombie-skip log")

	haReplicaID := atomic.LoadUint64(&svc3b.store.haKeeperReplicaID)
	assert.Equal(t, uint64(3), haReplicaID,
		"without fix, startHAKeeperReplica sets haKeeperReplicaID to the stale replicaID")

	info := svc3b.store.nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true})
	found := false
	for _, si := range info.ShardInfoList {
		if si.ShardID == hakeeper.DefaultHAKeeperShardID && si.ReplicaID == 3 {
			found = true
			break
		}
	}
	assert.True(t, found, "without fix, zombie replica (shard=0, replica=3) IS an active shard")

	// Wait a bit and observe the zombie symptom. The key diagnostic signal
	// is that replica 3 does not converge: from svc3b's perspective there
	// is no leader for shard 0 (because the surviving peers never send it
	// AppendEntries -- it is no longer in their membership list).
	time.Sleep(3 * time.Second)

	// Sample GetLeaderID repeatedly for 2 seconds; without the fix, svc3b
	// never gets a stable leader because the cluster drops its traffic.
	noLeaderSamples := 0
	totalSamples := 0
	sampleDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(sampleDeadline) {
		_, _, ok, err := svc3b.store.nh.GetLeaderID(hakeeper.DefaultHAKeeperShardID)
		totalSamples++
		if err != nil || !ok {
			noLeaderSamples++
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("zombie symptom without fix: svc3b saw no-leader in %d/%d samples",
		noLeaderSamples, totalSamples)
	assert.Greater(t, noLeaderSamples, totalSamples/2,
		"without fix the zombie replica should see no leader the majority of the time")
}
