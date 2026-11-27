// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

// Matching stale gossip answers must not override a committed Raft membership.
func TestRestartKeepsLiveReplicaWithMatchingStaleGossipPeers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	closed := [3]bool{}
	defer func() {
		for i, s := range services {
			if !closed[i] {
				require.NoError(t, s.Close())
			}
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()
	peers := map[uint64]dragonboat.Target{1: s1.ID(), 2: s2.ID(), 3: s3.ID()}
	for i, s := range services {
		require.NoError(t, s.store.startReplica(1, uint64(i+1), peers, false))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var membership *dragonboat.Membership
	require.Eventually(t, func() bool {
		var err error
		membership, err = s1.store.nh.SyncGetShardMembership(ctx, 1)
		return err == nil && len(membership.Nodes) == 3
	}, 5*time.Second, 20*time.Millisecond)
	require.Contains(t, membership.Nodes, uint64(3), "replica 3 is a committed voting member")
	require.NoError(t, s3.Close())
	closed[2] = true

	original := getShardMembershipFn
	defer func() { getShardMembershipFn = original }()
	getShardMembershipFn = func(_ context.Context, _, address string, shardID uint64) (map[uint64]string, bool, error) {
		if shardID == hakeeper.DefaultHAKeeperShardID {
			return map[uint64]string{1: cfg1.LogServiceServiceAddr(), 2: cfg2.LogServiceServiceAddr(), 3: cfg3.LogServiceServiceAddr()}, true, nil
		}
		if address == cfg1.LogServiceServiceAddr() || address == cfg2.LogServiceServiceAddr() {
			// Gossip has not yet delivered replica 3's membership entry.
			return map[uint64]string{1: cfg1.LogServiceServiceAddr(), 2: cfg2.LogServiceServiceAddr()}, true, nil
		}
		return nil, false, context.DeadlineExceeded
	}
	originalCommitted := getCommittedShardMembershipFn
	defer func() { getCommittedShardMembershipFn = originalCommitted }()
	secondCommittedRead := false
	getCommittedShardMembershipFn = func(ctx context.Context, sid, address string, shardID uint64) (map[uint64]string, error) {
		if shardID == 1 && address == cfg1.LogServiceServiceAddr() {
			return nil, context.DeadlineExceeded
		}
		if shardID == 1 && address == cfg2.LogServiceServiceAddr() {
			secondCommittedRead = true
		}
		return originalCommitted(ctx, sid, address, shardID)
	}
	s3b := restartService3(t, cfg3, logger)
	services[2] = s3b
	closed[2] = false
	require.True(t, secondCommittedRead)
	require.False(t, s3b.store.isSkippedZombie(1, 3))
	require.True(t, s3b.store.localReplicaPresent(1, 3))
	stillCommitted, err := s1.store.nh.SyncGetShardMembership(ctx, 1)
	require.NoError(t, err)
	require.Contains(t, stillCommitted.Nodes, uint64(3))
	// The same durable directory starts the replica once the real peer view is
	// available. This distinguishes a false startup classification from a
	// removed Raft member or damaged local files.
	require.NoError(t, s3b.Close())
	closed[2] = true
	getShardMembershipFn = original
	getCommittedShardMembershipFn = originalCommitted
	require.Eventually(t, func() bool {
		for _, address := range []string{cfg1.LogServiceServiceAddr(), cfg2.LogServiceServiceAddr()} {
			members, ok, err := getShardMembership(ctx, "", address, 1)
			if err != nil || !ok {
				return false
			}
			if _, present := members[3]; !present {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	s3c := restartService3(t, cfg3, logger)
	services[2] = s3c
	closed[2] = false
	require.False(t, s3c.store.isSkippedZombie(1, 3))
	require.True(t, s3c.store.localReplicaPresent(1, 3))
}

// A configured address is only a discovery seed. If that seed's gossip view
// omits a persisted HAKeeper voter while another reported voter is down, the
// restarting voter must run so the two available voters can regain quorum.
func TestHAKeeperRestartWithSingleStaleSeedRegainsQuorum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	closed := [3]bool{}
	defer func() {
		for i, s := range services {
			if !closed[i] {
				require.NoError(t, s.Close())
			}
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()
	waitLeader(t, []*store{s1.store, s2.store, s3.store}, 10*time.Second)
	transferLeaderTo(t, s2.store, 2)
	require.NoError(t, s3.Close())
	closed[2] = true
	require.NoError(t, s1.Close())
	closed[0] = true

	cfg3.HAKeeperClientConfig.ServiceAddresses = []string{cfg2.LogServiceServiceAddr()}
	original := getShardMembershipFn
	defer func() { getShardMembershipFn = original }()
	var probed []string
	getShardMembershipFn = func(_ context.Context, _, address string, shardID uint64) (map[uint64]string, bool, error) {
		require.Equal(t, hakeeper.DefaultHAKeeperShardID, shardID)
		probed = append(probed, address)
		if address == cfg2.LogServiceServiceAddr() {
			return map[uint64]string{
				1: cfg1.LogServiceServiceAddr(),
				2: cfg2.LogServiceServiceAddr(),
			}, true, nil
		}
		require.Equal(t, cfg1.LogServiceServiceAddr(), address)
		// Two peers can agree on the same view from before replica 3's ADD.
		// Neither gossip answer is a committed membership read.
		return map[uint64]string{
			1: cfg1.LogServiceServiceAddr(),
			2: cfg2.LogServiceServiceAddr(),
		}, true, nil
	}
	s3b := restartService3(t, cfg3, logger)
	services[2] = s3b
	closed[2] = false
	getShardMembershipFn = original
	require.Equal(t, []string{cfg2.LogServiceServiceAddr(), cfg1.LogServiceServiceAddr()}, probed)
	require.False(t, s3b.store.isSkippedZombie(hakeeper.DefaultHAKeeperShardID, 3))
	require.True(t, s3b.store.localReplicaPresent(hakeeper.DefaultHAKeeperShardID, 3))
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := s2.store.getCheckerStateWithContext(ctx)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "voters 2 and 3 should restore HAKeeper reads")
}

// A replacement can share the old replica's store address while the old
// metadata record is still on disk. Restart must use the committed membership
// to start only the replacement, regardless of the record order.
func TestRestartSkipsReplacedReplicaOnSameStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	closed := [3]bool{}
	defer func() {
		for i, s := range services {
			if !closed[i] {
				require.NoError(t, s.Close())
			}
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()
	peers := map[uint64]dragonboat.Target{1: s1.ID(), 2: s2.ID(), 3: s3.ID()}
	for i, s := range services {
		require.NoError(t, s.store.startReplica(1, uint64(i+1), peers, false))
	}
	readMembership := func() (*dragonboat.Membership, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return s1.store.nh.SyncGetShardMembership(ctx, 1)
	}
	var membership *dragonboat.Membership
	require.Eventually(t, func() bool {
		var err error
		membership, err = readMembership()
		return err == nil && len(membership.Nodes) == 3
	}, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		_ = s1.store.nh.RequestLeaderTransfer(1, 1)
		leaderID, _, ok, err := s1.store.nh.GetLeaderID(1)
		return err == nil && ok && leaderID == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, s1.store.removeReplica(1, 3, membership.ConfigChangeID))
	if stopErr := s3.store.stopReplicaRaw(1, 3); stopErr != nil {
		require.True(t, errors.Is(stopErr, dragonboat.ErrShardNotFound),
			"Raft may stop a replica itself after applying its removal: %v", stopErr)
	}
	require.Eventually(t, func() bool {
		var err error
		membership, err = readMembership()
		return err == nil && len(membership.Nodes) == 2 && membership.Nodes[3] == ""
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, s1.store.addReplica(1, 4, s3.ID(), membership.ConfigChangeID))
	require.NoError(t, s3.store.startReplica(1, 4, nil, true))
	require.Eventually(t, func() bool {
		current, readErr := readMembership()
		return readErr == nil && len(current.Nodes) == 3 && current.Nodes[4] == s3.ID()
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, s3.Close())
	closed[2] = true

	// The gossip view names the replacement at this same service address.
	// The quorum-backed read, rather than that address, decides which local
	// metadata record is still a voting member.
	original := getShardMembershipFn
	t.Cleanup(func() { getShardMembershipFn = original })
	getShardMembershipFn = func(_ context.Context, _, address string, shardID uint64) (map[uint64]string, bool, error) {
		if shardID != 1 {
			return nil, false, nil
		}
		return map[uint64]string{
			1: cfg1.LogServiceServiceAddr(),
			2: cfg2.LogServiceServiceAddr(),
			4: cfg3.LogServiceServiceAddr(),
		}, true, nil
	}
	s3b := restartService3(t, cfg3, logger)
	services[2] = s3b
	closed[2] = false
	require.True(t, s3b.store.isSkippedZombie(1, 3))
	require.False(t, s3b.store.isSkippedZombie(1, 4))
	require.False(t, s3b.store.localReplicaPresent(1, 3))
	require.True(t, s3b.store.localReplicaPresent(1, 4))

	// Without a quorum, the membership read cannot classify either local
	// record. The newest persisted voting ID must still start so this store
	// and voter 2 can restore quorum without another restart.
	require.NoError(t, s3b.Close())
	closed[2] = true
	require.NoError(t, s1.Close())
	closed[0] = true
	s3c := restartService3(t, cfg3, logger)
	services[2] = s3c
	closed[2] = false
	require.True(t, s3c.store.isSkippedZombie(1, 3))
	require.True(t, s3c.store.localReplicaPresent(1, 4))
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		current, err := s2.store.nh.SyncGetShardMembership(ctx, 1)
		return err == nil && current.Nodes[4] == s3c.ID()
	}, 10*time.Second, 100*time.Millisecond)
}
