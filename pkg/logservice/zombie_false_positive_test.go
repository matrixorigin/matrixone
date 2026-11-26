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
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

// A single stale gossip answer must not override a committed Raft membership.
func TestRestartKeepsLiveReplicaWhenOnlyStaleGossipPeerAnswers(t *testing.T) {
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
		if address == cfg1.LogServiceServiceAddr() {
			// Gossip has not yet delivered replica 3's membership entry.
			return map[uint64]string{1: cfg1.LogServiceServiceAddr(), 2: cfg2.LogServiceServiceAddr()}, true, nil
		}
		return nil, false, context.DeadlineExceeded
	}
	s3b := restartService3(t, cfg3, logger)
	services[2] = s3b
	closed[2] = false
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
		return nil, false, context.DeadlineExceeded
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
