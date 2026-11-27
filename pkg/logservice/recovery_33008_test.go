// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

// A real disk-backed three-store Raft group. The retired store never returns:
// a surviving store must reopen its existing replica from an actual command.
func TestRecoverExistingReplicaWithoutRetiredStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	var configs [3]Config
	var services [3]*Service
	members := make(map[uint64]string)
	seeds := make([]string, 0, len(configs))
	for i := range configs {
		configs[i] = getServiceTestConfig()
		configs[i].RTTMillisecond = 100
		configs[i].DataDir = t.TempDir()
		configs[i].SnapshotExportDir = filepath.Join(configs[i].DataDir, "exported-snapshot")
		configs[i].FS = vfs.Default
		configs[i].LogDBDisablePrealloc = true
		configs[i].RPC.MaxMessageSize = DefaultConfig().RPC.MaxMessageSize
		seeds = append(seeds, getTestGossipAddress(configs[i].GossipPort))
		members[uint64(i+1)] = configs[i].UUID
	}
	for i := range configs {
		configs[i].GossipSeedAddresses = seeds
		configs[i].HAKeeperClientConfig.ServiceAddresses = nil
		for j := range configs {
			configs[i].HAKeeperClientConfig.ServiceAddresses = append(configs[i].HAKeeperClientConfig.ServiceAddresses, getTestServiceAddress(configs[j].LogServicePort))
		}
	}
	open := func(i int) {
		t.Helper()
		s, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = s
	}
	for i := range configs {
		open(i)
		t.Cleanup(func() {
			if services[i] != nil {
				require.NoError(t, services[i].Close())
			}
		})
	}
	waitForRecoveryPeers(t, ctx, services[:])
	for i, service := range services {
		require.NoError(t, service.store.startHAKeeperReplica(uint64(i+1), members, false))
	}
	readMembership := func(s *Service, shard uint64, expected map[uint64]string) *dragonboat.Membership {
		t.Helper()
		var membership *dragonboat.Membership
		waitForRecovery(t, ctx, "exact Raft membership", 12*time.Second, 300*time.Millisecond, func(attempt context.Context) bool {
			var err error
			membership, err = s.store.nh.SyncGetShardMembership(attempt, shard)
			return err == nil && maps.Equal(membership.Nodes, expected)
		})
		return membership
	}
	a, b, c := services[0], services[1], services[2]
	readMembership(a, 0, members)
	// Initial members may be observed by HAKeeper one at a time. Each
	// bootstrap command must start without waiting for the full quorum.
	dataMembers := map[uint64]string{11: a.ID(), 12: b.ID(), 13: c.ID()}
	for i, service := range []*Service{a, b, c} {
		id := uint64(i + 11)
		service.handleCommands([]pb.ScheduleCommand{{UUID: service.ID(), ServiceType: pb.LogService, Bootstrapping: true,
			ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
				Replica: pb.Replica{UUID: service.ID(), ShardID: 1, ReplicaID: id}, InitialMembers: dataMembers}}})
	}
	readMembership(a, 1, dataMembers)
	old, err := a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	require.NoError(t, err, "initial bootstrap must form a writable three-node group")
	clientConfig := configs[2].GetHAKeeperClientConfig()
	clientConfig.ServiceAddresses = []string{configs[0].LogServiceServiceAddr()}
	var client LogHAKeeperClient
	waitForRecovery(t, ctx, "HAKeeper client", 10*time.Second, time.Second, func(attempt context.Context) bool {
		var err error
		client, err = NewLogHAKeeperClient(attempt, c.ID(), clientConfig)
		return err == nil
	})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	for i, service := range services {
		hb := recoveryHeartbeat(t, ctx, service, 1, uint64(i+11), dataMembers)
		_, err := client.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
	}
	state, err := client.GetClusterState(ctx)
	require.NoError(t, err)
	require.Equal(t, dataMembers, state.LogState.Shards[1].Replicas)
	require.NoError(t, client.Close())

	// Preserve C's Dragonboat files while modeling missing MO replica metadata.
	require.NoError(t, b.Close())
	services[1] = nil
	require.NoError(t, c.store.stopReplica(1, 13))
	c.store.removeMetadata(1, 13)
	require.NoError(t, c.Close())
	services[2] = nil
	open(2)
	c = services[2]
	require.True(t, c.store.nh.HasNodeInfo(1, 13))
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound)
	attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	stop()
	require.Error(t, err, "one surviving data voter must not commit")
	waitForRecoveryPeers(t, ctx, []*Service{a, c})
	readMembership(c, 0, members)
	waitForRecovery(t, ctx, "reopened HAKeeper client", 10*time.Second, time.Second, func(attempt context.Context) bool {
		var err error
		c.haClient, err = NewLogHAKeeperClient(attempt, c.ID(), clientConfig)
		return err == nil
	})
	_, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	wrong := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 12, UUID: c.ID()}}}
	var term uint64
	waitForRecovery(t, ctx, "HAKeeper leader", 10*time.Second, time.Second, func(context.Context) bool {
		for _, service := range []*Service{a, c} {
			leader, current, err := service.store.isLeaderHAKeeper()
			if err == nil && leader {
				term = current
				return true
			}
		}
		return false
	})
	_, err = a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(0), hakeeper.GetUpdateCommandsCmd(term, []pb.ScheduleCommand{wrong}))
	require.NoError(t, err)
	delivered, err := c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	require.Empty(t, delivered.Commands, "a different store's identity must be rejected at delivery")
	c.handleCommands(delivered.Commands)
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound, "another voter's ID must not be resurrected")
	start := wrong
	start.ConfigChange = &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 13, UUID: c.ID()}}
	shutdown := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: c.ID()}}
	_, err = a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(0), hakeeper.GetUpdateCommandsCmd(term, []pb.ScheduleCommand{shutdown, start}))
	require.NoError(t, err)
	delivered, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	require.Len(t, delivered.Commands, 2)
	require.NotZero(t, delivered.Commands[1].ConfigChange.Replica.Epoch)
	c.handleCommands(delivered.Commands)
	readMembership(c, 1, dataMembers)
	var next uint64
	waitForRecovery(t, ctx, "post-recovery write", 10*time.Second, 500*time.Millisecond, func(attempt context.Context) bool {
		result, err := a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
		next = result.Value
		return err == nil && next > old.Value
	})
	waitForRecovery(t, ctx, "post-recovery read", 10*time.Second, 300*time.Millisecond, func(attempt context.Context) bool {
		lsn, err := c.store.getLatestLsn(attempt, 1)
		return err == nil && lsn >= next
	})
}

func useRecoveryRPCSocket(t *testing.T, cfg *Config) {
	t.Helper()
	dir, err := os.MkdirTemp("", "mo-log-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	socket := filepath.Join(dir, "rpc.sock")
	// Darwin's sockaddr_un has room for 103 pathname bytes. On Linux a
	// directory descriptor gives the in-process peers a short alias while
	// keeping the socket in the test's own temp directory.
	if len(socket) > 103 {
		handle, err := os.Open(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, handle.Close()) })
		alias := fmt.Sprintf("/proc/self/fd/%d", handle.Fd())
		if _, err := os.Stat(alias); err != nil {
			return // Keep the test's loopback TCP endpoint without procfs.
		}
		socket = filepath.Join(alias, "rpc.sock")
	}
	addr := "unix://" + socket
	cfg.LogServicePort = 0
	cfg.ServiceListenAddress = addr
	cfg.ServiceAddress = addr
}

// Run readiness probes synchronously so a failed phase cannot leave a probe
// running during service cleanup.
func waitForRecoveryResult(t *testing.T, ctx context.Context, phase string, timeout, attemptTimeout time.Duration, probe func(context.Context) error) {
	t.Helper()
	waitForRecovery(t, ctx, phase, timeout, attemptTimeout, func(ctx context.Context) bool {
		return probe(ctx) == nil
	})
}

func waitForRecoveryPair(t *testing.T, ctx context.Context, timeout time.Duration, a, b *Service) {
	t.Helper()
	waitForRecoveryResult(t, ctx, "discover both Raft peers", timeout, 30*time.Millisecond, func(context.Context) error {
		for _, pair := range [2][2]*Service{{a, b}, {b, a}} {
			local, peer := pair[0], pair[1]
			registry, ok := local.store.nh.GetNodeHostRegistry()
			if !ok {
				return fmt.Errorf("store %s has no registry", local.ID())
			}
			meta, ok := registry.GetMeta(peer.ID())
			if !ok || string(meta) != peer.cfg.LogServiceServiceAddr() {
				return fmt.Errorf("store %s has not discovered %s", local.ID(), peer.ID())
			}
		}
		return nil
	})
}

// A real disk-backed three-store Raft group. The retired store never returns:
// a surviving store must reopen its existing replica from an actual command.
func TestNonVotingStartReopensDisk(t *testing.T) {
	cfg := getServiceTestConfig()
	useRecoveryRPCSocket(t, &cfg)
	cfg.DataDir = t.TempDir()
	cfg.SnapshotExportDir = filepath.Join(cfg.DataDir, "exported-snapshot")
	cfg.FS = vfs.Default
	cfg.LogDBDisablePrealloc = true
	var service *Service
	open := func() {
		t.Helper()
		var err error
		service, err = NewService(cfg, newFS(), nil)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		if service != nil {
			require.NoError(t, service.Close())
		}
	})
	open()
	start := pb.ScheduleCommand{UUID: service.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartNonVotingReplica,
			Replica: pb.Replica{UUID: service.ID(), ShardID: 1, ReplicaID: 2}}}
	service.handleCommands([]pb.ScheduleCommand{start})
	require.True(t, service.store.nh.HasNodeInfo(1, 2))
	require.NoError(t, service.store.stopReplica(1, 2))
	service.store.removeMetadata(1, 2)
	require.NoError(t, service.Close())
	service = nil

	open()
	require.True(t, service.store.nh.HasNodeInfo(1, 2), "the original Raft bootstrap must remain on disk")
	service.handleCommands([]pb.ScheduleCommand{start})
	_, err := service.store.nh.GetNodeUser(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound, "an old Start cannot reopen an orphaned bootstrap")
	start.ConfigChange.Replica.Epoch = 1 // Simulate a confirmed heartbeat reply for the same non-voter.
	service.handleCommands([]pb.ScheduleCommand{start})
	var found bool
	for _, local := range service.store.nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true}).ShardInfoList {
		found = found || (local.ShardID == 1 && local.ReplicaID == 2)
	}
	require.True(t, found, "the non-voter must reopen its original Raft state")
	shards := service.store.getShards()
	require.Len(t, shards, 1)
	require.Equal(t, uint64(1), shards[0].ShardID)
	require.Equal(t, uint64(2), shards[0].ReplicaID)
	require.True(t, shards[0].NonVoting)
}

func TestInitialLogBootstrapReopensBeforeMetadataWasSaved(t *testing.T) {
	cfg := getServiceTestConfig()
	useRecoveryRPCSocket(t, &cfg)
	cfg.DataDir = t.TempDir()
	cfg.SnapshotExportDir = filepath.Join(cfg.DataDir, "exported")
	cfg.FS = vfs.Default
	cfg.LogDBDisablePrealloc = true
	cfg.HAKeeperClientConfig.ServiceAddresses = nil

	var service *Service
	open := func() {
		t.Helper()
		var err error
		service, err = NewService(cfg, newFS(), nil)
		require.NoError(t, err)
	}
	open()
	t.Cleanup(func() {
		if service != nil {
			require.NoError(t, service.Close())
		}
	})

	const (
		phaseTimeout = 30 * time.Second
		control      = hakeeper.DefaultHAKeeperShardID
		shard        = uint64(1)
		replica      = uint64(2)
	)
	require.NoError(t, service.store.startHAKeeperReplica(1, map[uint64]string{1: cfg.UUID}, false))
	var term uint64
	hasLeader := func() bool {
		leader, observed, valid, err := service.store.nh.GetLeaderID(control)
		term = observed
		return err == nil && valid && leader == 1
	}
	require.Eventually(t, hasLeader, phaseTimeout, 25*time.Millisecond)
	// Give each operation its own budget. Reopening the on-disk store
	// must not use up the time available for the recovery assertions.
	propose := func(shardID uint64, command []byte) uint64 {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), phaseTimeout)
		defer cancel()
		session := service.store.nh.GetNoOPSession(shardID)
		result, err := service.store.propose(ctx, session, command)
		require.NoError(t, err, "propose on shard %d", shardID)
		return result.Value
	}
	initial := hakeeper.GetInitialClusterRequestCmd(1, 1, 1, 100, nil, nil)
	require.Equal(t, uint64(pb.HAKeeperCreated), propose(control, initial))

	members := map[uint64]string{replica: cfg.UUID}
	start := pb.ScheduleCommand{
		UUID: cfg.UUID, ServiceType: pb.LogService, Bootstrapping: true,
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StartReplica, InitialMembers: members,
			Replica: pb.Replica{UUID: cfg.UUID, ShardID: shard, ReplicaID: replica},
		},
	}
	propose(control, hakeeper.GetUpdateCommandsCmd(term, []pb.ScheduleCommand{start}))

	// Leave the files produced by Dragonboat before openDataReplica can
	// save the new replica in MatrixOne's metadata.
	require.NoError(t, service.store.nh.StartReplica(members, false, newStateMachine, getRaftConfig(shard, replica)))
	require.Equal(t, int64(-1), service.store.getReplicaID(shard))
	require.NoError(t, service.Close())
	service = nil
	open()
	require.Eventually(t, hasLeader, phaseTimeout, 25*time.Millisecond)
	require.True(t, service.store.nh.HasNodeInfo(shard, replica))
	require.Equal(t, int64(-1), service.store.getReplicaID(shard))

	refuses := func(command pb.ScheduleCommand) {
		t.Helper()
		service.handleCommands([]pb.ScheduleCommand{command})
		_, err := service.store.nh.GetNodeUser(shard)
		require.ErrorIs(t, err, dragonboat.ErrShardNotFound)
		require.Equal(t, int64(-1), service.store.getReplicaID(shard))
	}
	ordinary := start
	ordinary.Bootstrapping = false
	refuses(ordinary)

	wrong := start
	change := *start.ConfigChange
	wrong.ConfigChange = &change
	change.InitialMembers = map[uint64]string{replica: "different-store"}
	refuses(wrong)
	// Dragonboat validates the complete set even when the recipient matches.
	change.InitialMembers = map[uint64]string{replica: cfg.UUID, 3: cfg.UUID}
	refuses(wrong)

	key := zombieKey{shardID: shard, replicaID: replica}
	markAbsent := func(epoch uint64) {
		service.store.mu.Lock()
		service.store.mu.skippedZombies[key] = startupAbsence{epoch: epoch}
		service.store.mu.Unlock()
	}
	threeVoters := map[uint64]string{1: "first", replica: cfg.UUID, 3: "third"}
	markAbsent(1)
	require.True(t, service.store.canStartScheduledReplica(start.ConfigChange.Replica, false, true, threeVoters))
	markAbsent(2)
	require.False(t, service.store.canStartScheduledReplica(start.ConfigChange.Replica, false, true, threeVoters))
	markAbsent(8)
	refuses(start)
	service.store.mu.Lock()
	delete(service.store.mu.skippedZombies, key)
	service.store.mu.Unlock()

	dialCtx, cancelDial := context.WithTimeout(t.Context(), phaseTimeout)
	client, err := NewLogHAKeeperClient(dialCtx, cfg.UUID, HAKeeperClientConfig{
		ServiceAddresses: []string{cfg.LogServiceServiceAddr()},
	})
	cancelDial()
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	heartbeat := func() pb.CommandBatch {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), phaseTimeout)
		defer cancel()
		batch, err := client.SendLogHeartbeat(ctx, service.store.getHeartbeatMessage())
		require.NoError(t, err)
		return batch
	}
	batch := heartbeat()
	require.Equal(t, []pb.ScheduleCommand{start}, batch.Commands)
	service.handleCommands(batch.Commands)
	require.Equal(t, int64(replica), service.store.getReplicaID(shard))

	require.Eventually(t, func() bool {
		leader, _, valid, err := service.store.nh.GetLeaderID(shard)
		return err == nil && valid && leader == replica
	}, phaseTimeout, 25*time.Millisecond, "the recovered data shard must elect a leader")
	propose(shard, getTestUserEntry())
	batch = heartbeat()
	require.Empty(t, batch.Commands, "the retained Start is acknowledged by the recovered replica")
}

func TestInitialHAKeeperOrphanRestoresQuorum(t *testing.T) {
	const (
		shard        = hakeeper.DefaultHAKeeperShardID
		phaseTimeout = 60 * time.Second
	)
	var configs [3]Config
	var services [3]*Service
	members := make(map[uint64]string, len(configs))
	var seeds, bootstrap, addresses [3]string
	for i := range configs {
		cfg := getServiceTestConfig()
		useRecoveryRPCSocket(t, &cfg)
		cfg.DataDir = t.TempDir()
		cfg.SnapshotExportDir = filepath.Join(cfg.DataDir, "exported")
		cfg.FS = vfs.Default
		// Leave election headroom for disk and scheduling delays under -race.
		cfg.RTTMillisecond = 200
		cfg.LogDBDisablePrealloc = true
		configs[i] = cfg
		id := uint64(i + 1)
		members[id] = cfg.UUID
		seeds[i] = getTestGossipAddress(cfg.GossipPort)
		addresses[i] = cfg.LogServiceServiceAddr()
		bootstrap[i] = fmt.Sprintf("%d:%s", id, cfg.UUID)
	}
	for i := range configs {
		cfg := &configs[i]
		cfg.GossipSeedAddresses = seeds[:]
		cfg.HAKeeperClientConfig.ServiceAddresses = addresses[:]
		cfg.BootstrapConfig.BootstrapCluster = true
		cfg.BootstrapConfig.InitHAKeeperMembers = bootstrap[:]
		cfg.BootstrapConfig.NumOfLogShards = 1
		cfg.BootstrapConfig.NumOfTNShards = 1
		cfg.BootstrapConfig.NumOfLogShardReplicas = 3
	}

	open := func(i int) {
		t.Helper()
		s, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = s
	}
	closeStore := func(i int) {
		t.Helper()
		if s := services[i]; s != nil {
			services[i] = nil
			require.NoError(t, s.Close())
		}
	}
	for i := range configs {
		open(i)
		t.Cleanup(func() { closeStore(i) })
	}

	readMembership := func(i int) *dragonboat.Membership {
		t.Helper()
		nh := services[i].store.nh
		var membership *dragonboat.Membership
		phase := fmt.Sprintf("read HAKeeper membership from voter %d", i+1)
		waitForRecoveryResult(t, t.Context(), phase, phaseTimeout, 5*time.Second, func(ctx context.Context) error {
			leader, _, ok, err := nh.GetLeaderID(shard)
			if err != nil {
				return err
			}
			if !ok || leader == 0 {
				return fmt.Errorf("no elected leader")
			}
			attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			membership, err = nh.SyncGetShardMembership(attempt, shard)
			return err
		})
		return membership
	}
	waitForRecoveryPair(t, t.Context(), phaseTimeout, services[0], services[1])
	require.NoError(t, services[0].store.startHAKeeperReplica(1, members, false))
	require.NoError(t, services[1].store.startHAKeeperReplica(2, members, false))
	require.Equal(t, members, readMembership(1).Nodes)

	// Dragonboat persists its bootstrap before MatrixOne writes its metadata.
	native := services[2].store.nh
	require.NoError(t, native.StartReplica(members, false, hakeeper.NewStateMachine, getRaftConfig(shard, 3)))
	require.True(t, native.HasNodeInfo(shard, 3))
	require.Equal(t, int64(-1), services[2].store.getReplicaID(shard))
	closeStore(2)
	closeStore(0)

	prefix := pb.ShardInfoQueryResult{ShardID: shard, Epoch: 2, Replicas: map[uint64]pb.ReplicaInfo{
		1: {UUID: members[1]}, 2: {UUID: members[2]},
	}}
	var retired pb.ShardInfoQueryResult
	originalQuery := queryShardInfoRawFn
	t.Cleanup(func() { queryShardInfoRawFn = originalQuery })
	queryShardInfoRawFn = func(ctx context.Context, sid, address string, shardID uint64, includeExpired, excludeHardDown bool) (pb.ShardInfoQueryResult, bool, error) {
		if sid != configs[2].UUID || shardID != shard {
			return originalQuery(ctx, sid, address, shardID, includeExpired, excludeHardDown)
		}
		switch address {
		case addresses[1]:
			return prefix, true, nil
		case addresses[0]:
			return retired, retired.Epoch != 0, nil
		}
		return pb.ShardInfoQueryResult{}, false, nil
	}
	bootstrapStore := func() {
		t.Helper()
		s := services[2]
		require.True(t, s.store.nh.HasNodeInfo(shard, 3))
		require.Equal(t, int64(-1), s.store.getReplicaID(shard))
		ctx, cancel := context.WithTimeout(t.Context(), phaseTimeout)
		defer cancel()
		require.NoError(t, s.BootstrapHAKeeper(ctx, configs[2]))
	}
	open(2)
	waitForRecoveryPair(t, t.Context(), phaseTimeout, services[1], services[2])
	bootstrapStore()
	require.Equal(t, int64(3), services[2].store.getReplicaID(shard), "the valid voter must reopen")
	require.Equal(t, members, readMembership(1).Nodes)

	// A newer peer view proves retirement even when another peer has a prefix.
	open(0)
	waitForRecoveryPair(t, t.Context(), phaseTimeout, services[0], services[1])
	services[2].store.removeMetadata(shard, 3)
	closeStore(2)
	require.Equal(t, members, readMembership(1).Nodes)
	commitRemoveReplica(t, services[1].store, 3)
	membership := readMembership(1)
	require.NotContains(t, membership.Nodes, uint64(3))
	require.Greater(t, membership.ConfigChangeID, uint64(len(members)))
	retired = pb.ShardInfoQueryResult{ShardID: shard, Epoch: membership.ConfigChangeID, Replicas: map[uint64]pb.ReplicaInfo{
		1: {UUID: membership.Nodes[1]}, 2: {UUID: membership.Nodes[2]},
	}}
	for _, oldView := range []pb.ShardInfoQueryResult{
		prefix,
		{ShardID: shard, Epoch: uint64(len(members)), Replicas: map[uint64]pb.ReplicaInfo{
			1: {UUID: members[1]}, 2: {UUID: members[2]}, 3: {UUID: members[3]},
		}},
	} {
		prefix = oldView
		open(2)
		bootstrapStore()
		_, err := services[2].store.nh.GetNodeUser(shard)
		require.ErrorIs(t, err, dragonboat.ErrShardNotFound, "a later removal overrides older membership")
		require.Equal(t, int64(-1), services[2].store.getReplicaID(shard))
		closeStore(2)
	}
}
