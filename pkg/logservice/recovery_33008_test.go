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

// A real disk-backed three-store Raft group. The retired store never returns:
// a surviving store must reopen its existing replica from an actual command.
func TestRecoverExistingReplicaWithoutRetiredStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// Run readiness probes in this goroutine so none can outlive a failed
	// phase and race with service cleanup.
	wait := func(phase string, timeout, interval time.Duration, probe func(context.Context) error) {
		t.Helper()
		phaseCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var lastErr error
		for {
			if err := phaseCtx.Err(); err != nil {
				t.Fatalf("%s: %v; last observation: %v", phase, err, lastErr)
			}
			lastErr = probe(phaseCtx)
			if lastErr == nil {
				return
			}
			select {
			case <-phaseCtx.Done():
				t.Fatalf("%s: %v; last observation: %v", phase, phaseCtx.Err(), lastErr)
			case <-ticker.C:
			}
		}
	}
	var configs [3]Config
	var services [3]*Service
	members := make(map[uint64]string)
	seeds := make([]string, 0, len(configs))
	for i := range configs {
		configs[i] = getServiceTestConfig()
		useRecoveryRPCSocket(t, &configs[i])
		// Keep election timeouts above the scheduling delays seen in a race suite.
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
			configs[i].HAKeeperClientConfig.ServiceAddresses = append(configs[i].HAKeeperClientConfig.ServiceAddresses, configs[j].LogServiceServiceAddr())
		}
	}
	open := func(i int) {
		t.Helper()
		s, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = s
	}
	connect := func(s *Service) LogHAKeeperClient {
		t.Helper()
		var client LogHAKeeperClient
		wait("connect to HAKeeper for "+s.ID(), 10*time.Second, 50*time.Millisecond, func(phaseCtx context.Context) error {
			attempt, stop := context.WithTimeout(phaseCtx, time.Second)
			candidate, err := NewLogHAKeeperClient(attempt, s.ID(), configs[2].GetHAKeeperClientConfig())
			stop()
			if err != nil {
				return err
			}
			t.Cleanup(func() { require.NoError(t, candidate.Close()) })
			client = candidate
			return nil
		})
		return client
	}
	for i := range configs {
		open(i)
		t.Cleanup(func() {
			if services[i] != nil {
				require.NoError(t, services[i].Close())
			}
		})
	}
	// The two initial voters need each other's Raft addresses before election.
	wait("initial voters discover each other", 10*time.Second, 30*time.Millisecond, func(context.Context) error {
		for i := 0; i < 2; i++ {
			registry, ok := services[i].store.nh.GetNodeHostRegistry()
			if !ok {
				return fmt.Errorf("store %s has no registry", services[i].ID())
			}
			peer := 1 - i
			meta, ok := registry.GetMeta(services[peer].ID())
			if !ok || string(meta) != configs[peer].LogServiceServiceAddr() {
				return fmt.Errorf("store %s has not discovered %s", services[i].ID(), services[peer].ID())
			}
		}
		return nil
	})
	initialMembers := map[uint64]string{1: members[1], 2: members[2]}
	for i := 0; i < 2; i++ {
		require.NoError(t, services[i].store.startHAKeeperReplica(uint64(i+1), initialMembers, false))
	}
	readMembership := func(s *Service, shard uint64, expected map[uint64]string) *dragonboat.Membership {
		t.Helper()
		var membership *dragonboat.Membership
		phase := fmt.Sprintf("membership of shard %d on %s (want %v)", shard, s.ID(), expected)
		wait(phase, 12*time.Second, 30*time.Millisecond, func(phaseCtx context.Context) error {
			attempt, stop := context.WithTimeout(phaseCtx, time.Second)
			observed, err := s.store.nh.SyncGetShardMembership(attempt, shard)
			stop()
			if err != nil {
				if membership != nil {
					return fmt.Errorf("last members %v at config %d; read failed: %w", membership.Nodes, membership.ConfigChangeID, err)
				}
				return err
			}
			membership = observed
			if !maps.Equal(observed.Nodes, expected) {
				return fmt.Errorf("observed members %v at config %d", observed.Nodes, observed.ConfigChangeID)
			}
			return nil
		})
		return membership
	}
	// C is a previously joined voter, so this also exercises the released
	// branch's ordinary join=true restart path as a regression control.
	initial := readMembership(services[0], 0, initialMembers)
	require.NoError(t, services[0].store.nh.SyncRequestAddReplica(ctx, 0, 3, services[2].ID(), initial.ConfigChangeID))
	require.NoError(t, services[2].store.startHAKeeperReplica(3, nil, true))
	a, b, c := services[0], services[1], services[2]
	readMembership(c, 0, members)
	for i := range services {
		require.NoError(t, services[i].store.startReplica(1, uint64(i+1), members, false))
	}
	readMembership(a, 1, members)
	client := connect(c)
	// Before installing test commands, wait for a stable leader and committed
	// reports from all data voters. A successful client connection alone does
	// not imply that the first heartbeat can be committed.
	wait("initial committed data heartbeats", 12*time.Second, 50*time.Millisecond, func(phaseCtx context.Context) error {
		for _, service := range services {
			attempt, stop := context.WithTimeout(phaseCtx, time.Second)
			_, err := client.SendLogHeartbeat(attempt, service.store.getHeartbeatMessage())
			stop()
			if err != nil {
				return fmt.Errorf("heartbeat from %s: %w", service.ID(), err)
			}
		}
		attempt, stop := context.WithTimeout(phaseCtx, time.Second)
		state, err := client.GetClusterState(attempt)
		stop()
		if err != nil {
			return err
		}
		if !maps.Equal(members, state.LogState.Shards[1].Replicas) {
			return fmt.Errorf("unexpected data membership: %v", state.LogState.Shards[1].Replicas)
		}
		for id, uuid := range members {
			found := false
			for _, replica := range state.LogState.Stores[uuid].Replicas {
				if replica.ShardID == 1 && replica.ReplicaID == id && !replica.IsNonVoting {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("missing data voter %d on %s", id, uuid)
			}
		}
		return nil
	})
	old, err := a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	require.NoError(t, err)

	// A Start delivered while HAKeeper has quorum must still be able to restore
	// that quorum if a second voter goes away before command execution.
	require.NoError(t, c.store.stopReplica(0, 3))
	c.store.removeMetadata(0, 3)
	require.NoError(t, c.Close())
	services[2] = nil
	open(2)
	c = services[2]
	require.True(t, c.store.nh.HasNodeInfo(0, 3))
	c.store.addMetadata(0, 3, false)
	c.haClient = client
	controlStart := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 0, ReplicaID: 3, UUID: c.ID()}}}
	readLeaderTerm := func() uint64 {
		t.Helper()
		var term uint64
		wait("find a HAKeeper leader", 10*time.Second, 30*time.Millisecond, func(context.Context) error {
			for _, service := range []*Service{a, b} {
				leader, current, err := service.store.isLeaderHAKeeper()
				if err == nil && leader {
					term = current
					return nil
				}
			}
			return fmt.Errorf("neither surviving store reports the HAKeeper leader")
		})
		return term
	}
	term := readLeaderTerm()
	_, err = a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(0), hakeeper.GetUpdateCommandsCmd(term, []pb.ScheduleCommand{controlStart}))
	require.NoError(t, err)
	batch, err := client.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	require.Equal(t, []pb.ScheduleCommand{controlStart}, batch.Commands)
	require.NoError(t, b.Close())
	services[1] = nil
	attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = a.store.nh.SyncGetShardMembership(attempt, 0)
	stop()
	require.Error(t, err, "one control voter cannot authorize its own recovery")
	c.handleCommands(batch.Commands)
	readMembership(c, 0, members)
	_, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err, "the restored control quorum must accept replicated writes")

	// Preserve C's Dragonboat files while modeling missing MO replica metadata.
	require.NoError(t, c.store.stopReplica(1, 3))
	c.store.removeMetadata(1, 3)
	require.NoError(t, c.Close())
	services[2] = nil
	open(2)
	c = services[2]
	require.True(t, c.store.nh.HasNodeInfo(1, 3))
	_, _, _, err = c.store.nh.GetLeaderID(1)
	require.ErrorIs(t, err, dragonboat.ErrShardNotFound)
	attempt, stop = context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
	stop()
	require.Error(t, err, "one surviving data voter must not commit")
	readMembership(c, 0, members)
	c.haClient = connect(c)
	_, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err)
	start := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 1, ReplicaID: 3, UUID: c.ID()}}}
	shutdown := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService, ShutdownStore: &pb.ShutdownStore{StoreID: c.ID()}}
	c.handleCommands([]pb.ScheduleCommand{shutdown, start})
	readMembership(c, 1, members)
	var next uint64
	wait("write after data quorum recovery", 10*time.Second, 30*time.Millisecond, func(phaseCtx context.Context) error {
		attempt, stop := context.WithTimeout(phaseCtx, 500*time.Millisecond)
		result, err := a.store.nh.SyncPropose(attempt, a.store.nh.GetNoOPSession(1), getTestUserEntry())
		stop()
		if err != nil {
			return err
		}
		next = result.Value
		if next <= old.Value {
			return fmt.Errorf("write index %d has not advanced past %d", next, old.Value)
		}
		return nil
	})
	wait("recovered voter catches up", 10*time.Second, 30*time.Millisecond, func(phaseCtx context.Context) error {
		attempt, stop := context.WithTimeout(phaseCtx, 300*time.Millisecond)
		lsn, err := c.store.getLatestLsn(attempt, 1)
		stop()
		if err != nil {
			return err
		}
		if lsn < next {
			return fmt.Errorf("LSN %d has not reached %d", lsn, next)
		}
		return nil
	})

}

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
