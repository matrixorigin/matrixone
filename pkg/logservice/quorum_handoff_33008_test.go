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
	"maps"
	"path/filepath"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

type handoffHeartbeatClient struct {
	LogHAKeeperClient
	afterDelivery func(pb.CommandBatch)
}

func (c *handoffHeartbeatClient) SendLogHeartbeat(ctx context.Context, hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	batch, err := c.LogHAKeeperClient.SendLogHeartbeat(ctx, hb)
	if err == nil && c.afterDelivery != nil {
		after := c.afterDelivery
		c.afterDelivery = nil
		after(batch)
	}
	return batch, err
}

// The Start was delivered while A/B had quorum. C must reopen its configured
// local voter after B fails, without another read from the now unavailable HA.
func TestDeliveredExistingHAKeeperStartRestoresQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
		service, err := NewService(configs[i], newFS(), nil)
		require.NoError(t, err)
		services[i] = service
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
	for i := 0; i < 2; i++ {
		require.NoError(t, services[i].store.startHAKeeperReplica(uint64(i+1), map[uint64]string{1: members[1], 2: members[2]}, false))
	}
	a, b, c := services[0], services[1], services[2]
	var initial *dragonboat.Membership
	waitForRecovery(t, ctx, "initial two-voter membership", 12*time.Second, 300*time.Millisecond, func(attempt context.Context) bool {
		var err error
		initial, err = a.store.nh.SyncGetShardMembership(attempt, 0)
		return err == nil && maps.Equal(initial.Nodes, map[uint64]string{1: members[1], 2: members[2]})
	})
	require.NoError(t, a.store.nh.SyncRequestAddReplica(ctx, 0, 3, c.ID(), initial.ConfigChangeID))
	require.NoError(t, c.store.startHAKeeperReplica(3, nil, true))
	waitForRecovery(t, ctx, "three-voter membership", 12*time.Second, 300*time.Millisecond, func(attempt context.Context) bool {
		membership, err := a.store.nh.SyncGetShardMembership(attempt, 0)
		return err == nil && maps.Equal(membership.Nodes, members)
	})
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
		hb := recoveryHeartbeat(t, ctx, service, 0, uint64(i+1), members)
		_, err := client.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
	}
	require.NoError(t, c.store.stopReplica(0, 3))
	c.store.removeMetadata(0, 3)
	require.NoError(t, c.Close())
	services[2] = nil
	open(2)
	c = services[2]
	require.True(t, c.store.nh.HasNodeInfo(0, 3))
	require.Equal(t, int64(-1), c.store.getReplicaID(0))
	c.haClient = client
	start := pb.ScheduleCommand{UUID: c.ID(), ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 0, ReplicaID: 3, UUID: c.ID()}}}
	var term uint64
	waitForRecovery(t, ctx, "HAKeeper leader", 10*time.Second, time.Second, func(context.Context) bool {
		for _, s := range []*Service{a, b} {
			leader, current, err := s.store.isLeaderHAKeeper()
			if err == nil && leader {
				term = current
				return true
			}
		}
		return false
	})
	_, err := a.store.nh.SyncPropose(ctx, a.store.nh.GetNoOPSession(0), hakeeper.GetUpdateCommandsCmd(term, []pb.ScheduleCommand{start}))
	require.NoError(t, err)
	c.haClient = &handoffHeartbeatClient{LogHAKeeperClient: client, afterDelivery: func(batch pb.CommandBatch) {
		require.Len(t, batch.Commands, 1)
		require.NotZero(t, batch.Commands[0].ConfigChange.Replica.Epoch)
		expected := start
		change := *start.ConfigChange
		change.Replica.Epoch = batch.Commands[0].ConfigChange.Replica.Epoch
		expected.ConfigChange = &change
		require.Equal(t, expected, batch.Commands[0])
		require.NoError(t, b.Close())
		services[1] = nil
		attempt, stop := context.WithTimeout(ctx, 500*time.Millisecond)
		defer stop()
		_, err := a.store.nh.SyncGetShardMembership(attempt, 0)
		require.Error(t, err, "one control voter cannot perform a quorum read")
	}}
	// Exercise the real heartbeat path, including its health check between
	// receipt and command handling, with quorum lost at the handoff.
	c.heartbeat(ctx)
	waitForRecovery(t, ctx, "recovered three-voter membership", 12*time.Second, 300*time.Millisecond, func(attempt context.Context) bool {
		membership, err := c.store.nh.SyncGetShardMembership(attempt, 0)
		return err == nil && maps.Equal(membership.Nodes, members)
	})
	_, err = c.haClient.SendLogHeartbeat(ctx, c.store.getHeartbeatMessage())
	require.NoError(t, err, "the restored quorum must accept a replicated heartbeat")
}
