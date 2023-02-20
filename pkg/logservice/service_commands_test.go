// Copyright 2021 - 2022 Matrix Origin
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

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestBackgroundTickAndHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := Config{
		UUID:           uuid.New().String(),
		FS:             vfs.NewStrictMem(),
		DeploymentID:   1,
		RTTMillisecond: 5,
		DataDir:        "data-1",
		ServiceAddress: "127.0.0.1:9002",
		RaftAddress:    "127.0.0.1:9000",
		GossipAddress:  "127.0.0.1:9001",
		// below is an unreachable address intentionally set
		GossipSeedAddresses: []string{"127.0.0.1:9010"},
	}
	cfg.HeartbeatInterval.Duration = 5 * time.Millisecond
	cfg.HAKeeperTickInterval.Duration = 5 * time.Millisecond
	cfg.HAKeeperClientConfig.ServiceAddresses = []string{"127.0.0.1:9002"}
	cfg.Fill()
	service, err := NewService(cfg,
		testutil.NewFS(),
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service.Close())
	}()
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = service.ID()
	require.NoError(t, service.store.startHAKeeperReplica(1, peers, false))

	for i := 0; i < 500; i++ {
		done := true
		state, err := service.store.getCheckerState()
		require.NoError(t, err)
		if state.Tick < 10 {
			done = false
		}
		si, ok := state.LogState.Stores[service.ID()]
		if !ok {
			done = false
		} else {
			if si.Tick < 10 {
				done = false
			}
		}
		if done {
			return
		} else {
			time.Sleep(5 * time.Millisecond)
		}
	}
	t.Fatalf("failed to tick/heartbeat")
}

func TestHandleKillZombie(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)

		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.KillZombie,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		mustHaveReplica(t, s.store, 1, 1)
		s.handleCommands([]pb.ScheduleCommand{cmd})
		assert.False(t, hasReplica(s.store, 1, 1))

		has, err = hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.False(t, has)
	}
	runServiceTest(t, false, true, fn)
}

func TestHandleStartReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
				InitialMembers: map[uint64]string{1: s.ID()},
			},
		}
		s.handleCommands([]pb.ScheduleCommand{cmd})
		mustHaveReplica(t, s.store, 1, 1)

		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)
	}
	runServiceTest(t, false, false, fn)
}

func TestHandleStopReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
				InitialMembers: map[uint64]string{1: s.ID()},
			},
		}
		s.handleCommands([]pb.ScheduleCommand{cmd})
		mustHaveReplica(t, s.store, 1, 1)

		cmd = pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StopReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		s.handleCommands([]pb.ScheduleCommand{cmd})
		assert.False(t, hasReplica(s.store, 1, 1))

		has, err := hasMetadataRec(s.store.cfg.DataDir, logMetadataFilename, 1, 1, s.store.cfg.FS)
		require.NoError(t, err)
		assert.True(t, has)
	}
	runServiceTest(t, false, false, fn)
}

func TestHandleAddReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.close())
		require.NoError(t, store2.close())
	}()

	service1 := Service{
		store:   store1,
		runtime: runtime.DefaultRuntime(),
	}
	cmd := pb.ScheduleCommand{
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.AddReplica,
			Replica: pb.Replica{
				UUID:      uuid.New().String(),
				ShardID:   1,
				ReplicaID: 3,
				Epoch:     2,
			},
		},
	}
	service1.handleCommands([]pb.ScheduleCommand{cmd})
	count, ok := checkReplicaCount(store1, 1)
	require.True(t, ok)
	assert.Equal(t, 3, count)
}

func TestHandleRemoveReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.close())
		require.NoError(t, store2.close())
	}()

	service1 := Service{
		store:   store1,
		runtime: runtime.DefaultRuntime(),
	}
	cmd := pb.ScheduleCommand{
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.RemoveReplica,
			Replica: pb.Replica{
				ShardID:   1,
				ReplicaID: 2,
				Epoch:     2,
			},
		},
	}
	service1.handleCommands([]pb.ScheduleCommand{cmd})
	count, ok := checkReplicaCount(store1, 1)
	require.True(t, ok)
	assert.Equal(t, 1, count)
}

func checkReplicaCount(s *store, shardID uint64) (int, bool) {
	hb := s.getHeartbeatMessage()
	for _, info := range hb.Replicas {
		if info.ShardID == shardID {
			return len(info.Replicas), true
		}
	}
	return 0, false
}
