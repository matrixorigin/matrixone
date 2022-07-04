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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestHandleStartReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := pb.ScheduleCommand{
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.StartReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		s.handleCommands([]pb.ScheduleCommand{cmd})
		mustHaveReplica(t, s.store, 1, 1)
	}
	runServiceTest(t, false, fn)
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
	}
	runServiceTest(t, false, fn)
}

func TestHandleAddReplica(t *testing.T) {
	store1, store2, err := getTestStores()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store1.Close())
		require.NoError(t, store2.Close())
	}()

	service1 := Service{
		store: store1,
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
		require.NoError(t, store1.Close())
		require.NoError(t, store2.Close())
	}()

	service1 := Service{
		store: store1,
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
