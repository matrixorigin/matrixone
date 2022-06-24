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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
)

func TestHandleStartReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := hapb.ScheduleCommand{
			ConfigChange: hapb.ConfigChange{
				ChangeType: hapb.StartReplica,
				Replica: hapb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		s.handleCommands([]hapb.ScheduleCommand{cmd})
		mustHaveReplica(t, s.store, 1, 1)
	}
	runServiceTest(t, fn)
}

func TestHandleStopReplica(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cmd := hapb.ScheduleCommand{
			ConfigChange: hapb.ConfigChange{
				ChangeType: hapb.StartReplica,
				Replica: hapb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		s.handleCommands([]hapb.ScheduleCommand{cmd})
		mustHaveReplica(t, s.store, 1, 1)

		cmd = hapb.ScheduleCommand{
			ConfigChange: hapb.ConfigChange{
				ChangeType: hapb.StopReplica,
				Replica: hapb.Replica{
					ShardID:   1,
					ReplicaID: 1,
				},
			},
		}
		s.handleCommands([]hapb.ScheduleCommand{cmd})
		assert.False(t, hasReplica(s.store, 1, 1))
	}
	runServiceTest(t, fn)
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
	cmd := hapb.ScheduleCommand{
		ConfigChange: hapb.ConfigChange{
			ChangeType: hapb.RemoveReplica,
			Replica: hapb.Replica{
				ShardID:   1,
				ReplicaID: 2,
			},
		},
	}
	// TODO: add some checks
	service1.handleCommands([]hapb.ScheduleCommand{cmd})
}
