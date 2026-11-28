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

package hakeeper

import (
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestBootstrapCommandSurvivesPartialInitialMembership(t *testing.T) {
	const store = "c"
	machine := NewStateMachine(0, 1).(*stateMachine)
	// Three initial AddNode entries are applied individually. The existing
	// CheckBootstrap accepts two reported members as enough to enter Running.
	machine.state.State = pb.HAKeeperRunning
	machine.state.LogState.Shards[1] = pb.LogShardInfo{
		ShardID: 1, Epoch: 2, Replicas: map[uint64]string{1: "a", 2: "b"},
	}
	start := pb.ScheduleCommand{UUID: store, ServiceType: pb.LogService, Bootstrapping: true,
		ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica,
			Replica:        pb.Replica{UUID: store, ShardID: 1, ReplicaID: 3},
			InitialMembers: map[uint64]string{1: "a", 2: "b", 3: store}}}
	machine.state.ScheduleCommands[store] = pb.CommandBatch{Commands: []pb.ScheduleCommand{start}}
	data, err := (&pb.LogStoreHeartbeat{UUID: store}).Marshal()
	require.NoError(t, err)
	_, err = machine.Update(sm.Entry{Cmd: GetLogStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	require.Equal(t, []pb.ScheduleCommand{start}, machine.state.ScheduleCommands[store].Commands,
		"a not-yet-applied initial member is not an obsolete member")
}
