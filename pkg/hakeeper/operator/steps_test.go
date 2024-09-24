// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2021 Matrix Origin.
//
// Modified some tests.

package operator

import (
	"fmt"
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestIsFinish(t *testing.T) {
	logState := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:           1,
			Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
			Epoch:             1,
			NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
		}},
		Stores: nil,
	}
	clusterState := ClusterState{
		LogState:   logState,
		CNState:    pb.CNState{},
		TNState:    pb.TNState{},
		ProxyState: pb.ProxyState{},
	}
	assert.False(t, AddLogService{
		Replica: Replica{
			UUID:      "g",
			ShardID:   1,
			ReplicaID: 7,
		}}.IsFinish(clusterState),
	)
	assert.True(t, AddLogService{
		Replica: Replica{
			UUID:      "c",
			ShardID:   1,
			ReplicaID: 3,
		}}.IsFinish(clusterState),
	)
	assert.False(t, RemoveLogService{
		Replica: Replica{
			UUID:      "c",
			ShardID:   1,
			ReplicaID: 3,
		}}.IsFinish(clusterState),
	)
	assert.True(t, RemoveLogService{
		Replica: Replica{
			UUID:      "g",
			ShardID:   1,
			ReplicaID: 7,
		}}.IsFinish(clusterState),
	)
	// for non-voting replicas
	assert.False(t, AddNonVotingLogService{
		Replica: Replica{
			UUID:      "g",
			ShardID:   1,
			ReplicaID: 7,
		}}.IsFinish(clusterState),
	)
	assert.True(t, AddNonVotingLogService{
		Replica: Replica{
			UUID:      "f",
			ShardID:   1,
			ReplicaID: 6,
		}}.IsFinish(clusterState),
	)
	assert.False(t, RemoveNonVotingLogService{
		Replica: Replica{
			UUID:      "f",
			ShardID:   1,
			ReplicaID: 6,
		}}.IsFinish(clusterState),
	)
	assert.True(t, RemoveNonVotingLogService{
		Replica: Replica{
			UUID: "g",

			ShardID:   1,
			ReplicaID: 7,
		}}.IsFinish(clusterState),
	)
}

func TestAddLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  AddLogService
		state    pb.LogState
		expected bool
	}{
		{
			desc: "add log service completed",
			command: AddLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{1: "a"}},
				},
			},
			expected: true,
		},
		{
			desc: "add log service not completed",
			command: AddLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{}},
				},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestAddNonVotingLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  OpStep
		state    pb.LogState
		expected bool
	}{
		{
			desc: "add non-voting log service completed",
			command: AddNonVotingLogService{
				Replica: Replica{
					UUID:      "b",
					ShardID:   1,
					ReplicaID: 2},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b"},
						NonVotingReplicas: map[uint64]string{2: "b"},
					},
				},
			},
			expected: true,
		},
		{
			desc: "add non-voting log service not completed",
			command: AddNonVotingLogService{
				Replica: Replica{
					UUID:      "b",
					ShardID:   1,
					ReplicaID: 2},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b"},
					},
				},
			},
			expected: false,
		},
	}
	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestRemoveLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  RemoveLogService
		state    pb.LogState
		expected bool
	}{
		{
			desc: "remove log service not completed",
			command: RemoveLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{1: "a"}},
				},
			},
			expected: false,
		},
		{
			desc: "remove log service completed",
			command: RemoveLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1,
				},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{}},
				},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestRemoveNonVotingLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  OpStep
		state    pb.LogState
		expected bool
	}{
		{
			desc: "remove non-voting log service not completed",
			command: RemoveNonVotingLogService{
				Replica: Replica{
					UUID:      "b",
					ShardID:   1,
					ReplicaID: 2},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:           1,
						Replicas:          map[uint64]string{1: "a", 2: "b"},
						NonVotingReplicas: map[uint64]string{2: "b"},
					},
				},
			},
			expected: false,
		},
		{
			desc: "remove non-voting log service completed",
			command: RemoveNonVotingLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1,
				},
			},
			state: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{}},
				},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestStartLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  StartLogService
		state    pb.LogState
		expected bool
	}{
		{
			desc: "start log service not completed",
			command: StartLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{}},
				}},
			},
			expected: false,
		},
		{
			desc: "start log service completed",
			command: StartLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a"},
						},
						ReplicaID: 1,
					}},
				}},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestStartNonVotingLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  OpStep
		state    pb.LogState
		expected bool
	}{
		{
			desc: "start non-voting log service not completed",
			command: StartNonVotingLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{}},
				}},
			},
			expected: false,
		},
		{
			desc: "start non-voting log service completed",
			command: StartNonVotingLogService{
				Replica: Replica{
					UUID:      "a",
					ShardID:   1,
					ReplicaID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a"},
						},
						ReplicaID: 1,
					}},
				}},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestStopLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  StopLogService
		state    pb.LogState
		expected bool
	}{
		{
			desc: "stop log service completed",
			command: StopLogService{
				Replica: Replica{
					UUID:    "a",
					ShardID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{}},
				}},
			},
			expected: true,
		},
		{
			desc: "stop log service not completed",
			command: StopLogService{
				Replica: Replica{
					UUID:    "a",
					ShardID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a"},
						},
						ReplicaID: 1,
					}},
				}},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestStopNonVotingLogService(t *testing.T) {
	cases := []struct {
		desc     string
		command  OpStep
		state    pb.LogState
		expected bool
	}{
		{
			desc: "stop non-voting log service completed",
			command: StopNonVotingLogService{
				Replica: Replica{
					UUID:    "a",
					ShardID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{}},
				}},
			},
			expected: true,
		},
		{
			desc: "stop non-voting log service not completed",
			command: StopNonVotingLogService{
				Replica: Replica{
					UUID:    "a",
					ShardID: 1,
				},
			},
			state: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{"a": {
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a"},
						},
						ReplicaID: 1,
					}},
				}},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   c.state,
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestAddTnReplica(t *testing.T) {
	cases := []struct {
		desc     string
		command  AddTnReplica
		state    pb.TNState
		expected bool
	}{
		{
			desc: "add tn replica completed",
			command: AddTnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{"a": {
					Tick: 0,
					Shards: []pb.TNShardInfo{{
						ShardID:   1,
						ReplicaID: 1,
					}},
				}},
			},
			expected: true,
		},
		{
			desc: "add tn replica not completed",
			command: AddTnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{"a": {
					Tick:   0,
					Shards: []pb.TNShardInfo{},
				}},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   pb.LogState{},
			TNState:    c.state,
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestRemoveTnReplica(t *testing.T) {
	cases := []struct {
		desc     string
		command  RemoveTnReplica
		state    pb.TNState
		expected bool
	}{
		{
			desc: "remove tn replica not completed",
			command: RemoveTnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{"a": {
					Tick: 0,
					Shards: []pb.TNShardInfo{{
						ShardID:   1,
						ReplicaID: 1,
					}},
				}},
			},
			expected: false,
		},
		{
			desc: "remove tn replica completed",
			command: RemoveTnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{"a": {
					Tick:   0,
					Shards: []pb.TNShardInfo{},
				}},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   pb.LogState{},
			TNState:    c.state,
			CNState:    pb.CNState{},
			ProxyState: pb.ProxyState{},
		}))
	}
}

func TestDeleteProxyStore(t *testing.T) {
	cases := []struct {
		desc     string
		command  DeleteProxyStore
		state    pb.ProxyState
		expected bool
	}{
		{
			desc: "delete proxy store not finished",
			command: DeleteProxyStore{
				StoreID: "a",
			},
			state: pb.ProxyState{
				Stores: map[string]pb.ProxyStore{"a": {
					UUID: "a",
				}},
			},
			expected: false,
		},
		{
			desc: "delete proxy store finished",
			command: DeleteProxyStore{
				StoreID: "a",
			},
			state: pb.ProxyState{
				Stores: map[string]pb.ProxyStore{},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(ClusterState{
			LogState:   pb.LogState{},
			TNState:    pb.TNState{},
			CNState:    pb.CNState{},
			ProxyState: c.state,
		}))
	}
}
