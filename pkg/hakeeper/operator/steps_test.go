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
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			Epoch:    1,
		}},
		Stores: nil,
	}

	dnState := pb.DNState{}
	cnState := pb.CNState{}

	assert.False(t, AddLogService{Replica: Replica{UUID: "d", ShardID: 1, ReplicaID: 4}}.IsFinish(logState, dnState, cnState))
	assert.True(t, AddLogService{Replica: Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}.IsFinish(logState, dnState, cnState))
	assert.False(t, RemoveLogService{Replica: Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}.IsFinish(logState, dnState, cnState))
	assert.True(t, RemoveLogService{Replica: Replica{UUID: "d", ShardID: 1, ReplicaID: 4}}.IsFinish(logState, dnState, cnState))
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}, pb.CNState{}))
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}, pb.CNState{}))
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}, pb.CNState{}))
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}, pb.CNState{}))
	}
}

func TestAddDnReplica(t *testing.T) {
	cases := []struct {
		desc     string
		command  AddDnReplica
		state    pb.DNState
		expected bool
	}{
		{
			desc: "add dn replica completed",
			command: AddDnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.DNState{
				Stores: map[string]pb.DNStoreInfo{"a": {
					Tick: 0,
					Shards: []pb.DNShardInfo{{
						ShardID:   1,
						ReplicaID: 1,
					}},
				}},
			},
			expected: true,
		},
		{
			desc: "add dn replica not completed",
			command: AddDnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.DNState{
				Stores: map[string]pb.DNStoreInfo{"a": {
					Tick:   0,
					Shards: []pb.DNShardInfo{},
				}},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(pb.LogState{}, c.state, pb.CNState{}))
	}
}

func TestRemoveDnReplica(t *testing.T) {
	cases := []struct {
		desc     string
		command  RemoveDnReplica
		state    pb.DNState
		expected bool
	}{
		{
			desc: "remove dn replica not completed",
			command: RemoveDnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.DNState{
				Stores: map[string]pb.DNStoreInfo{"a": {
					Tick: 0,
					Shards: []pb.DNShardInfo{{
						ShardID:   1,
						ReplicaID: 1,
					}},
				}},
			},
			expected: false,
		},
		{
			desc: "remove dn replica completed",
			command: RemoveDnReplica{
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
			},
			state: pb.DNState{
				Stores: map[string]pb.DNStoreInfo{"a": {
					Tick:   0,
					Shards: []pb.DNShardInfo{},
				}},
			},
			expected: true,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		assert.Equal(t, c.expected, c.command.IsFinish(pb.LogState{}, c.state, pb.CNState{}))
	}
}
