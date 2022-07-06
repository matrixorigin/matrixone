package operator

import (
	"fmt"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"testing"
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

	assert.False(t, AddLogService{StoreID: "d", ShardID: 1, ReplicaID: 4}.IsFinish(logState, dnState))
	assert.True(t, AddLogService{StoreID: "c", ShardID: 1, ReplicaID: 3}.IsFinish(logState, dnState))
	assert.False(t, RemoveLogService{StoreID: "c", ShardID: 1, ReplicaID: 3}.IsFinish(logState, dnState))
	assert.True(t, RemoveLogService{StoreID: "d", ShardID: 1, ReplicaID: 4}.IsFinish(logState, dnState))
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
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
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
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}))
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
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
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
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}))
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
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
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
				StoreID:   "a",
				ShardID:   1,
				ReplicaID: 1,
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}))
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
				StoreID: "a",
				ShardID: 1,
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
				StoreID: "a",
				ShardID: 1,
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
		assert.Equal(t, c.expected, c.command.IsFinish(c.state, pb.DNState{}))
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
		assert.Equal(t, c.expected, c.command.IsFinish(pb.LogState{}, c.state))
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
		assert.Equal(t, c.expected, c.command.IsFinish(pb.LogState{}, c.state))
	}
}
