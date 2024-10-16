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

package bootstrap

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})

	runtime.SetupServiceBasedRuntime("", runtime.NewRuntime(metadata.ServiceType_LOG, "test", logutil.GetGlobalLogger()))
	m.Run()
}

func TestNewBootstrapManager(t *testing.T) {
	cases := []struct {
		cluster  pb.ClusterInfo
		expected *Manager
	}{
		{
			cluster:  pb.ClusterInfo{},
			expected: &Manager{},
		},
		{
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{{ShardID: 2}},
			},
			expected: &Manager{
				cluster: pb.ClusterInfo{
					TNShards: []metadata.TNShardRecord{{
						ShardID:    1,
						LogShardID: 1,
					}},
					LogShards: []metadata.LogShardRecord{{ShardID: 2}},
				},
			},
		},
	}

	for _, c := range cases {
		bm := NewBootstrapManager(c.cluster)
		assert.Equal(t, c.expected.cluster, bm.cluster)
		c.expected.cluster = pb.ClusterInfo{XXX_sizecache: 1}
		assert.NotEqual(t, c.expected.cluster, bm.cluster)
	}
}

func TestBootstrap(t *testing.T) {
	cases := []struct {
		desc string

		cluster pb.ClusterInfo
		tn      pb.TNState
		log     pb.LogState

		expectedNum            int
		expectedInitialMembers map[uint64]string
		expectedTNLogShardID   uint64
		err                    error
	}{
		{
			desc: "1 log shard with 1 replicas",

			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 1,
				}},
			},
			log: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{
					"log-a": {Tick: 100},
				},
			},

			expectedNum: 1,
			expectedInitialMembers: map[uint64]string{
				1: "log-a",
			},
		},
		{
			desc: "1 log shard with 3 replicas and 1 tn shard",

			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{ShardID: 1, LogShardID: 1}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			tn: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{"dn-a": {}},
			},
			log: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{
					"log-a": {Tick: 100},
					"log-b": {Tick: 110},
					"log-c": {Tick: 120},
					"log-d": {Tick: 130},
				},
			},

			expectedNum: 4,
			expectedInitialMembers: map[uint64]string{
				1: "log-d",
				2: "log-c",
				3: "log-b",
			},
			expectedTNLogShardID: 1,
		},
		{
			desc: "ignore shard 0",

			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          0,
					NumberOfReplicas: 3,
				}},
			},
			tn: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{},
			},
			log: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{
					"log-a": {Tick: 100},
					"log-b": {Tick: 110},
				},
			},

			expectedNum: 0,
			err:         nil,
		},
		{
			desc: "1 log shard with 3 replicas and 1 tn shard",

			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{ShardID: 1, LogShardID: 1}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			tn: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{"dn-a": {}},
			},
			log: pb.LogState{
				Stores: map[string]pb.LogStoreInfo{
					"log-a": {Tick: 100},
					"log-b": {Tick: 110},
					"log-c": {Tick: 120},
					"log-d": {Tick: 130},
				},
			},

			expectedNum: 4,
			expectedInitialMembers: map[uint64]string{
				1: "log-d",
				2: "log-c",
				3: "log-b",
			},
			expectedTNLogShardID: 1,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)

		alloc := util.NewTestIDAllocator(0)
		bm := NewBootstrapManager(c.cluster)
		output, err := bm.Bootstrap("", alloc, c.tn, c.log)
		assert.Equal(t, c.err, err)
		if err != nil {
			continue
		}
		assert.Equal(t, c.expectedNum, len(output))
		if len(output) != 0 {
			assert.Equal(t, c.expectedInitialMembers, output[0].ConfigChange.InitialMembers)
			assert.Equal(t, pb.StartReplica, output[0].ConfigChange.ChangeType)
		}

		for _, command := range output {
			if command.ServiceType == pb.TNService {
				assert.Equal(t, c.expectedTNLogShardID, command.ConfigChange.Replica.LogShardID)
			}
		}
	}
}

func TestCheckBootstrap(t *testing.T) {
	cases := []struct {
		desc string

		cluster pb.ClusterInfo
		log     pb.LogState

		expected bool
	}{
		{
			desc: "failed to start 1 replica",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 1},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{}},
				},
			},
			expected: false,
		},
		{
			desc: "successfully started 1 replica",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 1},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{1: "a"}},
				},
			},
			expected: true,
		},
		{
			desc: "successfully started 3 replicas",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
					{ShardID: 2, NumberOfReplicas: 3},
					{ShardID: 3, NumberOfReplicas: 3},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b"}},
					2: {ShardID: 2, Replicas: map[uint64]string{1: "a", 3: "c"}},
					3: {ShardID: 3, Replicas: map[uint64]string{2: "b", 3: "c"}},
				},
			},
			expected: true,
		},
		{
			desc: "shard 1 not started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
					{ShardID: 2, NumberOfReplicas: 3},
					{ShardID: 3, NumberOfReplicas: 3},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {ShardID: 1, Replicas: map[uint64]string{1: "a"}},
					2: {ShardID: 2, Replicas: map[uint64]string{1: "a", 3: "c"}},
					3: {ShardID: 3, Replicas: map[uint64]string{2: "b", 3: "c"}},
				},
			},
			expected: false,
		},
		{
			desc: "shard 1 not exists in log state",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
				},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{},
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		bm := NewBootstrapManager(c.cluster)
		output := bm.CheckBootstrap(c.log)
		assert.Equal(t, c.expected, output)
	}
}

func TestSortLogStores(t *testing.T) {
	cases := []struct {
		logStores map[string]pb.LogStoreInfo
		expected  []string
	}{
		{
			logStores: map[string]pb.LogStoreInfo{
				"a": {Tick: 100},
				"b": {Tick: 120},
				"c": {Tick: 90},
				"d": {Tick: 95},
			},
			expected: []string{"b", "a", "d", "c"},
		},
		{
			logStores: map[string]pb.LogStoreInfo{
				"a": {
					Tick: 100,
					Locality: pb.Locality{
						Value: map[string]string{"k1": "v1"},
					},
				},
				"b": {Tick: 120},
				"c": {Tick: 90},
				"d": {Tick: 95},
			},
			expected: []string{"b", "d", "c", "a"},
		},
		{
			logStores: map[string]pb.LogStoreInfo{
				"a": {
					Tick: 100,
					Locality: pb.Locality{
						Value: map[string]string{"k1": "v1"},
					},
				},
				"b": {
					Tick: 120,
					Locality: pb.Locality{
						Value: map[string]string{"k1": "v1"},
					},
				},
				"c": {
					Tick: 90,
				},
				"d": {
					Tick: 95,
				},
			},
			expected: []string{"d", "c", "b", "a"},
		},
	}

	for _, c := range cases {
		output := logStoresSortedByTick(c.logStores)
		assert.Equal(t, c.expected, output)
	}
}

func TestSortTNStores(t *testing.T) {
	cases := []struct {
		tnStores map[string]pb.TNStoreInfo
		expected []string
	}{{
		tnStores: map[string]pb.TNStoreInfo{
			"a": {Tick: 100},
			"b": {Tick: 120},
			"c": {Tick: 90},
			"d": {Tick: 95},
		},
		expected: []string{"b", "a", "d", "c"},
	}}

	for _, c := range cases {
		output := tnStoresSortedByTick(c.tnStores)
		assert.Equal(t, c.expected, output)
	}
}

func TestIssue3814(t *testing.T) {
	cases := []struct {
		desc string

		cluster pb.ClusterInfo
		tn      pb.TNState
		log     pb.LogState

		expected error
	}{
		{
			desc: "case not enough log store",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			log:      pb.LogState{},
			expected: moerr.NewInternalError(context.TODO(), "not enough log stores"),
		},
		{
			desc: "case not enough tn stores",
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
			},
			tn: pb.TNState{
				Stores: map[string]pb.TNStoreInfo{},
			},
			expected: nil,
		},
	}

	for _, c := range cases {
		alloc := util.NewTestIDAllocator(0)
		bm := NewBootstrapManager(c.cluster)
		_, err := bm.Bootstrap("", alloc, c.tn, c.log)
		assert.Equal(t, c.expected, err)
	}
}

func TestIssue3845(t *testing.T) {
	cases := []struct {
		desc string

		cluster pb.ClusterInfo
		log     pb.LogState

		expected bool
	}{
		{
			desc: "shardID is 0",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          0,
					NumberOfReplicas: 1,
				}},
			},
			log: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{0: {
					ShardID:  0,
					Replicas: map[uint64]string{1: "a"},
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  0,
							Replicas: map[uint64]string{1: "a"},
						},
						ReplicaID: 1,
					}},
				}},
			},
			expected: true,
		},
	}

	for _, c := range cases {
		bm := NewBootstrapManager(c.cluster)
		output := bm.CheckBootstrap(c.log)
		assert.Equal(t, c.expected, output)
	}
}
