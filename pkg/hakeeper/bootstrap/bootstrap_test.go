package bootstrap

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBootstrap(t *testing.T) {
	cases := []struct {
		desc string

		cluster  hapb.ClusterInfo
		dn       hapb.DNState
		log      hapb.LogState
		expected int
	}{
		{
			desc: "1 log shard with 3 replicas and 1 dn shard",

			cluster: hapb.ClusterInfo{
				DNShards: []metadata.DNShardRecord{{ShardID: 1, LogShardID: 1}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			dn: hapb.DNState{
				Stores: map[string]hapb.DNStoreInfo{"dn-a": {}},
			},
			log: hapb.LogState{
				Stores: map[string]hapb.LogStoreInfo{"log-a": {}, "log-b": {}, "log-c": {}},
			},

			expected: 4,
		},
	}

	for _, c := range cases {
		alloc := util.NewTestIDAllocator(0)
		output := Bootstrap(alloc, c.cluster, c.dn, c.log)
		assert.Equal(t, c.expected, len(output))

	}
}

func TestCheckBootstrap(t *testing.T) {
	cases := []struct {
		desc string

		cluster hapb.ClusterInfo
		log     hapb.LogState

		expected bool
	}{
		{
			desc: "successfully started",
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
					{ShardID: 2, NumberOfReplicas: 3},
					{ShardID: 3, NumberOfReplicas: 3},
				},
			},
			log: hapb.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b"},
					},
					2: {
						ShardID:  2,
						Replicas: map[uint64]string{1: "a", 3: "c"},
					},
					3: {
						ShardID:  3,
						Replicas: map[uint64]string{2: "b", 3: "c"},
					},
				},
				Stores: nil,
			},
			expected: true,
		},
		{
			desc: "shard 1 not started",
			cluster: hapb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{
					{ShardID: 1, NumberOfReplicas: 3},
					{ShardID: 2, NumberOfReplicas: 3},
					{ShardID: 3, NumberOfReplicas: 3},
				},
			},
			log: hapb.LogState{
				Shards: map[uint64]logservice.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a"},
					},
					2: {
						ShardID:  2,
						Replicas: map[uint64]string{1: "a", 3: "c"},
					},
					3: {
						ShardID:  3,
						Replicas: map[uint64]string{2: "b", 3: "c"},
					},
				},
				Stores: nil,
			},
			expected: false,
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		output := CheckBootstrap(c.cluster, c.log)
		assert.Equal(t, c.expected, output)
	}
}
