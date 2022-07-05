package logservice

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestCollectStats(t *testing.T) {
	cases := []struct {
		desc     string
		cluster  pb.ClusterInfo
		infos    pb.LogState
		tick     uint64
		expected *stats
	}{
		{
			desc: "Normal case",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0,
					}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0,
							}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}}},
			}, tick: 10, expected: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{}}},
		{
			desc: "Shard 1 has only 2 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b"},
					Epoch:    1,
					LeaderID: 0,
					Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			}, tick: 10, expected: &stats{toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{1: 1}}},
		{
			desc: "replica on Store c is not started.",
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{}},
				},
			}, tick: 10,
			expected: &stats{toStart: []replica{{"c", 1, 0, 3}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{}}},
		{
			desc: "replica on Store d is a zombie.",
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 4: "d"},
								Epoch:    0,
								LeaderID: 0,
								Term:     0}}}},
				},
			}, tick: 10,
			expected: &stats{toStop: []replica{{"d", 1, 0, 0}},
				toRemove: map[uint64][]replica{}, toAdd: map[uint64]int{}}},
		{
			desc: "Shard 1 has 4 replicas, which is expected as 3.",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             ""}}},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"d": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
				},
			}, tick: 10,
			expected: &stats{toRemove: map[uint64][]replica{1: {{"a", 1, 0, 1}}},
				toAdd: map[uint64]int{},
			},
		},
		{
			desc: "Store a is expired",
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					1: {
						ShardID:  1,
						Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
						Epoch:    1,
						LeaderID: 0,
						Term:     0}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {Tick: 0, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"b": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}},
					"c": {Tick: 999999999, RaftAddress: "", ServiceAddress: "", GossipAddress: "",
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 0,
								Term:     0}}}}},
			}, tick: 999999999,
			expected: &stats{
				toRemove: map[uint64][]replica{1: {{"a", 1, 1, 1}}},
				toAdd:    map[uint64]int{},
			},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		stat := parseLogShards(c.cluster, c.infos, c.tick)
		assert.Equal(t, c.expected, stat)
	}
}

func TestCollectStore(t *testing.T) {
	cases := []struct {
		desc     string
		cluster  pb.ClusterInfo
		infos    pb.LogState
		tick     uint64
		expected util.ClusterStores
	}{
		{
			desc: "no expired stores",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "",
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick:     uint64(10 * hakeeper.TickPerSecond * 60),
						Replicas: nil,
					},
					"b": {
						Tick:     uint64(13 * hakeeper.TickPerSecond * 60),
						Replicas: nil,
					},
					"c": {
						Tick:     uint64(12 * hakeeper.TickPerSecond * 60),
						Replicas: nil,
					},
				},
			},
			tick: uint64(10 * hakeeper.TickPerSecond * 60),
			expected: util.ClusterStores{
				Working: []*util.Store{{
					ID:       "a",
					Length:   0,
					Capacity: 32,
				}, {
					ID:       "b",
					Length:   0,
					Capacity: 32,
				}, {
					ID:       "c",
					Length:   0,
					Capacity: 32,
				}},
				Expired: nil,
			},
		},
		{
			desc: "store b expired",
			cluster: pb.ClusterInfo{
				DNShards: nil,
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
					Name:             "",
				}},
			},
			infos: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick:     uint64(10 * hakeeper.TickPerSecond * 60),
						Replicas: nil,
					},
					"b": {
						Tick:     0,
						Replicas: nil,
					},
					"c": {
						Tick:     uint64(12 * hakeeper.TickPerSecond * 60),
						Replicas: nil,
					},
				},
			},
			tick: uint64(15 * hakeeper.TickPerSecond * 60),
			expected: util.ClusterStores{
				Working: []*util.Store{{
					ID:       "a",
					Length:   0,
					Capacity: 32,
				}, {
					ID:       "c",
					Length:   0,
					Capacity: 32,
				}},
				Expired: []*util.Store{
					{
						ID:       "b",
						Length:   0,
						Capacity: 32,
					},
				},
			},
		},
	}
	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		stores := parseLogStores(c.infos, c.tick)
		sort.Slice(stores.Working, func(i, j int) bool {
			return stores.Working[i].ID < stores.Working[j].ID
		})
		sort.Slice(stores.Expired, func(i, j int) bool {
			return stores.Expired[i].ID < stores.Expired[j].ID
		})
		assert.Equal(t, c.expected, *stores)
	}
}
