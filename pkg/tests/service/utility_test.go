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

package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const (
	// default replica number for log shard
	defaultLogShardSize = 3
)

func TestParseExpectedDNShardCount(t *testing.T) {
	cluster := pb.ClusterInfo{
		DNShards: mockDNShardRecords(10, 11, 12, 12),
	}
	// expected dn shards: 10, 11, 12
	require.Equal(t, 3, ParseExpectedDNShardCount(cluster))
}

func TestParseExpectedLogShardCount(t *testing.T) {
	cluster := pb.ClusterInfo{
		LogShards: mockLogShardRecords(10, 11, 11),
	}
	// expected log shards: 10, 11
	require.Equal(t, 2, ParseExpectedLogShardCount(cluster))
}

func TestParseReportedDNShardCount(t *testing.T) {
	hkcfg := hakeeper.Config{}
	hkcfg.Fill()

	expiredTick := uint64(10)
	currTick := hkcfg.ExpiredTick(expiredTick, hkcfg.DNStoreTimeout) + 1

	dnState := pb.DNState{
		Stores: map[string]pb.DNStoreInfo{
			"expired1": {
				Tick: expiredTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(10, 100),
				},
			},
			"working1": {
				Tick: currTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(11, 101),
					mockDnShardInfo(11, 102),
					mockDnShardInfo(12, 103),
				},
			},
		},
	}

	// working dn shards: 11, 12
	require.Equal(t, 2, ParseReportedDNShardCount(dnState, hkcfg, currTick))
}

func TestParseReportedLogShardCount(t *testing.T) {
	hkcfg := hakeeper.Config{}
	hkcfg.Fill()

	expiredTick := uint64(10)
	currTick := hkcfg.ExpiredTick(expiredTick, hkcfg.LogStoreTimeout) + 1

	logState := pb.LogState{
		Stores: map[string]pb.LogStoreInfo{
			"expired1": {
				Tick: expiredTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(11, 100),
				},
			},
			"workding1": {
				Tick: currTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(11, 101),
				},
			},
			"workding2": {
				Tick: currTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(12, 102),
				},
			},
			"workding3": {
				Tick: currTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(12, 103),
				},
			},
		},
	}

	// reported working log shard: 11, 12
	require.Equal(t, 2, ParseReportedLogShardCount(logState, hkcfg, currTick))
}

func TestParseLogShardExpectedSize(t *testing.T) {
	cluster := pb.ClusterInfo{
		LogShards: mockLogShardRecords(10, 11),
	}

	// log shard 10 recorded in ClusterInfo
	require.Equal(t, defaultLogShardSize, ParseLogShardExpectedSize(10, cluster))

	// log shard 100 not recorded in ClusterInfo
	require.Equal(t, 0, ParseLogShardExpectedSize(100, cluster))
}

func TestParseLogShardReportedSize(t *testing.T) {
	hkcfg := hakeeper.Config{}
	hkcfg.Fill()

	expiredTick := uint64(10)
	currTick := hkcfg.ExpiredTick(expiredTick, hkcfg.LogStoreTimeout) + 1

	logState := pb.LogState{
		Stores: map[string]pb.LogStoreInfo{
			"expired1": {
				Tick: expiredTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(11, 100),
				},
			},
			"workding1": {
				Tick: currTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(11, 101),
				},
			},
			"workding2": {
				Tick: currTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(11, 102),
				},
			},
			"workding3": {
				Tick: currTick,
				Replicas: []pb.LogReplicaInfo{
					mockLogReplicaInfo(11, 103),
				},
			},
		},
	}

	// 3 working replicas reported for log shard 11
	require.Equal(t, 3, ParseLogShardReportedSize(11, logState, hkcfg, currTick))
}

func TestParseDNShardReportedSize(t *testing.T) {
	hkcfg := hakeeper.Config{}
	hkcfg.Fill()

	expiredTick := uint64(10)
	currTick := hkcfg.ExpiredTick(expiredTick, hkcfg.DNStoreTimeout) + 1

	dnState := pb.DNState{
		Stores: map[string]pb.DNStoreInfo{
			"expired1": {
				Tick: expiredTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(10, 100),
				},
			},
			"working1": {
				Tick: currTick,
				Shards: []pb.DNShardInfo{
					mockDnShardInfo(11, 101),
					mockDnShardInfo(11, 102),
					mockDnShardInfo(12, 103),
				},
			},
		},
	}

	require.Equal(t, 2, ParseDNShardReportedSize(11, dnState, hkcfg, currTick))
}

func mockDNShardRecords(ids ...uint64) []metadata.DNShardRecord {
	records := make([]metadata.DNShardRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, metadata.DNShardRecord{
			ShardID:    id,
			LogShardID: id,
		})
	}
	return records
}

func mockLogShardRecords(ids ...uint64) []metadata.LogShardRecord {
	records := make([]metadata.LogShardRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, metadata.LogShardRecord{
			ShardID:          id,
			NumberOfReplicas: uint64(defaultLogShardSize),
		})
	}
	return records
}

func mockDnShardInfo(shardID, replicaID uint64) pb.DNShardInfo {
	return pb.DNShardInfo{
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
}

func mockLogReplicaInfo(shardID, replicaID uint64) pb.LogReplicaInfo {
	return pb.LogReplicaInfo{
		ReplicaID: replicaID,
		LogShardInfo: pb.LogShardInfo{
			ShardID: shardID,
		},
	}
}
