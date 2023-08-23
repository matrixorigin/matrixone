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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	// The expected number of tn replicas.
	TNShardExpectedSize = 1
)

// ParseExpectedTNShardCount returns the expected count of tn shards.
func ParseExpectedTNShardCount(cluster pb.ClusterInfo) int {
	set := make(map[uint64]struct{})
	for _, record := range cluster.TNShards {
		set[record.ShardID] = struct{}{}

	}
	return len(set)
}

// ParseExpectedLogShardCount returns the expected count of log shards.
func ParseExpectedLogShardCount(cluster pb.ClusterInfo) int {
	set := make(map[uint64]struct{})
	for _, record := range cluster.LogShards {
		set[record.ShardID] = struct{}{}

	}
	return len(set)
}

// ParseReportedTNShardCount returns the reported count of tn shards.
func ParseReportedTNShardCount(
	state pb.TNState, hkcfg hakeeper.Config, currTick uint64,
) int {
	set := make(map[uint64]struct{})
	for _, storeInfo := range state.Stores {
		// ignore expired tn stores
		if hkcfg.TNStoreExpired(storeInfo.Tick, currTick) {
			continue
		}

		// record tn shard
		for _, shardInfo := range storeInfo.Shards {
			set[shardInfo.ShardID] = struct{}{}
		}
	}
	return len(set)
}

// ParseReportedLogShardCount returns the reported count of log shards.
func ParseReportedLogShardCount(
	state pb.LogState, hkcfg hakeeper.Config, currTick uint64,
) int {
	set := make(map[uint64]struct{})
	for _, storeInfo := range state.Stores {
		// ignore expired log stores
		if hkcfg.LogStoreExpired(storeInfo.Tick, currTick) {
			continue
		}

		for _, replicaInfo := range storeInfo.Replicas {
			set[replicaInfo.ShardID] = struct{}{}
		}
	}
	return len(set)
}

// ParseLogShardExpectedSize returns the expected count of log replicas.
func ParseLogShardExpectedSize(shardID uint64, cluster pb.ClusterInfo) int {
	for _, record := range cluster.LogShards {
		if record.ShardID == shardID {
			return int(record.NumberOfReplicas)
		}
	}
	return 0
}

// ParseLogShardReportedSize returns the reported count of log replicas.
func ParseLogShardReportedSize(
	shardID uint64, state pb.LogState, hkcfg hakeeper.Config, currTick uint64,
) int {
	set := make(map[uint64]struct{})
	for _, storeInfo := range state.Stores {
		// ignore expired log stores
		if hkcfg.LogStoreExpired(storeInfo.Tick, currTick) {
			continue
		}

		for _, replicaInfo := range storeInfo.Replicas {
			if replicaInfo.ShardID == shardID {
				set[replicaInfo.ReplicaID] = struct{}{}
			}
		}
	}
	return len(set)
}

// ParseTNShardReportedSize returns the reported count of tn replicas.
func ParseTNShardReportedSize(
	shardID uint64, state pb.TNState, hkcfg hakeeper.Config, currTick uint64,
) int {
	set := make(map[uint64]struct{})
	for _, storeInfo := range state.Stores {
		// ignore expired tn stores
		if hkcfg.TNStoreExpired(storeInfo.Tick, currTick) {
			continue
		}

		for _, shardInfo := range storeInfo.Shards {
			if shardInfo.ShardID == shardID {
				set[shardInfo.ReplicaID] = struct{}{}
			}
		}
	}
	return len(set)
}
