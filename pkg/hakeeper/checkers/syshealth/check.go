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

package syshealth

import (
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	defaultLogShardSize = 3
)

// Check checks system healthy or not.
// If system wasn't healthy, we would generate
// operators in order to shutdown all stores.
func Check(
	cluster pb.ClusterInfo,
	dnState pb.DNState,
	logState pb.LogState,
	currTick uint64,
) ([]*operator.Operator, bool) {
	sysHealthy := true

	// parse all log stores for expired stores mainly
	logStores := parseLogStores(logState, currTick)
	if len(logStores.expired) == 0 {
		return nil, sysHealthy
	}

	// check system healthy or not
	for _, shard := range logShardsWithExpired(logStores.expired, logState, cluster) {
		// if one of log shards wasn't healthy, the entire system wasn't healthy.
		if !shard.healthy() {
			sysHealthy = false
			break
		}
	}

	if sysHealthy {
		return nil, sysHealthy
	}

	// parse all dn stores
	dnStores := parseDnStores(dnState, currTick)

	// generate operators to shutdown all stores
	operators := make([]*operator.Operator, 0, logStores.length()+dnStores.length())
	operators = append(operators, logStores.shutdownExpiredStores()...)
	operators = append(operators, logStores.shutdownWorkingStores()...)
	operators = append(operators, dnStores.shutdownExpiredStores()...)
	operators = append(operators, dnStores.shutdownWorkingStores()...)

	return operators, sysHealthy
}

// logShardMap is just an syntax sugar.
type logShardMap map[uint64]*logShard

func newLogShardMap() logShardMap {
	return make(map[uint64]*logShard)
}

// registerExpiredReplica registers replica as expired.
func (m logShardMap) registerExpiredReplica(replica pb.LogReplicaInfo, cluster pb.ClusterInfo) {
	replicaID := replica.ReplicaID
	shardID := replica.ShardID

	if _, ok := m[shardID]; !ok {
		m[shardID] = newLogShard(shardID, getLogShardSize(shardID, cluster))
	}
	m[shardID].registerExpiredReplica(replicaID)
}

// logShardsWithExpired gathers metadata for expired log shards.
func logShardsWithExpired(
	expiredStores map[util.StoreID]struct{},
	logState pb.LogState,
	cluster pb.ClusterInfo,
) logShardMap {
	expiredShardMap := newLogShardMap()
	for id := range expiredStores {
		expiredReplicas := logState.Stores[string(id)].Replicas
		for _, replica := range expiredReplicas {
			expiredShardMap.registerExpiredReplica(replica, cluster)
		}
	}
	return expiredShardMap
}

// getLogShardSize gets raft group size for the specified shard.
func getLogShardSize(shardID uint64, cluster pb.ClusterInfo) uint64 {
	for _, shardMeta := range cluster.LogShards {
		if shardMeta.ShardID == shardID {
			return shardMeta.NumberOfReplicas
		}
	}
	return defaultLogShardSize
}

// logShard records metadata for log shard.
type logShard struct {
	shardID          uint64
	numberOfReplicas uint64
	expiredReplicas  map[uint64]struct{}
}

func newLogShard(shardID uint64, numberOfReplicas uint64) *logShard {
	return &logShard{
		shardID:          shardID,
		numberOfReplicas: numberOfReplicas,
		expiredReplicas:  make(map[uint64]struct{}),
	}
}

// registerExpiredReplica registers expired replica ID.
func (s *logShard) registerExpiredReplica(replicaID uint64) {
	s.expiredReplicas[replicaID] = struct{}{}
}

// healthy checks whether log shard working or not.
func (s *logShard) healthy() bool {
	if s.numberOfReplicas > 0 &&
		len(s.expiredReplicas)*2 >= int(s.numberOfReplicas) {
		return false
	}
	return true
}

// storeSet separates stores as expired and working.
type storeSet struct {
	serviceType pb.ServiceType
	working     map[util.StoreID]struct{}
	expired     map[util.StoreID]struct{}
}

func newStoreSet(serviceType pb.ServiceType) *storeSet {
	return &storeSet{
		serviceType: serviceType,
		working:     make(map[util.StoreID]struct{}),
		expired:     make(map[util.StoreID]struct{}),
	}
}

// length returns number of all stores within this set.
func (s *storeSet) length() int {
	return len(s.working) + len(s.expired)
}

// shutdownExpiredStores
func (s *storeSet) shutdownExpiredStores() []*operator.Operator {
	return shutdownStores(s.serviceType, s.expired)
}

// shutdownWorkingStores generates operators to shutdown working stores.
func (s *storeSet) shutdownWorkingStores() []*operator.Operator {
	return shutdownStores(s.serviceType, s.working)
}

// parseLogStores separates log stores as expired and working.
func parseLogStores(logState pb.LogState, currTick uint64) *storeSet {
	set := newStoreSet(pb.LogService)
	for id, storeInfo := range logState.Stores {
		if hakeeper.ExpiredTick(storeInfo.Tick, hakeeper.LogStoreTimeout) < currTick {
			set.expired[util.StoreID(id)] = struct{}{}
		} else {
			set.working[util.StoreID(id)] = struct{}{}
		}
	}
	return set
}

// parseDnStores separates dn stores as expired and working.
func parseDnStores(dnState pb.DNState, currTick uint64) *storeSet {
	set := newStoreSet(pb.DnService)
	for id, storeInfo := range dnState.Stores {
		if hakeeper.ExpiredTick(storeInfo.Tick, hakeeper.DnStoreTimeout) < currTick {
			set.expired[util.StoreID(id)] = struct{}{}
		} else {
			set.working[util.StoreID(id)] = struct{}{}
		}
	}
	return set
}

// shutdownStores generates operators to shutdown stores.
func shutdownStores(serviceType pb.ServiceType, stores map[util.StoreID]struct{}) []*operator.Operator {
	ops := make([]*operator.Operator, 0, len(stores))

	switch serviceType {
	case pb.LogService:
		for id := range stores {
			op := operator.NewOperator(
				"logservice", operator.NoopShardID, operator.NoopEpoch,
				operator.StopLogStore{StoreID: string(id)},
			)
			ops = append(ops, op)
		}
	case pb.DnService:
		for id := range stores {
			op := operator.NewOperator(
				"dnservice", operator.NoopShardID, operator.NoopEpoch,
				operator.StopDnStore{StoreID: string(id)},
			)
			ops = append(ops, op)
		}
	default:
		panic("unexpected service type")
	}

	return ops
}
