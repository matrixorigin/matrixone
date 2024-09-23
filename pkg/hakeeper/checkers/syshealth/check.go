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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	defaultLogShardSize = 3
)

// Check checks system healthy or not.
// If system wasn't healthy, we would generate
// operators in order to shut down all stores.
func Check(
	cfg hakeeper.Config,
	cluster pb.ClusterInfo,
	tnState pb.TNState,
	logState pb.LogState,
	currTick uint64,
) ([]*operator.Operator, bool) {
	sysHealthy := true

	// parse all log stores for expired stores mainly
	logStores := parseLogState(cfg, logState, currTick)
	if len(logStores.expired) == 0 {
		return nil, sysHealthy
	}

	// check system healthy or not
	expiredShards := listExpiredShards(logStores.expired, logStores.working, logState, cluster)
	for _, shard := range expiredShards {
		// if one of log shards wasn't healthy, the entire system wasn't healthy.
		if !shard.healthy() {
			sysHealthy = false
			break
		}
	}

	if sysHealthy {
		return nil, sysHealthy
	}

	detail := "Expired logStore info..."
	for uuid := range logStores.expired {
		detail += fmt.Sprintf("store %s replicas: [", uuid)
		for _, replicaInfo := range logState.Stores[uuid].Replicas {
			detail += fmt.Sprintf("%d-%d, ", replicaInfo.ShardID, replicaInfo.ReplicaID)
		}
		detail += "]; "
	}

	logutil.GetGlobalLogger().Info(detail)

	// parse all tn stores
	tnStores := parseTnState(cfg, tnState, currTick)

	// generate operators to shut down all stores
	operators := make([]*operator.Operator, 0, logStores.length()+tnStores.length())
	operators = append(operators, logStores.shutdownExpiredStores()...)
	operators = append(operators, logStores.shutdownWorkingStores()...)
	operators = append(operators, tnStores.shutdownExpiredStores()...)
	operators = append(operators, tnStores.shutdownWorkingStores()...)

	return operators, sysHealthy
}

// logShardMap is just a syntax sugar.
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

// registerWorkingReplica registers replica as working.
func (m logShardMap) registerWorkingReplica(replica pb.LogReplicaInfo, cluster pb.ClusterInfo) {
	replicaID := replica.ReplicaID
	shardID := replica.ShardID

	if _, ok := m[shardID]; !ok {
		m[shardID] = newLogShard(shardID, getLogShardSize(shardID, cluster))
	}
	m[shardID].registerWorkingReplica(replicaID)
}

// listExpiredShards lists those shards which has expired replica.
func listExpiredShards(
	expiredStores map[string]struct{},
	workingStores map[string]struct{},
	logState pb.LogState,
	cluster pb.ClusterInfo,
) logShardMap {
	expired := newLogShardMap()

	// register log shards on expired stores
	for id := range expiredStores {
		expiredReplicas := logState.Stores[id].Replicas
		for _, replica := range expiredReplicas {
			expired.registerExpiredReplica(replica, cluster)
		}
	}

	// register working replica for
	for id := range workingStores {
		workingReplicas := logState.Stores[id].Replicas
		for _, replica := range workingReplicas {
			// only register working replica for expired shards
			if _, ok := expired[replica.ShardID]; ok {
				expired.registerWorkingReplica(replica, cluster)
			}
		}
	}

	return expired
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
	workingReplicas  map[uint64]struct{}
}

func newLogShard(shardID uint64, numberOfReplicas uint64) *logShard {
	return &logShard{
		shardID:          shardID,
		numberOfReplicas: numberOfReplicas,
		expiredReplicas:  make(map[uint64]struct{}),
		workingReplicas:  make(map[uint64]struct{}),
	}
}

// registerExpiredReplica registers expired replica ID.
func (s *logShard) registerExpiredReplica(replicaID uint64) {
	s.expiredReplicas[replicaID] = struct{}{}
}

// registerWorkingReplica registers working replica ID.
func (s *logShard) registerWorkingReplica(replicaID uint64) {
	s.workingReplicas[replicaID] = struct{}{}
}

// healthy checks whether log shard working or not.
func (s *logShard) healthy() bool {
	if s.numberOfReplicas > 0 &&
		len(s.workingReplicas)*2 > int(s.numberOfReplicas) {
		return true
	}
	return false
}

// storeSet separates stores as expired and working.
type storeSet struct {
	serviceType pb.ServiceType
	working     map[string]struct{}
	expired     map[string]struct{}
}

func newStoreSet(serviceType pb.ServiceType) *storeSet {
	return &storeSet{
		serviceType: serviceType,
		working:     make(map[string]struct{}),
		expired:     make(map[string]struct{}),
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

// shutdownWorkingStores generates operators to shut down working stores.
func (s *storeSet) shutdownWorkingStores() []*operator.Operator {
	return shutdownStores(s.serviceType, s.working)
}

// parseLogState separates log stores as expired and working.
func parseLogState(cfg hakeeper.Config, logState pb.LogState, currTick uint64) *storeSet {
	set := newStoreSet(pb.LogService)
	for id, storeInfo := range logState.Stores {
		if cfg.LogStoreExpired(storeInfo.Tick, currTick) {
			set.expired[id] = struct{}{}
		} else {
			set.working[id] = struct{}{}
		}
	}
	return set
}

// parseTnState separates tn stores as expired and working.
func parseTnState(cfg hakeeper.Config, tnState pb.TNState, currTick uint64) *storeSet {
	set := newStoreSet(pb.TNService)
	for id, storeInfo := range tnState.Stores {
		if cfg.TNStoreExpired(storeInfo.Tick, currTick) {
			set.expired[id] = struct{}{}
		} else {
			set.working[id] = struct{}{}
		}
	}
	return set
}

// shutdownStores generates operators to shut down stores.
func shutdownStores(serviceType pb.ServiceType, stores map[string]struct{}) []*operator.Operator {
	ops := make([]*operator.Operator, 0, len(stores))

	switch serviceType {
	case pb.LogService:
		for id := range stores {
			op := operator.NewOperator(
				"logservice", operator.NoopShardID, operator.NoopEpoch,
				operator.StopLogStore{StoreID: id},
			)
			ops = append(ops, op)
		}
	case pb.TNService:
		for id := range stores {
			op := operator.NewOperator(
				"tnservice", operator.NoopShardID, operator.NoopEpoch,
				operator.StopTnStore{StoreID: id},
			)
			ops = append(ops, op)
		}
	default:
		panic("unexpected service type")
	}

	return ops
}
