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

package logservice

import (
	"cmp"
	"maps"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// fixingShard is the replicas of the shardID that required.
type fixingShard struct {
	// shardID is the shard ID.
	shardID uint64
	// They are the replicas after calculated. Other replicas need to be removed.
	// The key of the map is replica ID and the value is store UUID.
	replicas map[uint64]string
	// toAdd is the normal replica number needs to be added.
	toAdd uint32
}

func newFixingShardNormal(origin pb.LogShardInfo) *fixingShard {
	shard := &fixingShard{
		shardID:  origin.ShardID,
		replicas: make(map[uint64]string),
		toAdd:    0,
	}
	for replicaID, uuid := range origin.Replicas {
		shard.replicas[replicaID] = uuid
	}
	return shard
}

func newFixingShardNonVoting(origin pb.LogShardInfo) *fixingShard {
	shard := &fixingShard{
		shardID:  origin.ShardID,
		replicas: make(map[uint64]string),
		toAdd:    0,
	}
	for replicaID, uuid := range origin.NonVotingReplicas {
		shard.replicas[replicaID] = uuid
	}
	return shard
}

func fixedLogShardInfoNormal(
	record metadata.LogShardRecord,
	info pb.LogShardInfo,
	expiredStores []string,
	stores map[string]pb.LogStoreInfo,
) *fixingShard {
	// info.Replicas only contains the normal replicas.
	// info.NonVotingReplicas only contains the non-voting replicas.
	fixing := newFixingShardNormal(info)
	diff := len(fixing.replicas) - int(record.NumberOfReplicas)
	// The number of replicas is less than expected.
	// Record how many replicas should be added.
	if diff < 0 {
		fixing.toAdd = uint32(-diff)
	}

	// Unavailable voters already count toward any surplus.
	for replicaID, uuid := range info.Replicas {
		if isUnavailableVoter(info, stores, expiredStores, replicaID, uuid) {
			delete(fixing.replicas, replicaID)
			// do not remove replicas more than expected.
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected. Remove some of them.
	if diff > 0 {
		idSlice := slices.SortedFunc(maps.Keys(fixing.replicas), votingRemovalOrder(info, stores, expiredStores))
		for i := 0; i < diff; i++ {
			delete(fixing.replicas, idSlice[i])
		}
	}

	return fixing
}

func fixedLogShardInfoNonVoting(
	nonVotingReplicaNum uint64,
	info pb.LogShardInfo,
	expiredStores []string,
) *fixingShard {
	// info.Replicas only contains the normal replicas.
	// info.NonVotingReplicas only contains the non-voting replicas.
	fixing := newFixingShardNonVoting(info)
	// nonVotingReplicaNum is the number of expected non-voting replicas.
	// calculate the expected non-voting replicas number.
	diff := len(fixing.replicas) - int(nonVotingReplicaNum)
	if diff < 0 {
		fixing.toAdd = uint32(-diff)
	}

	// remove the expired replicas.
	for replicaID, uuid := range info.NonVotingReplicas {
		if contains(expiredStores, uuid) {
			delete(fixing.replicas, replicaID)
			if diff > 0 {
				diff--
			}
		}
	}

	// The number of replicas is more than expected. Remove some of them.
	if diff > 0 {
		idSlice := sortedReplicaID(fixing.replicas, info.LeaderID)
		for i := 0; i < diff; i++ {
			delete(fixing.replicas, idSlice[i])
		}
	}

	return fixing
}

// fixedLogShardInfo returns the replicas information after fixing for the shard.
// the first return value is the normal replicas, and the second return value
// is the non-voting replicas.
func fixedLogShardInfo(
	record metadata.LogShardRecord,
	nonVotingReplicaNum uint64,
	info pb.LogShardInfo,
	expiredStores []string,
	stores map[string]pb.LogStoreInfo,
) (*fixingShard, *fixingShard) {
	return fixedLogShardInfoNormal(record, info, expiredStores, stores),
		fixedLogShardInfoNonVoting(nonVotingReplicaNum, info, expiredStores)
}

func getReplicasToRemove(shardID uint64, current, left map[uint64]string) []replica {
	toRemove := make([]replica, 0, len(current)-len(left))
	for id, uuid := range current {
		if _, ok := left[id]; ok {
			continue
		}
		rep := replica{
			uuid:      uuid,
			shardID:   shardID,
			replicaID: id,
		}
		toRemove = append(toRemove, rep)
	}
	return toRemove
}

func getReplicasToStart(
	shardID uint64,
	replicas map[uint64]string,
	replicaStoreIncarnations map[uint64]string,
	stores map[string]pb.LogStoreInfo,
) []replica {
	toStart := make([]replica, 0)
	for id, uuid := range replicas {
		if replicaStoreIncarnationChanged(id, uuid, replicaStoreIncarnations, stores) {
			// This replica belongs to an older persistent storage generation.
			// Starting it with the same replica ID would make Raft peers reuse
			// progress acknowledged by the lost generation.
			continue
		}
		store := stores[uuid]
		if !replicaStarted(shardID, store.Replicas) {
			rep := replica{
				uuid:      uuid,
				shardID:   shardID,
				replicaID: id,
			}
			toStart = append(toStart, rep)
		}
	}
	return toStart
}

func replicaStoreIncarnationChanged(
	replicaID uint64,
	storeID string,
	replicaStoreIncarnations map[uint64]string,
	stores map[string]pb.LogStoreInfo,
) bool {
	replicaIncarnation := replicaStoreIncarnations[replicaID]
	storeIncarnation := stores[storeID].StoreIncarnation
	return replicaIncarnation != "" &&
		storeIncarnation != "" &&
		replicaIncarnation != storeIncarnation
}

func isUnavailableVoter(
	shard pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
	expired []string,
	replicaID uint64,
	uuid string,
) bool {
	return contains(expired, uuid) ||
		replicaStoreIncarnationChanged(replicaID, uuid, shard.ReplicaStoreIncarnations, stores)
}

func removeReplicasOnChangedStoreIncarnation(
	fixed *fixingShard,
	shardInfo pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
) {
	for replicaID, storeID := range fixed.replicas {
		if replicaStoreIncarnationChanged(
			replicaID,
			storeID,
			shardInfo.ReplicaStoreIncarnations,
			stores,
		) {
			delete(fixed.replicas, replicaID)
		}
	}
}

// isReportingVotingExecutor verifies that target still represents its configured
// voting replica and reported that exact identity in the current heartbeat.
func isReportingVotingExecutor(
	shard pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
	expired []string,
	target string,
) bool {
	for replicaID, uuid := range shard.Replicas {
		if uuid != target || contains(expired, uuid) {
			continue
		}
		return hakeeper.ReportsLogVoter(shard, replicaID, stores[uuid])
	}
	return false
}

// IsReportingVotingExecutor reports whether target is a live, current voter for
// this shard. It is shared with coordinator cleanup of stale membership work.
func IsReportingVotingExecutor(
	shard pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
	expired []string,
	target string,
) bool {
	return isReportingVotingExecutor(shard, stores, expired, target)
}

// votingRemovalOrder ranks the full surplus set before taking the requested
// number of removals. At minimum quorum an unreported voter can be removed,
// whereas removing a reporting voter can leave the new configuration short.
func votingRemovalOrder(shard pb.LogShardInfo, stores map[string]pb.LogStoreInfo, expired []string) func(uint64, uint64) int {
	reporters := reportingVotingExecutors(shard, stores, expired)
	voters := len(shard.Replicas)
	rank := func(id uint64) int {
		uuid := shard.Replicas[id]
		if isUnavailableVoter(shard, stores, expired, id, uuid) {
			return 0
		}
		_, reporting := slices.BinarySearch(reporters, uuid)
		if !hakeeper.HasQuorumAfterVotingRemoval(voters, len(reporters), reporting) {
			if id == shard.LeaderID {
				return 5
			}
			return 4
		}
		if id == shard.LeaderID {
			return 3
		}
		if reporting {
			return 2
		}
		return 1
	}
	return func(a, b uint64) int {
		if order := cmp.Compare(rank(a), rank(b)); order != 0 {
			return order
		}
		return cmp.Compare(a, b)
	}
}

func canRemoveVotingReplica(
	shard pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
	expired []string,
	replicaToRemove replica,
) bool {
	voters := len(shard.Replicas)
	if voters < 2 || shard.Replicas[replicaToRemove.replicaID] != replicaToRemove.uuid {
		return false
	}

	reporters := reportingVotingExecutors(shard, stores, expired)
	_, reporting := slices.BinarySearch(reporters, replicaToRemove.uuid)
	return hakeeper.HasQuorumAfterVotingRemoval(voters, len(reporters), reporting)
}

// CanRemoveVotingReplica checks projected quorum for a retained operator.
func CanRemoveVotingReplica(shard pb.LogShardInfo, stores map[string]pb.LogStoreInfo, expired []string, replicaID uint64, uuid string) bool {
	return canRemoveVotingReplica(shard, stores, expired, replica{replicaID: replicaID, uuid: uuid})
}

// reportingVotingExecutors returns configured voters that are live and have
// reported the same voter identity for this shard in the current heartbeat.
func reportingVotingExecutors(
	shard pb.LogShardInfo,
	stores map[string]pb.LogStoreInfo,
	expired []string,
) []string {
	executors := make([]string, 0, len(shard.Replicas))
	for replicaID, uuid := range shard.Replicas {
		if !contains(expired, uuid) && hakeeper.ReportsLogVoter(shard, replicaID, stores[uuid]) {
			executors = append(executors, uuid)
		}
	}
	slices.Sort(executors)
	return executors
}

// hasVotingMajority reports whether current voting membership has a strict
// majority of live stores reporting their configured voter identities.
func hasVotingMajority(shard pb.LogShardInfo, stores map[string]pb.LogStoreInfo, expired []string) bool {
	return len(shard.Replicas) > 0 &&
		len(reportingVotingExecutors(shard, stores, expired)) > len(shard.Replicas)/2
}

// canRepairVotingMembership limits configuration changes to reporting voters.
// If any configured voter is unavailable, a strict majority must remain.
func canRepairVotingMembership(shard pb.LogShardInfo, stores map[string]pb.LogStoreInfo, expired []string) bool {
	// Empty voter sets are the initial bootstrap case, which has no existing
	// configuration to change and is handled by checkToBootstrap.
	if len(shard.Replicas) == 0 {
		return true
	}
	// Preserve the existing no-failure policy. When a voter is expired (or its
	// storage incarnation changed on main), require a reporting majority before
	// scheduling any membership change.
	for replicaID, uuid := range shard.Replicas {
		if isUnavailableVoter(shard, stores, expired, replicaID, uuid) {
			return len(reportingVotingExecutors(shard, stores, expired)) > len(shard.Replicas)/2
		}
	}
	return true
}

// parseLogShards collects stats for further use.
// It returns two stats, the first is the stats for normal replicas
// and the second one is for the non-voting replicas.
func parseLogShards(
	cluster pb.ClusterInfo, infos pb.LogState, expired []string, nonVotingReplicaNum uint64,
) (*stats, *stats) {
	normalStats, nonVotingStats := newStats(false), newStats(true)
	for _, shardInfo := range infos.Shards {
		shardID := shardInfo.ShardID
		record := getRecord(shardID, cluster.LogShards)
		fixingNormal, fixingNonVoting := fixedLogShardInfo(record, nonVotingReplicaNum, shardInfo, expired, infos.Stores)
		removeReplicasOnChangedStoreIncarnation(fixingNonVoting, shardInfo, infos.Stores)

		// cal the toRemove field.
		toRemoveNonVoting := getReplicasToRemove(shardID, shardInfo.NonVotingReplicas, fixingNonVoting.replicas)

		// cal the toStart field.
		toStartNormal := getReplicasToStart(
			shardID,
			shardInfo.Replicas,
			shardInfo.ReplicaStoreIncarnations,
			infos.Stores,
		)
		toStartNonVoting := getReplicasToStart(
			shardID,
			shardInfo.NonVotingReplicas,
			shardInfo.ReplicaStoreIncarnations,
			infos.Stores,
		)

		if canRepairVotingMembership(shardInfo, infos.Stores, expired) {
			if fixingNormal.toAdd > 0 {
				normalStats.toAdd[shardID] = fixingNormal.toAdd
			}
			if removals := getReplicasToRemove(shardID, shardInfo.Replicas, fixingNormal.replicas); len(removals) > 0 {
				if len(removals) > 1 {
					order := votingRemovalOrder(shardInfo, infos.Stores, expired)
					slices.SortFunc(removals, func(a, b replica) int { return order(a.replicaID, b.replicaID) })
				}
				normalStats.toRemove[shardID] = removals
			}
			if fixingNonVoting.toAdd > 0 {
				nonVotingStats.toAdd[shardID] = fixingNonVoting.toAdd
			}
			if len(toRemoveNonVoting) > 0 {
				nonVotingStats.toRemove[shardID] = toRemoveNonVoting
			}
		}
		normalStats.toStart = append(normalStats.toStart, toStartNormal...)
		nonVotingStats.toStart = append(nonVotingStats.toStart, toStartNonVoting...)
	}
	// Check zombies
	for uuid, storeInfo := range infos.Stores {
		if contains(expired, uuid) {
			continue
		}
		zombie := make([]replica, 0)
		for _, replicaInfo := range storeInfo.Replicas {
			_, ok := infos.Shards[replicaInfo.ShardID].Replicas[replicaInfo.ReplicaID]
			if ok || replicaInfo.Epoch >= infos.Shards[replicaInfo.ShardID].Epoch {
				continue
			}
			_, ok = infos.Shards[replicaInfo.ShardID].NonVotingReplicas[replicaInfo.ReplicaID]
			if ok {
				continue
			}
			zombie = append(zombie, replica{uuid: uuid, shardID: replicaInfo.ShardID,
				replicaID: replicaInfo.ReplicaID})
		}
		normalStats.zombies = append(normalStats.zombies, zombie...)
	}
	return normalStats, nonVotingStats
}

// parseLogStores returns all expired stores' ids.
// The first return value is working stores, it is a map, key is uuid, value is locality.
// The second return value is expired stores.
func parseLogStores(cfg hakeeper.Config, infos pb.LogState, currentTick uint64) (map[string]pb.Locality, []string) {
	working := make(map[string]pb.Locality, 0)
	expired := make([]string, 0)
	for uuid, storeInfo := range infos.Stores {
		if cfg.LogStoreExpired(storeInfo.Tick, currentTick) {
			expired = append(expired, uuid)
		} else {
			working[uuid] = storeInfo.Locality
		}
	}
	return working, expired
}

// getRecord returns the LogShardRecord with the given shardID.
func getRecord(shardID uint64, LogShards []metadata.LogShardRecord) metadata.LogShardRecord {
	for _, record := range LogShards {
		if record.ShardID == shardID {
			return record
		}
	}
	return metadata.LogShardRecord{}
}

// sortedReplicaID returns a sorted replica id slice with leader at last.
// The first <expected-current> replicas will be removed as current replica
// num is larger than expected. So we put the leader replica at last to make
// sure that no leader election happened if replicas are removed.
func sortedReplicaID(replicas map[uint64]string, leaderID uint64) []uint64 {
	var exist bool
	idSlice := make([]uint64, 0, len(replicas))
	for id := range replicas {
		if id != leaderID {
			idSlice = append(idSlice, id)
		} else {
			exist = true
		}
	}
	slices.Sort(idSlice)
	if exist {
		idSlice = append(idSlice, leaderID)
	}
	return idSlice
}

// replicaStarted checks if a replica is started in LogReplicaInfo.
func replicaStarted(shardID uint64, replicas []pb.LogReplicaInfo) bool {
	for _, r := range replicas {
		if r.ShardID == shardID {
			return true
		}
	}
	return false
}
