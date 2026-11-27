// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// membershipTargets returns reporting, retained voters that can submit the
// change with a reporting majority in both configurations.
func membershipTargets(cfg hakeeper.Config, state pb.LogState, tick uint64, change pb.ConfigChangeType, replica pb.Replica) []string {
	shard, ok := state.Shards[replica.ShardID]
	n := len(shard.Replicas)
	if !ok || shard.Epoch != replica.Epoch || n == 0 {
		return nil
	}
	voting := change == pb.AddReplica || change == pb.RemoveReplica
	removing := change == pb.RemoveReplica || change == pb.RemoveNonVotingReplica
	switch change {
	case pb.RemoveReplica:
		if shard.Replicas[replica.ReplicaID] != replica.UUID {
			return nil
		}
	case pb.RemoveNonVotingReplica:
		if shard.NonVotingReplicas[replica.ReplicaID] != replica.UUID {
			return nil
		}
	case pb.AddReplica, pb.AddNonVotingReplica:
		store, exists := state.Stores[replica.UUID]
		if replica.UUID == "" || !exists || cfg.LogStoreExpired(store.Tick, tick) {
			return nil
		}
		if _, exists := shard.Replicas[replica.ReplicaID]; exists {
			return nil
		}
		if _, exists := shard.NonVotingReplicas[replica.ReplicaID]; exists {
			return nil
		}
		for _, uuid := range shard.Replicas {
			if uuid == replica.UUID {
				return nil
			}
		}
		for _, uuid := range shard.NonVotingReplicas {
			if uuid == replica.UUID {
				return nil
			}
		}
	default:
		return nil
	}

	resulting := n
	if voting {
		if removing {
			resulting--
		} else {
			resulting++
		}
	}
	if resulting == 0 {
		return nil
	}

	var targets []string
	active := 0
	retained := 0
	for id, uuid := range shard.Replicas {
		store, exists := state.Stores[uuid]
		if !exists || cfg.LogStoreExpired(store.Tick, tick) {
			continue
		}
		oldIncarnation := shard.ReplicaStoreIncarnations[id]
		if oldIncarnation != "" && store.StoreIncarnation != "" && oldIncarnation != store.StoreIncarnation {
			continue
		}
		reported := false
		for _, r := range store.Replicas {
			if r.ShardID == replica.ShardID && r.ReplicaID == id && !r.IsNonVoting {
				reported = true
				break
			}
		}
		if !reported {
			continue
		}
		active++
		if change != pb.RemoveReplica || id != replica.ReplicaID {
			retained++
			targets = append(targets, uuid)
		}
	}
	if active < n/2+1 || retained < resulting/2+1 {
		return nil
	}
	slices.Sort(targets)
	return targets
}
