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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ReplicaInfo struct {
	UUID           string
	ServiceAddress string
}

type ShardInfo struct {
	ShardID  uint64
	Replicas map[uint64]ReplicaInfo
	Epoch    uint64
	LeaderID uint64
	Term     uint64
}

func (s *Service) GetShardInfo(shardID uint64) (ShardInfo, bool) {
	r, ok := s.store.nh.GetNodeHostRegistry()
	if !ok {
		panic(moerr.NewError(moerr.INVALID_STATE, "gossip registry not enabled"))
	}
	shard, ok := r.GetShardInfo(shardID)
	if !ok {
		return ShardInfo{}, false
	}
	result := ShardInfo{
		ShardID:  shard.ShardID,
		Epoch:    shard.ConfigChangeIndex,
		LeaderID: shard.LeaderID,
		Term:     shard.Term,
		Replicas: make(map[uint64]ReplicaInfo),
	}
	for nodeID, uuid := range shard.Nodes {
		data, ok := r.GetMeta(uuid)
		if !ok {
			return ShardInfo{}, false
		}
		var md storeMeta
		md.unmarshal(data)
		result.Replicas[nodeID] = ReplicaInfo{
			UUID:           uuid,
			ServiceAddress: md.serviceAddress,
		}
	}
	return result, true
}
