// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
)

// downScheduler is a scheduler that schedules down shards.
// When a CN is found to be possibly down and it is confirmed in env
// that the CN no longer exists, it will mark the current CN as Down
// and will mark all shards running on the current CN as Tombstone.
//
// Shards in the Tombstone state are reallocated CN by the allocateScheduler.
type downScheduler struct {
	downCNs map[string]struct{}
}

func newDownScheduler() scheduler {
	return &downScheduler{
		downCNs: make(map[string]struct{}),
	}
}

func (s *downScheduler) schedule(
	r *rt,
	filters ...filter,
) error {
	r.Lock()
	defer r.Unlock()

	r.getDownCNsLocked(s.downCNs)
	if len(s.downCNs) == 0 {
		return nil
	}

	for _, shards := range r.tables {
		for i := range shards.shards {
			for j := range shards.shards[i].Replicas {
				if _, ok := s.downCNs[shards.shards[i].Replicas[j].CN]; !ok {
					continue
				}
				shards.shards[i].Replicas[j].CN = ""
				shards.shards[i].Replicas[j].State = pb.ReplicaState_Allocating
			}
		}
	}
	for k := range s.downCNs {
		delete(r.cns, k)
		delete(s.downCNs, k)
	}
	return nil
}
