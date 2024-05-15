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
	"go.uber.org/zap"
)

// replicaScheduler used to remove replicas which are in the moving completed state.
//
// balanceSchedule makes a decision to migrate a replica from one CN to another, and
// when the state of the target CN's replica is running, it can remove the From replica.
type replicaScheduler struct {
}

func newReplicaScheduler() scheduler {
	return &replicaScheduler{}
}

func (s *replicaScheduler) schedule(
	r *rt,
	filters ...filter,
) error {
	r.Lock()
	defer r.Unlock()

	for _, t := range r.tables {
		s.doSchedule(r, t)
	}
	return nil
}

func (s *replicaScheduler) doSchedule(
	r *rt,
	t *table,
) {
	i, j, ok := t.getMovingCompletedReplica()
	if !ok {
		return
	}
	getLogger().Info("remove moved replica",
		zap.Uint64("table", t.id),
		zap.String("replica", t.shards[i].Replicas[j].String()),
	)
	t.shards[i].Replicas[j].State = pb.ReplicaState_Tombstone
	replica := t.shards[i].Replicas[j]
	r.addOpLocked(
		replica.CN,
		newDeleteReplicaOp(t.shards[i], replica),
	)
}
