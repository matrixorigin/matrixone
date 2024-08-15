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
	freezeFilter  *freezeFilter
	excludeFilter *excludeFilter
	pausedCNs     map[string]struct{}
	filters       []filter
	filterCount   int
}

func newReplicaScheduler(
	freezeFilter *freezeFilter,
) scheduler {
	s := &replicaScheduler{
		freezeFilter:  freezeFilter,
		excludeFilter: newExcludeFilter(),
		pausedCNs:     make(map[string]struct{}),
	}
	s.filters = append(s.filters, s.freezeFilter, s.excludeFilter)
	s.filterCount = len(s.filters)
	return s
}

func (s *replicaScheduler) schedule(
	r *rt,
	filters ...filter,
) error {
	r.Lock()
	defer r.Unlock()

	r.getPausedCNLocked(s.pausedCNs)
	for _, t := range r.tables {
		s.doSchedule(r, t, filters...)
	}
	for k := range s.pausedCNs {
		delete(s.pausedCNs, k)
	}
	return nil
}

func (s *replicaScheduler) doSchedule(
	r *rt,
	t *table,
	filters ...filter,
) {
	s.scheduleMoveCompleted(
		r,
		t,
		filters...,
	)
	s.scheduleMovePauseCNReplicas(
		r,
		t,
		filters...,
	)
}

func (s *replicaScheduler) scheduleMoveCompleted(
	r *rt,
	t *table,
	_ ...filter,
) {
	i, j, ok := t.getMoveCompletedReplica()
	if !ok {
		return
	}
	r.logger.Info("remove move completed replica",
		zap.Uint64("table", t.id),
		zap.String("shard", t.shards[i].String()),
		zap.String("replica", t.shards[i].Replicas[j].String()),
	)

	// replica will removed on the next heartbeat
	t.shards[i].Replicas[j].State = pb.ReplicaState_Tombstone
	replica := t.shards[i].Replicas[j]
	r.addOpLocked(
		replica.CN,
		newDeleteReplicaOp(t.shards[i], replica),
	)
}

func (s *replicaScheduler) scheduleMovePauseCNReplicas(
	r *rt,
	t *table,
	filters ...filter,
) {
	if len(s.pausedCNs) == 0 {
		return
	}
	s.filters = s.filters[:s.filterCount]
	s.filters = append(s.filters, filters...)

	cns := r.getAvailableCNsLocked(t, s.filters...)
	if len(cns) == 0 {
		return
	}

	has := false
	for cn := range s.pausedCNs {
		if !r.env.Available(t.metadata.AccountID, cn) {
			continue
		}
		has = true
		break
	}

	if !has {
		return
	}

	fromCN := ""
	toCN := ""
	shard, new, ok := t.move(
		func(from string) bool {
			_, ok := s.pausedCNs[from]
			if ok {
				s.excludeFilter.add(from)
				fromCN = from
			}
			return ok
		},
		func(from string) string {
			cns = r.filterCN(cns, s.filters...)
			if len(cns) == 0 {
				return ""
			}
			toCN = cns[0].id
			return cns[0].id
		},
	)
	if !ok {
		return
	}

	s.excludeFilter.reset()
	s.freezeFilter.add(fromCN, toCN)
	r.logger.Info("move replica out of paused CN",
		zap.String("from", fromCN),
		zap.String("to", toCN),
		zap.String("shard", shard.String()),
		zap.String("new-replica", new.String()),
	)
	r.addOpLocked(toCN, newAddReplicaOp(shard, new))
}
