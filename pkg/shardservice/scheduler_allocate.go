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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// allocateScheduler is a scheduler that allocates shards to CNs.
// Shards that meet one of the following 2 conditions will be assigned a CN:
// 1. shard.cn == "", means the shard is new created.
// 2. shard.state == Tombstone, means the old CN is down.
type allocateScheduler struct {
}

func newAllocateScheduler() scheduler {
	return &allocateScheduler{}
}

func (s *allocateScheduler) schedule(
	r *rt,
	filters ...filter,
) error {
	r.Lock()
	defer r.Unlock()

	for _, t := range r.tables {
		s.doAllocate(r, t, filters...)
	}
	return nil
}

func (s *allocateScheduler) doAllocate(
	r *rt,
	t *table,
	filters ...filter,
) {
	if !t.needAllocate() {
		return
	}

	cns := r.getAvailableCNsLocked(t, filters...)
	if len(cns) == 0 {
		r.logger.Warn("no available CNs for allocate",
			zap.Uint64("table", t.id),
			zap.Any("cns", r.cns),
		)
		v2.ReplicaScheduleSkipWithNoCNCounter.Add(1)
		return
	}

	r.logger.Info("ready to allocate shard replica",
		zap.Uint64("table", t.id),
		zap.Any("cns", r.cns),
	)

	seq := 0
	getCN := func() string {
		defer func() {
			seq++
		}()
		return cns[seq%len(cns)].id
	}
	n := 0
	for i := range t.shards {
		for j := range t.shards[i].Replicas {
			if t.shards[i].Replicas[j].CN == "" {
				cn := getCN()
				t.allocate(cn, i, j)
				r.logger.Info("allocate shard replica",
					zap.String("shard", t.shards[i].String()),
					zap.String("replica", t.shards[i].Replicas[j].String()),
				)
				n++
				r.addOpLocked(
					cn,
					newAddReplicaOp(t.shards[i], t.shards[i].Replicas[j]),
				)
			}
		}
	}
	if n > 0 {
		v2.ReplicaScheduleAllocateReplicaCounter.Add(float64(n))
	}
}
