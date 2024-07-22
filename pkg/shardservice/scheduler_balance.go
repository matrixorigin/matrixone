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
	"sort"

	"go.uber.org/zap"
)

type balanceOption func(*balanceScheduler)

func withBalanceOrder(tables []uint64) balanceOption {
	return func(s *balanceScheduler) {
		s.option.orderFunc = func(
			r *rt,
			apply func(*table) (bool, error),
		) error {
			for _, id := range tables {
				ok, err := apply(r.tables[id])
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			return nil
		}
	}
}

// balanceScheduler balance the number of shards between CNs, with the goal of
// ensuring that for each Table, shards are distributed evenly across available
// CNs.
//
// This balancing is an operation to reach a final state of equilibrium, a single
// call will not immediately balance completely, the number of shards adjusted each
// time has an upper limit to ensure that the system can not fluctuate too much.
//
// The balancing algorithm is as follows:
//  1. iterate over each table and perform the balance logic. Adjust up to N tables
//  2. adjust only for tables where all shards are Running state
//  3. select a list of available CNs based on the table, selecting the CN with the
//     fewest shards and the most CNs that contain that table
//  4. if the difference in the number of shards between min_cn and max_cn is <=1
//     then skip
//  5. select a shard from max_cn, migrate to min_cn, only one shards can be moved
//     at a time
//  6. min_cn and max_cn are added to the list of freeze CNs and wait until the
//     scheduling is successful before freeze timed out. Avoid frequent scheduling
//     on the same CN
type balanceScheduler struct {
	freezeFilter         *freezeFilter
	stateFilter          *stateFilter
	filters              []filter
	filterCount          int
	maxTablesPerSchedule int

	// fot testing
	option struct {
		orderFunc func(r *rt, apply func(*table) (bool, error)) error
	}
}

func newBalanceScheduler(
	maxTablesPerSchedule int,
	freezeFilter *freezeFilter,
	opts ...balanceOption,
) scheduler {
	s := &balanceScheduler{
		freezeFilter:         freezeFilter,
		stateFilter:          newStateFilter(),
		maxTablesPerSchedule: maxTablesPerSchedule,
	}
	s.filters = append(s.filters, s.freezeFilter, s.stateFilter)
	s.filterCount = len(s.filters)

	for _, opt := range opts {
		opt(s)
	}
	s.adjust()
	return s
}

func (s *balanceScheduler) adjust() {
	if s.option.orderFunc == nil {
		s.option.orderFunc = func(
			r *rt,
			apply func(*table) (bool, error),
		) error {
			for _, id := range r.tables {
				ok, err := apply(id)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			return nil
		}
	}
}

func (s *balanceScheduler) schedule(
	r *rt,
	filters ...filter,
) error {
	scheduledTables := 0
	r.Lock()
	defer r.Unlock()

	return s.option.orderFunc(
		r,
		func(t *table) (bool, error) {
			ok, err := s.doBalance(r, t, filters...)
			if err != nil {
				return false, err
			}
			if ok {
				scheduledTables++
			}
			return scheduledTables < s.maxTablesPerSchedule, nil
		},
	)
}

func (s *balanceScheduler) doBalance(
	r *rt,
	t *table,
	filters ...filter,
) (bool, error) {
	s.filters = s.filters[:s.filterCount]
	s.filters = append(s.filters, filters...)

	cns := r.getAvailableCNsLocked(
		t,
		s.filters...,
	)
	if len(cns) < 2 {
		return false, nil
	}

	sort.Slice(
		cns,
		func(i, j int) bool {
			return t.getReplicaCount(cns[i].id) < t.getReplicaCount(cns[j].id)
		},
	)

	min := t.getReplicaCount(cns[0].id)
	max := t.getReplicaCount(cns[len(cns)-1].id)
	if max-min <= 1 {
		return false, nil
	}

	from := cns[len(cns)-1].id
	to := cns[0].id

	// We cannot directly remove the migrated replica, because we need to still provide
	// the service during the migration process. The old replica can only be removed when
	// the new replica is running. See replicaScheduler for delete logic.
	shard, new, ok := t.move(
		func(s string) bool {
			return s == from
		},
		func(_ string) string {
			return to
		},
	)
	if !ok {
		panic("cannot find running shard replica")
	}

	r.addOpLocked(to, newAddReplicaOp(shard, new))

	r.logger.Info(
		"move shard",
		zap.Uint64("table", t.id),
		zap.String("from", from),
		zap.Int("from-count", max),
		zap.String("to", to),
		zap.Int("to-count", min),
		zap.String("shard", shard.String()),
		zap.String("new-replica", new.String()),
	)

	s.freezeFilter.add(from, to)
	return true, nil
}
