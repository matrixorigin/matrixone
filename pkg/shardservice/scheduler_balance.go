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
	"time"
)

// balanceScheduler balance the number of shards between CNs, with the goal of
// ensuring that for each Table, shards are distributed evenly across available
// CNs.
//
// This balancing is an operation to reach a final state of equilibrium, a single
// call will not immediately balance completely, the number of shards adjusted each
// time has an upper limit to ensure that the system can not fluctuate too much.
//
// The balancing algorithm is as follows:
//  1. Iterate over each table and perform the equalization logic. Adjust up to N Tables
//  2. Adjust only for tables where all shards are Running state
//  3. Select a list of available CNs based on the table, selecting the CN with the fewest
//     shards and the most CNs that contain that table
//  4. If the difference in the number of shards between min_cn and max_cn is <=1 then skip
//  5. Select a shard from max_cn, migrate to min_cn, only one shards can be moved at a time
//  6. min_cn and max_cn are added to the list of freeze CNs and wait until the scheduling is
//     successful before replying. Avoid frequent scheduling on the same CN
type balanceScheduler struct {
	freezeFilter         *freezeFilter
	stateFilter          *stateFilter
	filters              []filter
	filterCount          int
	maxTablesPerSchedule int
}

func newBalanceScheduler(
	maxTablesPerSchedule int,
	maxFreezeCNTimeout time.Duration,
) scheduler {
	s := &balanceScheduler{
		freezeFilter:         newFreezeFilter(maxFreezeCNTimeout),
		stateFilter:          &stateFilter{},
		maxTablesPerSchedule: maxTablesPerSchedule,
	}
	s.filters = append(s.filters, s.freezeFilter, s.stateFilter)
	s.filterCount = len(s.filters)
	return s
}

func (s *balanceScheduler) schedule(
	r *rt,
	filters ...filter,
) error {
	scheduledTables := 0
	r.Lock()
	defer r.Unlock()
	for _, t := range r.tables {
		ok, err := s.doBalance(r, t, filters...)
		if err != nil {
			return err
		}
		if ok {
			scheduledTables++
		}

		if scheduledTables >= s.maxTablesPerSchedule {
			break
		}
	}
	return nil
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
			return t.getShardsCount(cns[i].id) < t.getShardsCount(cns[j].id)
		},
	)

	min := t.getShardsCount(cns[0].id)
	max := t.getShardsCount(cns[len(cns)-1].id)
	if max-min <= 1 {
		return false, nil
	}

	from := cns[len(cns)-1].id
	to := cns[0].id

	old, new := t.moveLocked(from, to)
	r.addOpLocked(from, newDeleteOp(old))
	r.addOpLocked(to, newAddOp(new))

	now := time.Now()
	s.freezeFilter.freeze[from] = now
	s.freezeFilter.freeze[to] = now
	return true, nil
}
