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
	"time"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type freezeFilter struct {
	freeze        map[string]time.Time
	maxFreezeTime time.Duration
}

func newFreezeFilter(maxFreezeTime time.Duration) *freezeFilter {
	return &freezeFilter{
		freeze:        make(map[string]time.Time),
		maxFreezeTime: maxFreezeTime,
	}
}

func (f *freezeFilter) add(cns ...string) {
	now := time.Now()
	for _, cn := range cns {
		f.freeze[cn] = now
	}
	v2.ReplicaFreezeCNCountGauge.Set(float64(len(f.freeze)))
}

func (f *freezeFilter) filter(
	r *rt,
	cns []*cn,
) []*cn {
	if len(f.freeze) == 0 {
		return cns
	}
	values := cns[:0]
	for _, c := range cns {
		if t, ok := f.freeze[c.id]; !ok ||
			time.Since(t) > f.maxFreezeTime {
			values = append(values, c)
			delete(f.freeze, c.id)
			v2.ReplicaFreezeCNCountGauge.Set(float64(len(f.freeze)))
		} else {
			v2.ReplicaScheduleSkipFreezeCNCounter.Add(1)
		}
	}
	return values
}

type stateFilter struct {
}

func newStateFilter() *stateFilter {
	return &stateFilter{}
}

func (f *stateFilter) filter(
	r *rt,
	cns []*cn,
) []*cn {
	if len(cns) == 0 {
		return cns
	}
	values := cns[:0]
	for _, c := range cns {
		if !r.hasNotRunningShardLocked(c.id) {
			values = append(values, c)
		}
	}
	return values
}

type excludeFilter struct {
	values map[string]struct{}
}

func newExcludeFilter() *excludeFilter {
	return &excludeFilter{
		values: make(map[string]struct{}),
	}
}

func (f *excludeFilter) filter(
	r *rt,
	cns []*cn,
) []*cn {
	if len(cns) == 0 {
		return cns
	}
	values := cns[:0]
	for _, c := range cns {
		if _, ok := f.values[(c.id)]; !ok {
			values = append(values, c)
		}
	}
	return values
}

func (f *excludeFilter) add(cns ...string) {
	for _, cn := range cns {
		f.values[cn] = struct{}{}
	}
}

func (f *excludeFilter) reset() {
	for k := range f.values {
		delete(f.values, k)
	}
}
