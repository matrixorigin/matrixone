// Copyright 2021 Matrix Origin
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

package dbutils

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/shirou/gopsutil/v3/mem"
)

const ObjectMemUnit = mpool.MB * 64

type ThrottleOption func(*Throttle)

func WithThrottleCompactionMaxTasksLimit(limit int32) ThrottleOption {
	return func(t *Throttle) {
		t.Compaction.MaxActiveTasksLimit = limit
	}
}

func WithThrottleCompactionMaxMemUsedLimit(limit int64) ThrottleOption {
	return func(t *Throttle) {
		t.Compaction.MaxMemUsedLimit = limit
	}
}

type Throttle struct {
	Compaction struct {
		// coarse grained throttle
		ActiveTasks         atomic.Int32
		MaxActiveTasksLimit int32

		MemUsed         atomic.Int64 // to be used in the future
		MaxMemUsedLimit int64        // to be used in the future
	}
}

func NewThrottle(opts ...ThrottleOption) *Throttle {
	t := &Throttle{}

	for _, opt := range opts {
		opt(t)
	}

	t.fillDefaults()

	return t
}

func (t *Throttle) fillDefaults() {
	if t.Compaction.MaxActiveTasksLimit <= 0 {
		cpus := runtime.NumCPU()
		if cpus <= 4 {
			t.Compaction.MaxActiveTasksLimit = 1
		} else {
			t.Compaction.MaxActiveTasksLimit = int32(cpus / 4)
		}
	}

	if t.Compaction.MaxMemUsedLimit <= 0 {
		memStats, err := mem.VirtualMemory()
		if err != nil {
			panic(err)
		}
		units := memStats.Total / ObjectMemUnit
		if units > 20 {
			units = 20
		} else if units < 1 {
			units = 1
		}
		t.Compaction.MaxMemUsedLimit = int64(units) * ObjectMemUnit
	}
}

// only used for fine grained throttle
func (t *Throttle) TryApplyCompactionTask() (ok bool) {
	for {
		cnt := t.Compaction.ActiveTasks.Load()
		if cnt >= t.Compaction.MaxActiveTasksLimit {
			return false
		}
		if t.Compaction.ActiveTasks.CompareAndSwap(cnt, cnt+1) {
			return true
		}
	}
}

func (t *Throttle) AcquireCompactionQuota() {
	t.Compaction.ActiveTasks.Add(1)
}

func (t *Throttle) ReleaseCompactionQuota() {
	t.Compaction.ActiveTasks.Add(-1)
}

// coarse grained throttle check
func (t *Throttle) CanCompact() bool {
	return t.Compaction.ActiveTasks.Load() < t.Compaction.MaxActiveTasksLimit
}

func (t *Throttle) String() string {
	return fmt.Sprintf(
		"Throttle: Compaction: ActiveTasks: %d, MemUsed: %d, MaxActiveTasksLimit: %d, MaxMemUsedLimit: %d",
		t.Compaction.ActiveTasks.Load(),
		t.Compaction.MemUsed.Load(),
		t.Compaction.MaxActiveTasksLimit,
		t.Compaction.MaxMemUsedLimit,
	)
}
