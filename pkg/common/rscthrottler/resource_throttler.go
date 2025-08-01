// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"
)

const (
	refreshMaxInterval = time.Second * 10

	MemoryThrottlerLogHeader = "MemoryThrottler"
)

type RSCThrottler interface {
	Refresh()
	PrintUsage()
	Acquire(int64) (int64, bool)
	Release(int64) int64
	Available() int64
}

type memThrottler struct {
	limit    atomic.Int64
	rss      atomic.Int64
	reserved atomic.Int64
	proc     *process.Process

	cgroup atomic.Uint64
	total  atomic.Uint64

	actualTotalMemory atomic.Uint64

	name      string
	limitRate float64

	lastRefresh atomic.Int64

	options struct {
		acquirePolicy func(*memThrottler, int64) (int64, bool)

		constLimit int64

		// if false, the acquiring fails if
		// the wanted memory exceeds the available.
		allowOutOfMemoryAcquire bool
	}
}

func (m *memThrottler) String() string {
	return fmt.Sprintf(
		"{%s: limit=%s, total=%s, available=%s, cgroup=%s, rss=%s, pinned=%s, pinnedRate=%f, limitRate=%f, isConstLimit=%v}",
		m.name,
		common.HumanReadableBytes(int(m.limit.Load())),
		common.HumanReadableBytes(int(m.total.Load())),
		common.HumanReadableBytes(int(m.Available())),
		common.HumanReadableBytes(int(m.cgroup.Load())),
		common.HumanReadableBytes(int(m.rss.Load())),
		common.HumanReadableBytes(int(m.reserved.Load())),
		m.pinnedRate(),
		m.limitRate,
		m.options.constLimit > 0,
	)
}

func (m *memThrottler) pinnedRate() float64 {
	limit := float64(m.limit.Load())
	pinned := float64(m.reserved.Load())

	return pinned / limit
}

func (m *memThrottler) Refresh() {
	last := m.lastRefresh.Load()
	now := time.Now().UnixNano()

	if time.Duration(now-last) <= refreshMaxInterval {
		return
	}

	m.lastRefresh.Store(now)

	var (
		err    error
		cgroup uint64
		info   *process.MemoryInfoStat

		total uint64
	)

	defer func() {
		logutil.Info(
			fmt.Sprintf("%s-Refresh", MemoryThrottlerLogHeader),
			zap.String("detail", m.String()),
			zap.Error(err),
		)
	}()

	total = objectio.TotalMem()
	cgroup, err = memlimit.FromCgroup()

	if cgroup != 0 && cgroup < total {
		m.actualTotalMemory.Store(cgroup)
	} else if total != 0 {
		m.actualTotalMemory.Store(total)
	} else {
		panic("failed to get system total memory")
	}

	// if the const limit option is set, we should not change the limit.
	if m.options.constLimit < 0 {
		m.limit.Store(int64(float64(m.actualTotalMemory.Load()) * m.limitRate))
	}

	if m.proc == nil {
		m.proc, _ = process.NewProcess(int32(os.Getpid()))
	}

	if info, err = m.proc.MemoryInfo(); err == nil {
		m.rss.Store(int64(info.RSS))
	}

	m.cgroup.Store(cgroup)
	m.total.Store(total)
}

/*
		| -------- actual max memory  -----|
		|***RSS****|                       |
									limit
							|--------------|  case 1

    	        |--------------------------|  case 2
							limit

 |-----------------------------------------|  case 3
				   limit
*/

func (m *memThrottler) Available() int64 {
	//avail := m.limit.Load() - m.rss.Load() - m.reserved.Load()

	var (
		avail    int64
		limit    = m.limit.Load()
		rss      = m.rss.Load()
		reserved = m.reserved.Load()

		actualMaxMemory = int64(m.actualTotalMemory.Load())
	)

	if actualMaxMemory-rss >= limit {
		avail = limit - reserved
	} else {
		avail = actualMaxMemory - rss - reserved
	}

	if avail < 0 {
		avail = 0
	}

	return avail
}

func (m *memThrottler) PrintUsage() {
	logutil.Info(
		fmt.Sprintf("%s-Usage", MemoryThrottlerLogHeader),
		zap.String("detail", m.String()),
	)
}

// Acquire requires memory from memThrottler,
// it returns (new available, true) if success, or
// (available, false).
func (m *memThrottler) Acquire(ask int64) (int64, bool) {

	m.Refresh()

	var (
		left    int64
		granted bool
	)

	if m.options.acquirePolicy != nil {
		left, granted = m.options.acquirePolicy(m, ask)
	} else {
		left, granted = defaultAcquirePolicy(m, ask)
	}

	if !granted {
		logutil.Info(
			fmt.Sprintf("%s-Acquire", MemoryThrottlerLogHeader),
			zap.String("err", "out of available"),
			zap.String("ask", common.HumanReadableBytes(int(ask))),
			zap.String("detail", m.String()),
		)
	}

	return left, granted
}

func (m *memThrottler) Release(mem int64) int64 {
	m.reserved.Add(-int64(mem))
	if m.reserved.Load() < 0 {
		m.reserved.Store(0)
	}

	return m.Available()
}

// NewMemThrottler creates a new throttler for the `name`.
//
// you can use the limit rate to limit the max memory the `name` can
// pin. the max memory will be limitRate * totalFreeMemoryOfTheSystem.
//
// besides, you also can set the constLimit in the options, if done,
// the max memory will be the constLimit.
//
// another more flexible way to manage the max memory and allocation is the
// acquirePoly, you can use it in the options.
func NewMemThrottler(
	name string,
	limitRate float64,
	opts ...MemThrottlerOption,
) RSCThrottler {

	throttler := &memThrottler{
		limitRate: limitRate,
		name:      name,
	}

	throttler.fillDefaults()

	for _, opt := range opts {
		opt(throttler)
	}

	if throttler.options.constLimit > 0 {
		throttler.limit.Store(throttler.options.constLimit)
	}

	throttler.Refresh()

	return throttler
}

func (m *memThrottler) fillDefaults() {
	m.options.constLimit = -1
	m.options.allowOutOfMemoryAcquire = false
}

type MemThrottlerOption func(throttler *memThrottler)

func WithAllowOutOfLimitAcquire() MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.allowOutOfMemoryAcquire = true
	}
}

func WithConstLimit(constLimit int64) MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.constLimit = constLimit
	}
}

func WithAcquirePolicy(
	policy func(*memThrottler, int64) (int64, bool),
) MemThrottlerOption {
	return func(throttler *memThrottler) {
		throttler.options.acquirePolicy = policy
	}
}

func defaultAcquirePolicy(m *memThrottler, ask int64) (int64, bool) {
	for {
		avail := m.Available()
		if !m.options.allowOutOfMemoryAcquire && avail < ask {
			return 0, false
		}

		currReserved := m.reserved.Load()
		if currReserved < 0 {
			currReserved = 0
		}
		newReserved := currReserved + ask
		if m.reserved.CompareAndSwap(currReserved, newReserved) {
			return avail - ask, true
		}
	}
}

func AcquirePolicyForCNFlushS3(
	throttler *memThrottler,
	ask int64,
) (int64, bool) {

	rate := throttler.pinnedRate()

	if rate >= 0.80 {
		// almost exhausted the memory,
		// cannot pin only batch anymore.
		return 0, false
	}

	if rate >= 0.50 {
		// pinned more than half, only allow pinning small batch
		if ask >= mpool.MB*10 {
			return 0, false
		}
		return defaultAcquirePolicy(throttler, ask)
	}

	return defaultAcquirePolicy(throttler, ask)
}
