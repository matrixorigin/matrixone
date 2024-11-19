// Copyright 2023 Matrix Origin
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

package merge

import (
	"math"
	"os"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/process"
)

const estimateMemUsagePerRow = 30

func originalSize(objs []*catalog.ObjectEntry) int {
	size := 0
	for _, o := range objs {
		size += int(o.OriginSize())
	}
	return size
}

func estimateMergeSize(objs []*catalog.ObjectEntry) int {
	size := 0
	for _, o := range objs {
		size += int(o.Rows() * estimateMemUsagePerRow)
	}
	return size
}

func entryOutdated(entry *catalog.ObjectEntry, lifetime time.Duration) bool {
	createdAt := entry.CreatedAt.Physical()
	return time.Unix(0, createdAt).Add(lifetime).Before(time.Now())
}

type resourceController struct {
	proc *process.Process

	limit    int64
	using    int64
	reserved int64

	reservedMergeRows int64
	transferPageLimit int64

	cpuPercent float64
}

func (c *resourceController) setMemLimit(total uint64) {
	cgroup, err := memlimit.FromCgroup()
	if cgroup != 0 && cgroup < total {
		c.limit = int64(cgroup / 4 * 3)
	} else if total != 0 {
		c.limit = int64(total / 4 * 3)
	} else {
		panic("failed to get system total memory")
	}

	if c.limit > 200*common.Const1GBytes {
		c.transferPageLimit = c.limit / 25 * 2 // 8%
	} else if c.limit > 100*common.Const1GBytes {
		c.transferPageLimit = c.limit / 25 * 3 // 12%
	} else if c.limit > 40*common.Const1GBytes {
		c.transferPageLimit = c.limit / 25 * 4 // 16%
	} else {
		c.transferPageLimit = math.MaxInt64 // no limit
	}

	logutil.Info(
		"MergeExecutorMemoryInfo",
		common.AnyField("container-limit", common.HumanReadableBytes(int(cgroup))),
		common.AnyField("host-memory", common.HumanReadableBytes(int(total))),
		common.AnyField("merge-limit", common.HumanReadableBytes(int(c.limit))),
		common.AnyField("transfer-page-limit", common.HumanReadableBytes(int(c.transferPageLimit))),
		common.AnyField("error", err),
	)
}

func (c *resourceController) refresh() {
	if c.limit == 0 {
		c.setMemLimit(totalMem())
	}

	if c.proc == nil {
		c.proc, _ = process.NewProcess(int32(os.Getpid()))
	}
	if m, err := c.proc.MemoryInfo(); err == nil {
		c.using = int64(m.RSS)
	}

	if percents, err := cpu.Percent(0, false); err == nil {
		c.cpuPercent = percents[0]
	}
	c.reservedMergeRows = 0
	c.reserved = 0
}

func (c *resourceController) availableMem() int64 {
	avail := c.limit - c.using - c.reserved
	if avail < 0 {
		avail = 0
	}
	return avail
}

func (c *resourceController) printStats() {
	if c.reservedMergeRows == 0 && c.availableMem() > 512*common.Const1MBytes {
		return
	}

	logutil.Info("MergeExecutorMemoryStats",
		common.AnyField("merge-limit", common.HumanReadableBytes(int(c.limit))),
		common.AnyField("process-mem", common.HumanReadableBytes(int(c.using))),
		common.AnyField("reserving-rows", common.HumanReadableBytes(int(c.reservedMergeRows))),
		common.AnyField("reserving-mem", common.HumanReadableBytes(int(c.reserved))),
	)
}

func (c *resourceController) reserveResources(objs []*catalog.ObjectEntry) {
	for _, obj := range objs {
		c.reservedMergeRows += int64(obj.Rows())
		c.reserved += estimateMemUsagePerRow * int64(obj.Rows())
	}
}

func (c *resourceController) resourceAvailable(objs []*catalog.ObjectEntry) bool {
	if c.reservedMergeRows*36 /*28 * 1.3 */ > c.transferPageLimit/8 {
		return false
	}

	mem := c.availableMem()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	return estimateMergeSize(objs) <= int(2*mem/3)
}

func objectValid(objectEntry *catalog.ObjectEntry) bool {
	if objectEntry.IsAppendable() {
		return false
	}
	if !objectEntry.IsActive() {
		return false
	}
	if !objectEntry.IsCommitted() {
		return false
	}
	if objectEntry.IsCreatingOrAborted() {
		return false
	}
	return true
}
