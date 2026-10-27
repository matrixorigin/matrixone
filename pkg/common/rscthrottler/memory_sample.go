// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"errors"
	"math"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/shirou/gopsutil/v3/process"
)

var errNoMemoryController = moerr.NewInternalErrorNoCtx("cgroup memory controller is not available")

type memorySample struct {
	rss            int64
	total          int64
	limit          int64
	current        int64
	inactiveFile   int64
	rssErr         error
	cgroupObserved bool
	cgroupErr      error
	err            error
}

func memoryInt(n uint64) int64 {
	if n > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(n)
}

func readProcessMemory() memorySample {
	s := memorySample{rss: -1, total: memoryInt(objectio.TotalMem())}
	p, err := process.NewProcess(int32(os.Getpid()))
	if err == nil {
		var info *process.MemoryInfoStat
		info, err = p.MemoryInfo()
		if err == nil {
			s.rss = memoryInt(info.RSS)
		}
	}
	s.rssErr = err
	s.err = err
	s.limit, s.current, s.inactiveFile, err = readCgroupMemory()
	s.cgroupObserved = err == nil
	s.cgroupErr = err
	s.err = errors.Join(s.err, err)
	return s
}
