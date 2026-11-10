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

package system

import (
	"github.com/elastic/gosigar"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// memoryStatsFromPages converts one kernel snapshot using that kernel's page
// size. In particular, Darwin ARM64 uses 16 KiB pages, not the 4 KiB assumed by
// the pinned gosigar implementation. Reject invalid counters rather than
// overflowing or underflowing a value used for hard allocation admission.
func memoryStatsFromPages(total, freePages, inactivePages, pageSize uint64) (gosigar.Mem, error) {
	if pageSize == 0 || freePages > total/pageSize || inactivePages > total/pageSize-freePages {
		return gosigar.Mem{}, moerr.NewInternalErrorNoCtx("invalid host memory page counters")
	}
	free := freePages * pageSize
	available := (freePages + inactivePages) * pageSize
	return gosigar.Mem{
		Total: total, Free: free, Used: total - free,
		ActualFree: available, ActualUsed: total - available,
	}, nil
}
