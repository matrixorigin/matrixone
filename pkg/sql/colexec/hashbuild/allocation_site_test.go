// Copyright 2026 Matrix Origin
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

package hashbuild_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
)

func TestHashBuildAllocationSiteRangesDoNotOverlap(t *testing.T) {
	ranges := []struct {
		name        string
		first, last mpool.AllocationSite
	}{
		{"hash-table", hashbuild.HashBuildAllocationSiteHashCell, hashbuild.HashBuildAllocationSiteHashIterator},
		{"shared-spill", spillutil.SpillAllocationSiteDecodedData, spillutil.SpillAllocationSiteSelectedGrouping},
		{"runtime-filter-dedup", hashbuild.HashBuildAllocationSiteUniqueKeyData, hashbuild.HashBuildAllocationSiteDedupDeleteOnlyGrouping},
		{"shared-spill-read", spillutil.SpillAllocationSiteReadBuffer, spillutil.SpillAllocationSiteReadBuffer},
		{"hashbuild-spill", hashbuild.HashBuildSpillAllocationSiteSelectedData, hashbuild.HashBuildSpillAllocationSiteCoalesceBuffer},
		{"hashjoin-matched", 80, 80},
		{"dedupjoin-state", 82, 88},
		{"rightdedupjoin-matched", 90, 90},
		{"loopjoin-state", 92, 93},
		{"product-result", 94, 97},
		{"expression", 98, 101},
		{"hashjoin-result", 102, 105},
		{"loopjoin-result", 106, 109},
		{"dedupjoin-result", 110, 113},
		{"rightdedupjoin-result", 114, 117},
		{"loopjoin-condition", 118, 121},
	}
	used := make(map[mpool.AllocationSite]string)
	for _, allocationRange := range ranges {
		for site := allocationRange.first; site <= allocationRange.last; site++ {
			if previous, ok := used[site]; ok {
				t.Fatalf(
					"allocation site %d is shared by %s and %s",
					site,
					previous,
					allocationRange.name,
				)
			}
			used[site] = allocationRange.name
		}
	}
}
