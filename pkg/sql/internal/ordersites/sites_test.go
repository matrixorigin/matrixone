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

package ordersites

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/stretchr/testify/require"
)

func TestOrderAllocationSiteRangesDoNotOverlap(t *testing.T) {
	type siteRange struct {
		name        string
		first, last mpool.AllocationSite
	}
	ranges := []siteRange{
		{"spill", spillutil.SpillAllocationSiteDecodedData, spillutil.SpillAllocationSiteReadBuffer},
		{"merge-order", MergeOrderRetainedData, MergeOrderSpillWriteBuffer},
		{"order", OrderRetainedData, OrderAppendCheckpoints},
	}
	for i, current := range ranges {
		require.GreaterOrEqual(t, current.first, mpool.AllocationSiteMin, current.name)
		require.LessOrEqual(t, current.first, current.last, current.name)
		require.LessOrEqual(t, current.last, mpool.AllocationSiteMax, current.name)
		if i != 0 {
			require.Less(t, ranges[i-1].last, current.first,
				"%s overlaps %s", ranges[i-1].name, current.name)
		}
	}
}
