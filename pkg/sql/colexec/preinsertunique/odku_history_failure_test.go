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

package preinsertunique

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestODKUFixedHistoryAppendFailureCleanup(t *testing.T) {
	a, proc, b, account := newODKUKeyFixture(t, 1024)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	// Admit every earlier publication step first. The next unique action must
	// fail specifically when its group-aligned key history needs to grow.
	require.NoError(t, a.ctr.acceptedKeys[0].rows.PreExtend(1025, proc.Mp()))
	require.NoError(t, a.ctr.acceptedKeys[0].index.(*hashmap.IntHashMap).PreAlloc(1025))
	next := batch.NewWithSize(4)
	for p := range next.Vecs {
		next.Vecs[p] = vector.NewVec(types.T_int64.ToType())
		value := int64(2000)
		if p == 2 {
			value = 1025
		}
		require.NoError(t, vector.AppendFixed(next.Vecs[p], value, p == 3, proc.Mp()))
	}
	next.SetRowCount(1)
	defer next.Clean(proc.Mp())
	used := account.Snapshot().Used
	filler, err := proc.Mp().AllocAccounted(int(odkuKeyTestBudget-used-64), account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
	require.NoError(t, err)
	defer proc.Mp().Free(filler)
	err = seedODKUKeyRows(proc, a, next.Vecs[1])
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, uint64(1025), a.ctr.acceptedKeys[0].index.GroupCount())
	require.Equal(t, 1024, a.ctr.acceptedKeys[0].fixedKeys.Length())
	require.Equal(t, 1024, a.ctr.acceptedKeys[0].rows.Length())
}
