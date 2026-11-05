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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const odkuKeyTestBudget = 8 << 20

// seedODKUKeyRows supplies already accepted owners for narrow codec/history
// tests; action target selection is exercised through Call in odku_actions_test.
func seedODKUKeyRows(proc *process.Process, a *PreInsertUnique, v *vector.Vector) error {
	key := &a.ctr.acceptedKeys[0]
	key.key[0] = v
	if key.index == nil {
		if err := a.initODKUKey(proc, 0); err != nil {
			return err
		}
	}
	for row := range v.Length() {
		_, found, err := a.lookupODKUKeyIndex(proc, key, row)
		if err != nil {
			return err
		}
		if !found && !v.IsNull(uint64(row)) {
			if err := a.claimODKUKey(proc, 0, row, int64(key.rows.Length())); err != nil {
				return err
			}
		}
	}
	return nil
}

func newODKUKeyFixture(t *testing.T, rows int) (*PreInsertUnique, *process.Process, *batch.Batch, *mpool.AllocationAccount) {
	t.Helper()
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	a := &PreInsertUnique{}
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(odkuKeyTestBudget)
	require.NoError(t, err)
	require.NoError(t, a.SetAllocationAccount(account))
	a.ctr.acceptedKeys = make([]odkuKeyIndex, 1)
	b := batch.NewWithSize(4)
	for p := range b.Vecs {
		b.Vecs[p] = vector.NewVec(types.T_int64.ToType())
	}
	t.Cleanup(func() {
		a.Reset(proc, true, nil)
		a.Free(proc, true, nil)
		b.Clean(mp)
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, mp.CurrNB())
		require.NoError(t, a.ClearAllocationAccount(account))
	})
	for row := range rows {
		for p := range b.Vecs {
			require.NoError(t, vector.AppendFixed(b.Vecs[p], int64(row+1), p == 3, mp))
		}
	}
	b.SetRowCount(rows)
	return a, proc, b, account
}

func TestODKUKeyMigrationAdmissionCleanup(t *testing.T) {
	for _, rows := range []int{1, 1024} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			a, proc, b, account := newODKUKeyFixture(t, rows)
			err := seedODKUKeyRows(proc, a, b.Vecs[1])
			require.NoError(t, err)
			require.IsType(t, &hashmap.IntHashMap{}, a.ctr.acceptedKeys[0].index)
			used := account.Snapshot().Used
			filler, err := proc.Mp().AllocAccounted(int(odkuKeyTestBudget-used-64), account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
			require.NoError(t, err)
			defer proc.Mp().Free(filler)
			b.Vecs[1].GetGrouping().Add(0)
			err = a.ensureODKUKeyMap(proc, 0)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			if rows == 1 {
				require.Nil(t, a.ctr.acceptedKeys[0].index, "new map admission failed after old map release")
			} else {
				require.IsType(t, &hashmap.StrHashMap{}, a.ctr.acceptedKeys[0].index, "failure is inside the rebuild")
			}
			require.NotNil(t, a.ctr.acceptedKeys[0].fixedKeys)
			require.Equal(t, rows, a.ctr.acceptedKeys[0].rows.Length())
			require.ErrorIs(t, a.ClearAllocationAccount(account), mpool.ErrAllocationAccountInvariant)
		})
	}
}

func TestODKUInitialGroupingAndPhysicalTypeGuard(t *testing.T) {
	t.Run("initial grouping retains generic map", func(t *testing.T) {
		a, proc, b, _ := newODKUKeyFixture(t, 2)
		b.Vecs[1].GetGrouping().Add(1)
		a.ctr.acceptedKeys[0].key[0] = b.Vecs[1]
		require.NoError(t, a.initODKUKey(proc, 0))
		require.IsType(t, &hashmap.StrHashMap{}, a.ctr.acceptedKeys[0].index)
		require.Nil(t, a.ctr.acceptedKeys[0].fixedKeys)
	})

}
