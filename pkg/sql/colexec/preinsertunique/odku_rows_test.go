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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newODKUActionRowsFixture(t *testing.T, mutable ...int32) (*PreInsertUnique, *process.Process, *batch.Batch, *batch.Batch) {
	return newODKUActionRowsFixtureWithOrigin(t, types.T_int64.ToType(), mutable...)
}

func newODKUActionRowsFixtureWithOrigin(t *testing.T, originType types.Type, mutable ...int32) (*PreInsertUnique, *process.Process, *batch.Batch, *batch.Batch) {
	t.Helper()
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	a := &PreInsertUnique{}
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(odkuKeyTestBudget)
	require.NoError(t, err)
	require.NoError(t, a.SetAllocationAccount(account))
	a.ctr.acceptedKeys = make([]odkuKeyIndex, 2)
	image := batch.NewWithSize(2)
	image.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	image.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	keys := batch.NewWithSize(2)
	keys.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	keys.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	origins := batch.NewWithSize(2)
	origins.Vecs[0] = vector.NewVec(originType)
	origins.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	t.Cleanup(func() {
		a.Reset(proc, true, nil)
		a.Free(proc, true, nil)
		image.Clean(mp)
		keys.Clean(mp)
		origins.Clean(mp)
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, mp.CurrNB())
		require.NoError(t, a.ClearAllocationAccount(account))
	})
	require.NoError(t, a.initODKUActionRows(proc, image, keys, origins, 1, mutable))
	return a, proc, image, keys
}

func setODKUActionInput(t *testing.T, proc *process.Process, image, keys *batch.Batch, number, key int64, value, unique string, nullKey bool) {
	t.Helper()
	image.CleanOnlyData()
	keys.CleanOnlyData()
	require.NoError(t, vector.AppendFixed(image.Vecs[0], number, false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(image.Vecs[1], []byte(value), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(keys.Vecs[0], key, false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(keys.Vecs[1], []byte(unique), nullKey, proc.Mp()))
	image.SetRowCount(1)
	keys.SetRowCount(1)
}

func TestODKUActionRowsPublishAndConflict(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
	publish := func(owner int64) {
		t.Helper()
		slot, _, err := a.publishODKUAction(proc, owner, image, keys, nil)
		require.NoError(t, err)
		require.Equal(t, -1, slot)
	}
	setODKUActionInput(t, proc, image, keys, 1, 10, strings.Repeat("first", 12), "alpha", false)
	publish(0)
	setODKUActionInput(t, proc, image, keys, 2, 20, strings.Repeat("second", 12), "beta", false)
	publish(1)
	rows := a.ctr.actionRows
	require.Equal(t, 2, rows.images.RowCount())
	setODKUActionInput(t, proc, image, keys, 2, 30, strings.Repeat("second", 12), "gamma", false)
	publish(0)

	// The second constraint conflicts after a proposed first-key change.
	// Neither the old image nor either key owner may be published partially.
	setODKUActionInput(t, proc, image, keys, 9, 40, "new", "beta", false)
	slot, other, err := a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, 1, slot)
	require.Equal(t, int64(1), other)
	require.True(t, a.ctr.failed)
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](rows.images.Vecs[0], 0))
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](rows.keys.Vecs[0], 0))
	require.Equal(t, "gamma", string(rows.keys.Vecs[1].GetBytesAt(0)))
	_, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
	require.ErrorContains(t, err, "invalid ODKU action owner")
}

func TestODKUActionRowsUnchangedKeysRetainStorageAndClaims(t *testing.T) {
	for _, null := range []bool{false, true} {
		t.Run(fmt.Sprintf("null=%v", null), func(t *testing.T) {
			a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
			value := strings.Repeat("value", 8192)
			setODKUActionInput(t, proc, image, keys, 1, 10, value, strings.Repeat("key", 8192), null)
			slot, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
			require.NoError(t, err)
			require.Equal(t, -1, slot)
			rows := a.ctr.actionRows
			area := len(rows.keys.Vecs[1].GetArea())
			imageArea := len(rows.images.Vecs[1].GetArea())
			for i := range 8 {
				vector.SetFixedAtNoTypeCheck(image.Vecs[0], 0, int64(i))
				slot, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
				require.NoError(t, err)
				require.Equal(t, -1, slot)
			}
			require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](rows.images.Vecs[0], 0))
			require.Equal(t, area, len(rows.keys.Vecs[1].GetArea()), "unchanged keys accumulate no dead copies")
			require.Equal(t, imageArea, len(rows.images.Vecs[1].GetArea()), "unchanged payload accumulates no dead copies")
			for i := range keys.Vecs {
				owner, found, e := a.lookupODKUKeyVector(proc, i, keys.Vecs[i], 0)
				require.NoError(t, e)
				require.Equal(t, i == 0 || !null, found)
				require.Zero(t, owner)
			}
			setODKUActionInput(t, proc, image, keys, 99, 20, "next inserted payload", "next unique", false)
			slot, _, err = a.publishODKUAction(proc, 1, image, keys, nil)
			require.NoError(t, err)
			require.Equal(t, -1, slot)
			require.Equal(t, "next inserted payload", string(rows.images.Vecs[1].GetBytesAt(1)), "new owners refill all image columns after update scratch reuse")
		})
	}
}

func TestODKUActionRowsGroupingUsesGenericKeyEncoder(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
	setODKUActionInput(t, proc, image, keys, 1, 10, "initial", "unique", false)
	slot, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	keys.Vecs[0].GetGrouping().Add(0)
	slot, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	require.Nil(t, a.ctr.acceptedKeys[0].fixedKeys, "grouping takes the generic transition and encoder path")
	require.True(t, a.ctr.actionRows.keys.Vecs[0].GetGrouping().Contains(0))
}

func TestODKUActionRowsValidateUnchangedKeyLayout(t *testing.T) {
	for _, invalid := range []string{"nil later key", "short later key", "wrong later type"} {
		t.Run(invalid, func(t *testing.T) {
			a, proc, image, keys := newODKUActionRowsFixture(t, 0)
			setODKUActionInput(t, proc, image, keys, 1, 10, "initial", "unique", false)
			slot, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
			require.NoError(t, err)
			require.Equal(t, -1, slot)
			original := keys.Vecs[1]
			defer func() { keys.Vecs[1] = original }()
			switch invalid {
			case "nil later key":
				keys.Vecs[1] = nil
			case "short later key":
				original.CleanOnlyData()
			case "wrong later type":
				keys.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				defer keys.Vecs[1].Free(proc.Mp())
				require.NoError(t, vector.AppendFixed(keys.Vecs[1], int64(1), false, proc.Mp()))
			}
			_, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
			require.Error(t, err)
			require.True(t, a.ctr.failed)
			require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](a.ctr.actionRows.keys.Vecs[0], 0))
		})
	}
}

func TestODKUActionRowsKeepOriginalIdentityWhenKeyIsReused(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
	origin := batch.NewWithSize(2)
	origin.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	origin.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	defer origin.Clean(proc.Mp())
	rowID := types.Rowid{17, 42}
	require.NoError(t, vector.AppendFixed(origin.Vecs[0], int64(2), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(origin.Vecs[1], rowID, false, proc.Mp()))
	origin.SetRowCount(1)

	setODKUActionInput(t, proc, image, keys, 1, 2, "old", "alpha", false)
	slot, _, err := a.publishODKUAction(proc, 0, image, keys, origin)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	setODKUActionInput(t, proc, image, keys, 2, 4, "moved", "alpha", false)
	slot, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	setODKUActionInput(t, proc, image, keys, 3, 2, "new", "beta", false)
	slot, _, err = a.publishODKUAction(proc, 1, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)

	owner, found, err := a.lookupODKUOrigin(proc, origin.Vecs[1], 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, owner, "snapshot RowID still identifies the original row")
	owner, found, err = a.lookupODKUKeyVector(proc, 0, keys.Vecs[0], 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), owner, "current key now belongs to the new row")
	rows := a.ctr.actionRows
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](rows.origins.Vecs[0], 0))
	require.Equal(t, rowID, vector.GetFixedAtNoTypeCheck[types.Rowid](rows.origins.Vecs[1], 0))
	require.True(t, rows.origins.Vecs[0].IsNull(1))
	require.True(t, rows.origins.Vecs[1].IsNull(1))

	// Bypassing the owner's RowID lookup is an integrity error at native claim.
	// The failed attempt is terminal; reset owns any partially changed state.
	setODKUActionInput(t, proc, image, keys, 4, 8, "duplicate", "delta", false)
	_, _, err = a.publishODKUAction(proc, 2, image, keys, origin)
	require.Error(t, err)
	require.True(t, a.ctr.failed)
	_, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
	require.ErrorContains(t, err, "invalid ODKU action owner", "failed state cannot be used again before reset")
}

func TestODKUActionRowsRetainVarlenOriginalPrimaryKey(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixtureWithOrigin(t, types.T_varchar.ToType(), 0)
	origin := batch.NewWithSize(2)
	origin.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	origin.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	defer origin.Clean(proc.Mp())
	originalKey := strings.Repeat("old-key-", 32)
	require.NoError(t, vector.AppendBytes(origin.Vecs[0], []byte(originalKey), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(origin.Vecs[1], types.Rowid{37}, false, proc.Mp()))
	origin.SetRowCount(1)
	setODKUActionInput(t, proc, image, keys, 1, 2, "unchanged", "alpha", false)
	slot, _, err := a.publishODKUAction(proc, 0, image, keys, origin)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	owner, found, err := a.lookupODKUOrigin(proc, origin.Vecs[1], 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, owner)
	require.Equal(t, originalKey, string(a.ctr.actionRows.origins.Vecs[0].GetBytesAt(0)))
}

func TestODKUActionRowsOriginalRowIDSeparatesSignedZeroPrimaryKeys(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixtureWithOrigin(t, types.T_float64.ToType(), 0, 1)
	origin := batch.NewWithSize(2)
	t.Cleanup(func() { origin.Clean(proc.Mp()) })
	origin.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	origin.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
	values := []float64{math.Copysign(0, -1), 0}
	for i, value := range values {
		origin.CleanOnlyData()
		require.NoError(t, vector.AppendFixed(origin.Vecs[0], value, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(origin.Vecs[1], types.Rowid{byte(i + 1)}, false, proc.Mp()))
		origin.SetRowCount(1)
		unique := []string{"left", "right"}[i]
		setODKUActionInput(t, proc, image, keys, int64(i), int64(i+1), unique, unique, false)
		slot, _, err := a.publishODKUAction(proc, int64(i), image, keys, origin)
		require.NoError(t, err)
		require.Equal(t, -1, slot)
	}
	for i, value := range values {
		owner, found, err := a.lookupODKUOrigin(proc, a.ctr.actionRows.origins.Vecs[1], i)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, int64(i), owner)
		require.Equal(t, math.Float64bits(value), math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](a.ctr.actionRows.origins.Vecs[0], i)))
	}
}

func TestODKUActionRowsCompactVarlenAndRetainNullKey(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
	payload := strings.Repeat("x", 1024)
	setODKUActionInput(t, proc, image, keys, 1, 1, payload, "", true)
	slot, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	setODKUActionInput(t, proc, image, keys, 2, 2, payload, "", true)
	slot, _, err = a.publishODKUAction(proc, 1, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	rows := a.ctr.actionRows
	// Exercise the internal row-storage compaction boundary. Mutable-key Call
	// paths retain distinct groups for locks until Reset; they do not compact
	// the ownership map to a bound based on the current row count.
	for i := 0; i < 250; i++ {
		unique := fmt.Sprintf("%04d-%s", i, payload)
		setODKUActionInput(t, proc, image, keys, int64(i), int64(i+3), unique, unique, false)
		slot, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
		require.NoError(t, err)
		require.Equal(t, -1, slot)
		require.LessOrEqual(t, len(rows.images.Vecs[1].GetArea()), odkuDeadAreaSlack+3*len(unique))
		require.LessOrEqual(t, len(rows.keys.Vecs[1].GetArea()), odkuDeadAreaSlack+2*len(unique))
	}
	require.True(t, rows.keys.Vecs[1].IsNull(1), "another NULL key stays unowned")
	require.Equal(t, int64(249), vector.GetFixedAtNoTypeCheck[int64](rows.images.Vecs[0], 0))
	require.Equal(t, payload, string(rows.images.Vecs[1].GetBytesAt(1)), "compaction preserves untouched rows")
	for i := range keys.Vecs {
		owner, found, e := a.lookupODKUKeyVector(proc, i, keys.Vecs[i], 0)
		require.NoError(t, e)
		require.True(t, found)
		require.Zero(t, owner)
	}
}

func TestODKUActionRowsCanceledBeforePublish(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
	setODKUActionInput(t, proc, image, keys, 1, 1, "value", "key", false)
	original := proc.Ctx
	canceled, cancel := context.WithCancel(original)
	cancel()
	proc.Ctx = canceled
	_, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
	proc.Ctx = original
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, a.ctr.failed)
	require.Zero(t, a.ctr.actionRows.images.RowCount())
	require.Zero(t, a.ctr.actionRows.keys.RowCount())
}

func TestODKUActionRowsDoNotCopyUnchangedColumn(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 0)
	payload := strings.Repeat("unchanged", 8192)
	setODKUActionInput(t, proc, image, keys, 1, 1, payload, "unique", false)
	slot, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	rows := a.ctr.actionRows
	areaBytes := len(rows.images.Vecs[1].GetArea())
	for i := 0; i < 20; i++ {
		setODKUActionInput(t, proc, image, keys, int64(i), int64(i+2), "different but not assigned", "unique", false)
		slot, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
		require.NoError(t, err)
		require.Equal(t, -1, slot)
	}
	require.Equal(t, payload, string(rows.images.Vecs[1].GetBytesAt(0)))
	require.Equal(t, areaBytes, len(rows.images.Vecs[1].GetArea()))
	require.Equal(t, int64(19), vector.GetFixedAtNoTypeCheck[int64](rows.images.Vecs[0], 0))
	require.Nil(t, rows.originKey.index, "inserted rows need no original-identity index")
}

func TestODKUActionRowsImageAdmissionFailureAfterKeyMove(t *testing.T) {
	a, proc, image, keys := newODKUActionRowsFixture(t, 0, 1)
	value := strings.Repeat("p", 32<<10)
	setODKUActionInput(t, proc, image, keys, 1, 1, value, "unique", false)
	slot, _, err := a.publishODKUAction(proc, 0, image, keys, nil)
	require.NoError(t, err)
	require.Equal(t, -1, slot)
	rows := a.ctr.actionRows

	// Fill only unused area capacity so the next copy has to allocate. The
	// staging vectors and map have already admitted this payload/key shape.
	for i := 0; i < 8 && len(rows.images.Vecs[1].GetArea())+len(value) <= cap(rows.images.Vecs[1].GetArea()); i++ {
		if value[0] == 'p' {
			value = strings.Repeat("q", len(value))
		} else {
			value = strings.Repeat("p", len(value))
		}
		setODKUActionInput(t, proc, image, keys, 1, 1, value, "unique", false)
		slot, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
		require.NoError(t, err)
		require.Equal(t, -1, slot)
	}
	require.Greater(t, len(rows.images.Vecs[1].GetArea())+len(value), cap(rows.images.Vecs[1].GetArea()))
	value = strings.Repeat("r", len(value))
	setODKUActionInput(t, proc, image, keys, 2, 2, value, "unique", false)
	used := a.allocationAccount.Snapshot().Used
	filler, err := proc.Mp().AllocAccounted(int(odkuKeyTestBudget-used-64), a.allocationAccount, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
	require.NoError(t, err)
	defer proc.Mp().Free(filler)
	_, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.True(t, a.ctr.failed)
	owner, found, err := a.lookupODKUKeyVector(proc, 0, keys.Vecs[0], 0)
	require.NoError(t, err)
	require.True(t, found, "the key moved before image allocation failed")
	require.Zero(t, owner)
	_, _, err = a.publishODKUAction(proc, 0, image, keys, nil)
	require.ErrorContains(t, err, "invalid ODKU action owner")
}
