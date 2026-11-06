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
	"math/rand"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestODKUPhysicalKeyMapMatchesGeneric(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_bool.ToType(), types.T_uint8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType(),
		types.T_enum.ToType(), types.T_date.ToType(), types.T_decimal64.ToType(), types.T_float32.ToType(), types.T_float64.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			run := func(generic bool) []int64 {
				mp := mpool.MustNewZero()
				proc := testutil.NewProcessWithMPool(t, "", mp)
				a := &PreInsertUnique{}
				registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
				require.NoError(t, err)
				account, err := registry.Open(8 << 20)
				require.NoError(t, err)
				require.NoError(t, a.SetAllocationAccount(account))
				a.ctr.acceptedKeys = make([]odkuKeyIndex, 1)
				defer func() {
					a.Reset(proc, false, nil)
					a.Free(proc, false, nil)
					proc.Free()
					require.Zero(t, account.Snapshot().Used)
					require.Zero(t, mp.CurrNB())
				}()
				var targets []int64
				for batchNo := range 2 {
					func() {
						keys := vector.NewVec(typ)
						defer keys.Free(mp)
						for i, key := range []uint64{0, 1, 0, 2, 1, 3} {
							null := i == 3
							var err error
							switch typ.Oid {
							case types.T_bool:
								err = vector.AppendFixed(keys, key != 0, null, mp)
							case types.T_int16:
								err = vector.AppendFixed(keys, int16(key), null, mp)
							case types.T_int32:
								err = vector.AppendFixed(keys, int32(key), null, mp)
							case types.T_int64:
								err = vector.AppendFixed(keys, int64(key), null, mp)
							case types.T_enum:
								err = vector.AppendFixed(keys, types.Enum(key), null, mp)
							case types.T_date:
								err = vector.AppendFixed(keys, types.Date(key), null, mp)
							case types.T_decimal64:
								err = vector.AppendFixed(keys, types.Decimal64(key), null, mp)
							case types.T_float32:
								values := []float32{0, math.Float32frombits(1 << 31), 0, 1, float32(math.NaN()), float32(math.NaN())}
								err = vector.AppendFixed(keys, values[i], null, mp)
							case types.T_float64:
								values := []float64{0, math.Copysign(0, -1), 0, 1, math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000001)}
								err = vector.AppendFixed(keys, values[i], null, mp)
							default:
								switch typ.TypeSize() {
								case 1:
									err = vector.AppendFixed(keys, uint8(key), null, mp)
								case 2:
									err = vector.AppendFixed(keys, uint16(key), null, mp)
								case 4:
									err = vector.AppendFixed(keys, uint32(key), null, mp)
								default:
									err = vector.AppendFixed(keys, key, null, mp)
								}
							}
							require.NoError(t, err)
						}

						if generic && batchNo == 0 {
							a.ctr.acceptedKeys[0].key[0] = keys
							require.NoError(t, a.initODKUKey(proc, 0))
							hashmap.IteratorClearOwner(a.ctr.acceptedKeys[0].iterator)
							a.ctr.acceptedKeys[0].index.Free()
							if a.ctr.acceptedKeys[0].fixedKeys != nil {
								a.ctr.acceptedKeys[0].fixedKeys.Free(mp)
							}
							a.ctr.acceptedKeys[0].fixedKeys = nil
							require.NoError(t, a.newODKUKeyStrMap(proc, &a.ctr.acceptedKeys[0]))
						}
						require.NoError(t, seedODKUKeyRows(proc, a, keys))
						for row := range keys.Length() {
							owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], row)
							require.NoError(t, err)
							if found {
								targets = append(targets, owner+1)
							} else {
								targets = append(targets, 0)
							}
						}

						if !generic && typ.Oid != types.T_float32 && typ.Oid != types.T_float64 {
							require.IsType(t, &hashmap.IntHashMap{}, a.ctr.acceptedKeys[0].index)
						}
					}()
				}
				return targets
			}
			require.Equal(t, run(true), run(false))
		})
	}
}

func TestODKUFixedKeyMigrationAndCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	a := &PreInsertUnique{}
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(8 << 20)
	require.NoError(t, err)
	require.NoError(t, a.SetAllocationAccount(account))
	a.ctr.acceptedKeys = make([]odkuKeyIndex, 1)
	b := batch.NewWithSize(4)
	for p := range b.Vecs {
		b.Vecs[p] = vector.NewVec(types.T_int64.ToType())
	}
	t.Cleanup(func() {
		a.Reset(proc, false, nil)
		a.Free(proc, false, nil)
		b.Clean(mp)
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, mp.CurrNB())
	})
	for row := range 300 {
		for p := range b.Vecs {
			require.NoError(t, vector.AppendFixed(b.Vecs[p], int64(row+1), p == 3, mp))
		}
	}
	b.SetRowCount(300)
	err = seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	require.IsType(t, &hashmap.IntHashMap{}, a.ctr.acceptedKeys[0].index)
	require.Equal(t, 300, a.ctr.acceptedKeys[0].fixedKeys.Length())
	// GROUPING appears in a later vector generation. Before any current key is
	// consulted, cancellation must leave the old map and its history intact.
	b.Vecs[1].GetGrouping().Add(299)
	oldCtx := proc.Ctx
	ctx, cancel := context.WithCancel(oldCtx)
	cancel()
	proc.Ctx = ctx
	require.ErrorIs(t, a.ensureODKUKeyMap(proc, 0), context.Canceled)
	require.IsType(t, &hashmap.IntHashMap{}, a.ctr.acceptedKeys[0].index)
	proc.Ctx = oldCtx
	require.NoError(t, a.ensureODKUKeyMap(proc, 0))
	require.IsType(t, &hashmap.StrHashMap{}, a.ctr.acceptedKeys[0].index)
	require.Nil(t, a.ctr.acceptedKeys[0].fixedKeys)
	groups, _, err := a.ctr.acceptedKeys[0].iterator.Find(0, hashmap.UnitLimit, []*vector.Vector{b.Vecs[1]})
	require.NoError(t, err)
	for i, g := range groups {
		require.Equal(t, uint64(i+1), g)
		require.Equal(t, int64(i), vector.GetFixedAtNoTypeCheck[int64](a.ctr.acceptedKeys[0].rows, i))
	}
}

func TestODKUKeyOwnerReleaseAndReuse(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 3)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	key := &a.ctr.acceptedKeys[0]
	assertOwner := func(row int, owner int64, found bool) {
		t.Helper()
		got, ok, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], row)
		require.NoError(t, err)
		require.Equal(t, found, ok)
		if found {
			require.Equal(t, owner, got)
		}
	}
	assertOwner(0, 0, true)
	assertOwner(1, 1, true)
	require.ErrorContains(t, a.releaseODKUKey(proc, 0, 0, 1), "owner mismatch")
	assertOwner(0, 0, true)
	require.NoError(t, a.releaseODKUKey(proc, 0, 0, 0))
	// Promoting the physical encoder must keep the vacated group unowned.
	b.Vecs[1].GetGrouping().Add(2)
	assertOwner(0, 0, false)
	require.IsType(t, &hashmap.StrHashMap{}, key.index)
	require.Error(t, a.releaseODKUKey(proc, 0, 0, 0))
	require.NoError(t, a.claimODKUKey(proc, 0, 0, 2))
	assertOwner(0, 2, true)
	assertOwner(1, 1, true)
	require.Error(t, a.claimODKUKey(proc, 0, 0, 0))

	// A reused key retains one hash group, even across repeated moves.
	for i := 0; i < 100; i++ {
		require.NoError(t, a.releaseODKUKey(proc, 0, 0, 2))
		require.NoError(t, a.claimODKUKey(proc, 0, 0, 2))
	}
	require.Equal(t, uint64(3), key.index.GroupCount())
	require.Equal(t, 3, key.rows.Length())
	require.Zero(t, key.dead)
	assertOwner(0, 2, true)
}

func TestODKUKeyTransitionAcrossConstraints(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 2)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	keys := [2]*vector.Vector{vector.NewVec(types.T_int64.ToType()), vector.NewVec(types.T_int64.ToType())}
	t.Cleanup(func() {
		for _, v := range keys {
			v.Free(proc.Mp())
		}
	})
	for slot, values := range [][]int64{{1, 2, 3, 4, 0}, {10, 20, 30, 40, 0}} {
		for row, value := range values {
			require.NoError(t, vector.AppendFixed(keys[slot], value, row == len(values)-1, proc.Mp()))
		}
	}
	a.ctr.acceptedKeys = append(a.ctr.acceptedKeys, odkuKeyIndex{})
	a.ctr.acceptedKeys[1].key[0] = keys[1]
	require.NoError(t, a.initODKUKey(proc, 1))
	require.NoError(t, a.claimODKUKey(proc, 1, 0, 0))
	require.NoError(t, a.claimODKUKey(proc, 1, 1, 1))
	lookup := func(slot, row int, expected int64, want bool) {
		t.Helper()
		owner, found, e := a.lookupODKUKeyVector(proc, slot, keys[slot], row)
		require.NoError(t, e)
		require.Equal(t, want, found)
		if want {
			require.Equal(t, expected, owner)
		}
	}
	changes := func(old0, old1, next0, next1 int) []odkuKeyChange {
		return []odkuKeyChange{
			{old: keys[0], oldRow: old0, next: keys[0], nextRow: next0},
			{old: keys[1], oldRow: old1, next: keys[1], nextRow: next1},
		}
	}
	conflict, _, err := a.transitionODKUKeys(proc, 0, changes(0, 0, 2, 2))
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(0, 0, 0, false)
	lookup(1, 0, 0, false)
	lookup(0, 2, 0, true)
	lookup(1, 2, 0, true)

	// Another owner can claim both vacated keys; a NULL unique key owns none.
	conflict, _, err = a.transitionODKUKeys(proc, 1, changes(1, 1, 0, 0))
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(0, 0, 1, true)
	lookup(1, 0, 1, true)
	conflict, _, err = a.transitionODKUKeys(proc, 2, []odkuKeyChange{{next: keys[0], nextRow: 1}, {next: keys[1], nextRow: 4}})
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(0, 1, 2, true)
	lookup(1, 4, 0, false)

	conflict, _, err = a.transitionODKUKeys(proc, 0, changes(2, 2, 2, 4))
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(0, 2, 0, true)
	lookup(1, 2, 0, false)
	conflict, _, err = a.transitionODKUKeys(proc, 0, changes(2, 4, 2, 2))
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(1, 2, 0, true)

	// Reuse the same transition scratch after a retained next-key result.
	// NULL must clear that result and release the previously owned key.
	reused := changes(2, 2, 2, 2)
	conflict, _, err = a.transitionODKUKeys(proc, 0, reused)
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	reused[1].nextRow = 4
	conflict, _, err = a.transitionODKUKeys(proc, 0, reused)
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(1, 2, 0, false)
	reused[1].oldRow, reused[1].nextRow = 4, 2
	conflict, _, err = a.transitionODKUKeys(proc, 0, reused)
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	lookup(1, 2, 0, true)

	// The second constraint conflicts: the first key must not move early,
	// and a failed action cannot be followed by another transition.
	conflict, other, err := a.transitionODKUKeys(proc, 0, changes(2, 2, 3, 0))
	require.NoError(t, err)
	require.Equal(t, 1, conflict)
	require.Equal(t, int64(1), other)
	require.True(t, a.ctr.failed)
	_, _, err = a.transitionODKUKeys(proc, 0, changes(2, 2, 3, 3))
	require.ErrorContains(t, err, "invalid ODKU key transition")
	lookup(0, 2, 0, true)
	lookup(0, 3, 0, false)
	lookup(1, 2, 0, true)
	lookup(1, 0, 1, true)
}

func TestODKUKeyTransitionCanceled(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 1)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	changes := []odkuKeyChange{{old: b.Vecs[1], oldRow: 0, next: b.Vecs[1], nextRow: 0}}
	previous := proc.Ctx
	cancelled, cancel := context.WithCancel(previous)
	cancel()
	proc.Ctx = cancelled
	_, _, err = a.transitionODKUKeys(proc, 0, changes)
	require.ErrorIs(t, err, context.Canceled)
	proc.Ctx = previous
	require.True(t, a.ctr.failed)
	_, _, err = a.transitionODKUKeys(proc, 0, changes)
	require.ErrorContains(t, err, "invalid ODKU key transition")
	owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, owner)
}

func TestODKUKeyTransitionFailureRequiresReset(t *testing.T) {
	a, proc, b, account := newODKUKeyFixture(t, 2)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	// Reuse a vacated fixed-width group so the first transition needs no
	// allocation. The second transition must grow its full varlen hash map.
	require.NoError(t, a.releaseODKUKey(proc, 0, 1, 1))
	textKeys := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { textKeys.Free(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(textKeys, []byte("old"), false, proc.Mp()))
	for i := 1; i <= 512; i++ {
		require.NoError(t, vector.AppendBytes(textKeys, []byte(fmt.Sprintf("key-%d", i)), false, proc.Mp()))
	}
	a.ctr.acceptedKeys = append(a.ctr.acceptedKeys, odkuKeyIndex{})
	a.ctr.acceptedKeys[1].key[0] = textKeys
	require.NoError(t, a.initODKUKey(proc, 1))
	require.NoError(t, a.claimODKUKey(proc, 1, 0, 0))
	for row := 1; row < 512; row++ {
		require.NoError(t, a.claimODKUKey(proc, 1, row, int64(row+10)))
	}
	_, found, err := a.lookupODKUKeyVector(proc, 1, textKeys, 512)
	require.NoError(t, err)
	require.False(t, found)
	used := account.Snapshot().Used
	filler, err := proc.Mp().AllocAccounted(int(odkuKeyTestBudget-used-64), account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
	require.NoError(t, err)
	defer proc.Mp().Free(filler)
	changes := []odkuKeyChange{
		{old: b.Vecs[1], oldRow: 0, next: b.Vecs[1], nextRow: 1},
		{old: textKeys, oldRow: 0, next: textKeys, nextRow: 512},
	}
	_, _, err = a.transitionODKUKeys(proc, 0, changes)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.True(t, a.ctr.failed)
	owner, found, err := a.lookupODKUKeyVector(proc, 0, b.Vecs[1], 1)
	require.NoError(t, err)
	require.True(t, found, "the injected failure must occur after the first key moved")
	require.Zero(t, owner)
	_, _, err = a.transitionODKUKeys(proc, 0, changes)
	require.ErrorContains(t, err, "invalid ODKU key transition")
}

type panicOnKeyClaim struct{ hashmap.Iterator }

func (*panicOnKeyClaim) DetectDup([]*vector.Vector, int) (bool, error) {
	panic("injected key claim panic")
}

func TestODKUKeyTransitionPanicRequiresReset(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 2)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(b.Vecs[1], int64(3), false, proc.Mp()))
	otherKeys := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { otherKeys.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(otherKeys, int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(otherKeys, int64(30), false, proc.Mp()))
	a.ctr.acceptedKeys = append(a.ctr.acceptedKeys, odkuKeyIndex{})
	a.ctr.acceptedKeys[1].key[0] = otherKeys
	require.NoError(t, a.initODKUKey(proc, 1))
	require.NoError(t, a.claimODKUKey(proc, 1, 0, 0))
	original := a.ctr.acceptedKeys[1].iterator
	a.ctr.acceptedKeys[1].iterator = &panicOnKeyClaim{Iterator: original}
	defer func() { a.ctr.acceptedKeys[1].iterator = original }()
	changes := []odkuKeyChange{
		{old: b.Vecs[1], oldRow: 0, next: b.Vecs[1], nextRow: 2},
		{old: otherKeys, oldRow: 0, next: otherKeys, nextRow: 1},
	}
	require.PanicsWithValue(t, "injected key claim panic", func() { _, _, _ = a.transitionODKUKeys(proc, 0, changes) })
	require.True(t, a.ctr.failed)
	owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 2)
	require.NoError(t, err)
	require.True(t, found, "the injected panic must occur after the first key moved")
	require.Zero(t, owner)
	_, _, err = a.transitionODKUKeys(proc, 0, changes)
	require.ErrorContains(t, err, "invalid ODKU key transition")
}

func TestODKUKeyTransitionRejectsIndexStorageAlias(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 1)
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	indexOwned := a.ctr.acceptedKeys[0].fixedKeys
	require.NotNil(t, indexOwned)
	_, _, err = a.transitionODKUKeys(proc, 0, []odkuKeyChange{{old: indexOwned, oldRow: 0, next: b.Vecs[1], nextRow: 0}})
	require.ErrorContains(t, err, "invalid ODKU transition key")
	require.True(t, a.ctr.failed)
}

func TestODKUKeyTransitionVarlen(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 0)
	b.Vecs[1].Free(proc.Mp())
	b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for _, value := range []string{strings.Repeat("old", 32), strings.Repeat("new", 32)} {
		require.NoError(t, vector.AppendBytes(b.Vecs[1], []byte(value), false, proc.Mp()))
	}
	a.ctr.acceptedKeys[0].key[0] = b.Vecs[1]
	require.NoError(t, a.initODKUKey(proc, 0))
	conflict, _, err := a.transitionODKUKeys(proc, 0, []odkuKeyChange{{next: b.Vecs[1], nextRow: 0}})
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	conflict, _, err = a.transitionODKUKeys(proc, 0, []odkuKeyChange{{old: b.Vecs[1], oldRow: 0, next: b.Vecs[1], nextRow: 1}})
	require.NoError(t, err)
	require.Equal(t, -1, conflict)
	_, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 0)
	require.NoError(t, err)
	require.False(t, found)
	owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 1)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, owner)
}

func TestODKUKeyIndexVariableLengthOwnership(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 3)
	b.Vecs[1].Free(proc.Mp())
	b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for i, value := range []string{strings.Repeat("x", 1024), "other", ""} {
		require.NoError(t, vector.AppendBytes(b.Vecs[1], []byte(value), i == 2, proc.Mp()))
	}
	err := seedODKUKeyRows(proc, a, b.Vecs[1])
	require.NoError(t, err)
	key := &a.ctr.acceptedKeys[0]
	require.IsType(t, &hashmap.StrHashMap{}, key.index)
	require.NoError(t, a.releaseODKUKey(proc, 0, 0, 0))
	require.NoError(t, a.claimODKUKey(proc, 0, 0, 2))
	owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(2), owner)
	_, found, err = a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 2)
	require.NoError(t, err)
	require.False(t, found)
	groups := key.index.GroupCount()
	require.NoError(t, a.claimODKUKey(proc, 0, 2, 2))
	require.Equal(t, groups, key.index.GroupCount(), "NULL keys do not own a group")
	owner, found, err = a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), owner)

}

func TestODKUKeyIndexMatchesCurrentOwners(t *testing.T) {
	const rowCount = 20
	a, proc, input, _ := newODKUKeyFixture(t, rowCount)
	err := seedODKUKeyRows(proc, a, input.Vecs[1])
	require.NoError(t, err)
	key := &a.ctr.acceptedKeys[0]
	current := make([]int64, rowCount)
	expected := make(map[int64]int64, rowCount)
	for row := range rowCount {
		current[row] = int64(row + 1)
		expected[current[row]] = int64(row)
	}
	probe := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { probe.Free(proc.Mp()) })
	setProbe := func(value int64) {
		t.Helper()
		probe.CleanOnlyData()
		require.NoError(t, vector.AppendFixed(probe, value, false, proc.Mp()))
		key.key[0] = probe
	}
	verify := func() {
		t.Helper()
		for value := int64(1); value <= 200; value++ {
			setProbe(value)
			owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 0)
			require.NoError(t, err)
			want, exists := expected[value]
			require.Equal(t, exists, found, "key %d", value)
			if exists {
				require.Equal(t, want, owner, "key %d", value)
			}
		}
	}
	rng := rand.New(rand.NewSource(31979))
	nullMoves := 0
	for step := 0; step < 2000; step++ {
		row := rng.Intn(rowCount)
		next := int64(rng.Intn(201)) // zero represents a NULL unique key
		old := current[row]
		if next == old {
			continue
		}
		if next != 0 {
			setProbe(next)
			owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], 0)
			require.NoError(t, err)
			if found {
				require.NotEqual(t, int64(row), owner)
				continue // a different row owns the candidate key
			}
		}
		if old != 0 {
			key.key[0] = input.Vecs[1]
			require.NoError(t, a.releaseODKUKey(proc, 0, row, int64(row)))
			delete(expected, old)
		}
		if next == 0 {
			input.Vecs[1].SetNull(uint64(row))
			nullMoves++
		} else {
			setProbe(next)
			require.NoError(t, a.claimODKUKey(proc, 0, 0, int64(row)))
			require.NoError(t, vector.SetFixedAtNoTypeCheck[int64](input.Vecs[1], row, next))
			input.Vecs[1].UnsetNull(uint64(row))
			expected[next] = int64(row)
		}
		current[row] = next
		if step%127 == 0 {
			verify()
		}
	}
	verify()
	require.Positive(t, nullMoves)
}
