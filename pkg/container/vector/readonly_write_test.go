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

package vector

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestReadonlySetterSources(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	operations := []struct {
		name string
		run  func(*Vector, *Vector, int, *mpool.MPool) error
	}{
		{"bytes-from", func(v, w *Vector, row int, mp *mpool.MPool) error { return SetBytesAtFrom(v, 0, w, row, mp) }},
		{"raw-from", func(v, w *Vector, row int, mp *mpool.MPool) error { return v.SetRawBytesAtFrom(0, w, row, mp) }},
		{"raw-unset", func(v, w *Vector, row int, mp *mpool.MPool) error {
			return v.SetRawBytesAtFromAndUnsetNull(0, w, row, mp)
		}},
		{"const-from", func(v, w *Vector, row int, mp *mpool.MPool) error {
			return GetConstSetFunction(*v.GetType(), mp)(v, w, int64(row), 3)
		}},
		{"raw-bytes", func(v, w *Vector, row int, mp *mpool.MPool) error { return SetBytesAt(v, 0, w.GetBytesAt(row), mp) }},
		{"const-bytes", func(v, w *Vector, row int, mp *mpool.MPool) error { return SetConstBytes(v, w.GetBytesAt(row), 3, mp) }},
	}
	for _, operation := range operations {
		for _, leased := range []bool{false, true} {
			for _, window := range []bool{false, true} {
				for row := range 2 {
					t.Run(operation.name+"/leased="+strconv.FormatBool(leased)+"/window="+strconv.FormatBool(window)+"/row="+strconv.Itoa(row), func(t *testing.T) {
						mp := mpool.MustNewZero()
						t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
						values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), bytes.Repeat([]byte{'c'}, 64)}
						_, destination := readonlyAliasVectors(t, mp, values, leased, nil)
						input := destination
						selected := row
						if window {
							var err error
							input, err = destination.Window(row, row+1)
							require.NoError(t, err)
							t.Cleanup(func() { input.Free(mp) })
							selected = 0
						}
						require.NoError(t, operation.run(destination, input, selected, mp))
						require.Equal(t, values[row], destination.GetBytesAt(0))
					})
				}
			}
		}
	}
}

type readonlyWriteController struct {
	rejectNextVectorAllocation
	onAcquire func()
}

func (c *readonlyWriteController) AcquireAllocationCapacity(size uint64) error {
	if callback := c.onAcquire; callback != nil {
		c.onAcquire = nil
		callback()
	}
	return c.rejectNextVectorAllocation.AcquireAllocationCapacity(size)
}

func readonlyWriteAccount(t *testing.T, controller *readonlyWriteController) testVectorAllocationAccount {
	t.Helper()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	class, err := state.account.RegisterCapacityController(controller)
	require.NoError(t, err)
	state.selection, err = NewAllocationAccountSelectionWithCapacityClass(
		state.account, testVectorAllocationOwner, testVectorDataAllocationSite,
		testVectorAreaAllocationSite, testVectorNullAllocationSite, testVectorGroupAllocationSite, class,
	)
	require.NoError(t, err)
	return state
}

func TestReadonlyAppendInterleavedSnapshots(t *testing.T) {
	for _, leased := range []bool{false, true} {
		t.Run(strconv.FormatBool(leased), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			secondMP := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, secondMP.CurrNB()); mpool.DeleteMPool(secondMP) })
			controller := &readonlyWriteController{}
			state := readonlyWriteAccount(t, controller)
			base := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
			t.Cleanup(func() { base.Free(mp) })
			require.NoError(t, AppendBytes(base, bytes.Repeat([]byte{'a'}, 64), false, mp))
			require.NoError(t, base.PreExtendWithArea(0, 128, mp))
			buffer := DetachVectorArea(base)
			lease, err := NewRefCountedBufferLease(buffer.data, int64(buffer.Capacity()), func() { buffer.Free(mp) })
			if err != nil {
				buffer.Free(mp)
			}
			require.NoError(t, err)
			t.Cleanup(lease.Release)
			require.NoError(t, base.InstallBorrowedArea(buffer.data, lease))
			first, err := base.RetainedReadonlyViewWithMP(mp)
			require.NoError(t, err)
			t.Cleanup(func() { first.Free(mp) })
			// An independently owned source descriptor keeps the second retained
			// snapshot outside the first account's admission lock. Both snapshots
			// still retain exactly the same physical area and starting offset.
			secondSource := NewOffHeapVecWithType(*base.GetType())
			t.Cleanup(func() { secondSource.Free(secondMP) })
			require.NoError(t, secondSource.PreExtend(1, secondMP))
			secondSource.SetLength(1)
			copy(secondSource.data, base.data[:types.VarlenaSize])
			require.NoError(t, secondSource.InstallBorrowedArea(base.area, lease))
			second, err := secondSource.RetainedReadonlyViewWithMP(secondMP)
			require.NoError(t, err)
			t.Cleanup(func() { second.Free(secondMP) })
			if !leased {
				require.NoError(t, first.MaterializeOwned(mp))
				require.NoError(t, second.MaterializeOwned(secondMP))
				require.NoError(t, first.PreExtendWithArea(0, 128, mp))
				require.NoError(t, second.PreExtendWithArea(0, 128, secondMP))
			}
			x, y := bytes.Repeat([]byte{'x'}, 24), bytes.Repeat([]byte{'y'}, 24)
			interleaved := false
			controller.onAcquire = func() {
				interleaved = true
				require.NoError(t, AppendBytes(second, y, false, secondMP))
			}
			require.NoError(t, AppendBytes(first, x, false, mp))
			controller.onAcquire = nil
			if !interleaved {
				require.NoError(t, AppendBytes(second, y, false, secondMP))
			}
			if leased {
				require.True(t, interleaved)
			}
			require.Equal(t, x, first.GetBytesAt(1))
			require.Equal(t, y, second.GetBytesAt(1))
			require.Equal(t, bytes.Repeat([]byte{'a'}, 64), base.GetBytesAt(0))
		})
	}
}

func TestReadonlyFixedPreflightPublication(t *testing.T) {
	for _, validated := range []bool{false, true} {
		for _, alias := range []bool{false, true} {
			t.Run("validated="+strconv.FormatBool(validated)+"/alias="+strconv.FormatBool(alias), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
				controller := &readonlyWriteController{}
				state := readonlyWriteAccount(t, controller)
				destination := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
				t.Cleanup(func() { destination.Free(mp) })
				for _, value := range []int64{11, 22, 33} {
					require.NoError(t, AppendFixed(destination, value, false, mp))
				}
				destination.Shrink([]int64{2}, true)
				var input *Vector
				if alias {
					var err error
					input, err = destination.Window(0, 1)
					require.NoError(t, err)
				} else {
					input = NewOffHeapVecWithType(types.T_int64.ToType())
					require.NoError(t, AppendFixed(input, int64(42), false, mp))
				}
				t.Cleanup(func() { input.Free(mp) })
				before := mp.CurrNB()
				var err error
				if validated {
					err = destination.PreExtendSelectedBatchValidated(input, 0, 1, []uint8{1}, 3, mp)
				} else {
					err = destination.PreExtendSelectedBatch(input, 0, 1, []uint8{1}, 3, mp)
				}
				if alias {
					require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
					require.Equal(t, before, mp.CurrNB())
					require.False(t, destination.preflightAreaReady)
					return
				}
				require.NoError(t, err)
				calls := controller.calls
				controller.failAt = calls + 1
				require.NoError(t, destination.UnionBatchPreflighted(input, 0, 1, []uint8{1}, mp))
				require.Equal(t, calls, controller.calls)
				require.Equal(t, []int64{11, 22, 42}, MustFixedColNoTypeCheck[int64](destination))
			})
		}
	}
}
