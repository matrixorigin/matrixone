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

type readonlyAreaMutation struct {
	name string
	copy bool
	run  func(*Vector, *Vector, *mpool.MPool) error
}

func readonlyAreaMutations() []readonlyAreaMutation {
	return []readonlyAreaMutation{
		{"copy", true, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.Copy(src, 0, 0, mp)
		}},
		{"batch", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.UnionBatch(src, 0, 1, nil, mp)
		}},
		{"selected-batch", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.UnionBatch(src, 0, 1, []uint8{1}, mp)
		}},
		{"one", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.UnionOne(src, 0, mp)
		}},
		{"multi", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.UnionMulti(src, 0, 1, mp)
		}},
		{"selection", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.Union(src, []int64{0}, mp)
		}},
		{"selection-int32", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return dst.UnionInt32(src, []int32{0}, mp)
		}},
		{"all", false, func(dst, src *Vector, mp *mpool.MPool) error {
			return GetUnionAllFunction(*dst.GetType(), mp)(dst, src)
		}},
		{"preflighted", false, func(dst, src *Vector, mp *mpool.MPool) error {
			flags := []uint8{1}
			preflight, err := dst.PreExtendSelectedBatch(src, 0, 1, flags, dst.Length()+1, mp)
			if err != nil {
				return err
			}
			return dst.UnionBatchPreflighted(src, 0, 1, flags, &preflight, mp)
		}},
	}
}

func TestCopyWithReadonlyAreaMutations(t *testing.T) {
	for _, mutation := range readonlyAreaMutations() {
		for _, leased := range []bool{false, true} {
			t.Run(mutation.name+"/leased="+strconv.FormatBool(leased), func(t *testing.T) {
				mp := mpool.MustNewZero()
				source := NewOffHeapVecWithType(types.T_text.ToType())
				destination := NewOffHeapVecWithType(types.T_text.ToType())
				extra := NewOffHeapVecWithType(types.T_text.ToType())
				defer func() {
					source.Free(mp)
					destination.Free(mp)
					extra.Free(mp)
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}()
				values := [][]byte{
					bytes.Repeat([]byte{'a'}, 64),
					bytes.Repeat([]byte{'b'}, 64),
					bytes.Repeat([]byte{'c'}, 64),
				}
				for _, value := range values {
					require.NoError(t, AppendBytes(source, value, false, mp))
				}
				if leased {
					require.NoError(t, copyWithBorrowedArea(destination, source, mp))
					view, err := readonlyView(destination, mp)
					require.NoError(t, err)
					defer view.Free(mp)
					defer func() {
						for row, value := range values {
							require.Equal(t, value, view.GetBytesAt(row))
						}
					}()
				} else {
					require.NoError(t, destination.UnionBatch(source, 0, source.Length(), nil, mp))
				}
				if !mutation.copy {
					// Keep spare descriptors: area growth must not invalidate the
					// capacity admitted for the next appended row.
					require.NoError(t, destination.Shrink([]int64{2}, true, mp))
				}
				for _, size := range []int{4096, 6} {
					extra.CleanOnlyData()
					replacement := bytes.Repeat([]byte{'x'}, size)
					require.NoError(t, AppendBytes(extra, replacement, false, mp))
					require.NoError(t, mutation.run(destination, extra, mp))
					require.GreaterOrEqual(t, len(destination.GetData()), destination.Length()*types.VarlenaSize)
					if mutation.copy {
						require.Equal(t, 3, destination.Length())
						require.Equal(t, replacement, destination.GetBytesAt(0))
						require.Equal(t, values[1], destination.GetBytesAt(1))
						require.Equal(t, values[2], destination.GetBytesAt(2))
					} else {
						require.Equal(t, replacement, destination.GetBytesAt(destination.Length()-1))
						require.Equal(t, values[0], destination.GetBytesAt(0))
						require.Equal(t, values[1], destination.GetBytesAt(1))
					}
				}
				for row, value := range values {
					require.Equal(t, value, source.GetBytesAt(row))
				}
				require.False(t, destination.HasBorrowedBacking())
			})
		}
	}
}

func TestCopyWithReadonlyAreaPreflightWithinCapacity(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_text.ToType())
	destination := NewOffHeapVecWithType(types.T_text.ToType())
	extra := NewOffHeapVecWithType(types.T_text.ToType())
	defer func() {
		source.Free(mp)
		destination.Free(mp)
		extra.Free(mp)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()
	for range 3 {
		require.NoError(t, AppendBytes(source, bytes.Repeat([]byte{'a'}, 64), false, mp))
	}
	require.NoError(t, copyWithBorrowedArea(destination, source, mp))
	require.NoError(t, destination.Shrink([]int64{2}, true, mp))
	require.NoError(t, AppendBytes(extra, []byte("inline"), false, mp))
	before := mp.CurrNB()
	flags := []uint8{0}
	preflight, err := destination.PreExtendSelectedBatch(extra, 0, 1, flags, 2, mp)
	require.NoError(t, err)
	require.NoError(t, destination.UnionBatchPreflighted(extra, 0, 1, flags, &preflight, mp))
	require.Equal(t, before, mp.CurrNB())
	require.True(t, destination.HasBorrowedBacking(), "an empty selection must not materialize")
	flags[0] = 1
	preflight, err = destination.PreExtendSelectedBatch(extra, 0, 1, flags, 3, mp)
	require.NoError(t, err)
	require.False(t, destination.HasBorrowedBacking(), "preflight must finish COW even without area growth")
	before = mp.CurrNB()
	require.NoError(t, destination.UnionBatchPreflighted(extra, 0, 1, flags, &preflight, mp))
	require.Equal(t, before, mp.CurrNB(), "publication must not repeat admission")
	require.Equal(t, 3, destination.Length())
	require.Equal(t, "inline", destination.GetStringAt(2))
}

func TestCopyWithReadonlyAreaCopyAliasedSource(t *testing.T) {
	for _, window := range []bool{false, true} {
		t.Run(strconv.FormatBool(window), func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewOffHeapVecWithType(types.T_text.ToType())
			destination := NewOffHeapVecWithType(types.T_text.ToType())
			defer func() {
				source.Free(mp)
				destination.Free(mp)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			}()
			value := bytes.Repeat([]byte{'a'}, 64)
			require.NoError(t, AppendBytes(source, value, false, mp))
			require.NoError(t, AppendBytes(source, []byte("replace"), false, mp))
			require.NoError(t, copyWithBorrowedArea(destination, source, mp))
			input := destination
			if window {
				var err error
				input, err = destination.Window(0, 1)
				require.NoError(t, err)
				defer input.Free(mp)
			}
			require.NoError(t, destination.Copy(input, 1, 0, mp))
			require.Equal(t, value, destination.GetBytesAt(0))
			require.Equal(t, value, destination.GetBytesAt(1))
			require.False(t, destination.HasBorrowedBacking())
		})
	}
}

func TestCopyWithReadonlyAreaMutationFailure(t *testing.T) {
	for _, mutation := range readonlyAreaMutations() {
		t.Run(mutation.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			// The lent area is not charged to the account (its lender owns
			// it), so the limit leaves no room for the copy-on-write either.
			state := newTestVectorAllocationAccount(t, 320, 32)
			source := NewOffHeapVecWithType(types.T_text.ToType())
			destination := newAccountedTestVector(t, types.T_text.ToType())
			amp := state.view(t, mp)
			extra := NewOffHeapVecWithType(types.T_text.ToType())
			defer func() {
				source.Free(mp)
				destination.Free(mp)
				extra.Free(mp)
				require.Zero(t, mp.CurrNB())
				finalizeTestVectorAllocationAccount(t, state)
				mpool.DeleteMPool(mp)
			}()
			value := bytes.Repeat([]byte{'a'}, 64)
			for range 3 {
				require.NoError(t, AppendBytes(source, value, false, mp))
			}
			require.NoError(t, copyWithBorrowedArea(destination, source, amp))
			require.NoError(t, AppendBytes(extra, bytes.Repeat([]byte{'x'}, 4096), false, mp))
			extra.GetGrouping().Add(0)
			beforeMP := mp.CurrNB()
			beforeAccount := state.account.Snapshot().Used
			err := mutation.run(destination, extra, amp)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, beforeMP, mp.CurrNB())
			require.Equal(t, beforeAccount, state.account.Snapshot().Used)
			require.True(t, destination.HasBorrowedBacking())
			require.Equal(t, 3, destination.Length())
			require.False(t, destination.GetGrouping().Any())
			for row := range 3 {
				require.Equal(t, value, destination.GetBytesAt(row))
			}
		})
	}
}
