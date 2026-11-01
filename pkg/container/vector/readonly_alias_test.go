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

func readonlyAliasVectors(
	t *testing.T, mp *mpool.MPool, values [][]byte, leased bool, selection *AllocationAccountSelection,
) (*Vector, *Vector) {
	t.Helper()
	source := NewOffHeapVecWithType(types.T_text.ToType())
	t.Cleanup(func() { source.Free(mp) })
	destination := NewOffHeapVecWithType(types.T_text.ToType())
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, destination.SetAllocationAccount(selection))
	for _, value := range values {
		require.NoError(t, AppendBytes(source, value, value == nil, mp))
	}
	if leased {
		require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
	} else {
		require.NoError(t, destination.UnionBatch(source, 0, source.Length(), nil, mp))
	}
	return source, destination
}

func TestReadonlyWindowMutations(t *testing.T) {
	for _, poison := range []bool{false, true} {
		name := "default"
		if poison {
			name = "poison"
		}
		t.Run(name, func(t *testing.T) {
			// These tests are serial; restore the process-wide debug mode before
			// any other test can run.
			if poison {
				mpool.EnableDebugPoisonOnFree()
				t.Cleanup(mpool.DisableDebugPoisonOnFree)
			}
			for _, mutation := range readonlyAreaMutations() {
				if mutation.name == "preflighted" {
					continue
				}
				t.Run(mutation.name, func(t *testing.T) {
					for row := range 2 {
						name := "inline"
						if row == 1 {
							name = "area"
						}
						t.Run(name, func(t *testing.T) {
							for _, leased := range []bool{false, true} {
								t.Run("leased="+strconv.FormatBool(leased), func(t *testing.T) {
									mp := mpool.MustNewZero()
									t.Cleanup(func() {
										require.Zero(t, mp.CurrNB())
										mpool.DeleteMPool(mp)
									})
									values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), bytes.Repeat([]byte{'c'}, 64)}
									source, destination := readonlyAliasVectors(t, mp, values, leased, nil)
									destination.Shrink([]int64{2}, true)
									require.GreaterOrEqual(t, cap(destination.GetData()), 3*types.VarlenaSize)
									input, err := destination.Window(row, row+1)
									require.NoError(t, err)
									t.Cleanup(func() { input.Free(mp) })
									require.True(t, byteSlicesOverlap(destination.data, input.data))
									require.NoError(t, mutation.run(destination, input, mp))
									want := [][]byte{values[0], values[1], values[row]}
									if mutation.copy {
										want = [][]byte{values[row], values[1]}
									}
									require.Equal(t, len(want), destination.Length())
									require.GreaterOrEqual(t, len(destination.GetData()), len(want)*types.VarlenaSize)
									for index, value := range want {
										require.Equal(t, value, destination.GetBytesAt(index))
									}
									for index, value := range values {
										require.Equal(t, value, source.GetBytesAt(index))
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func TestReadonlyWindowSelectionMetadata(t *testing.T) {
	for _, int32Selection := range []bool{false, true} {
		for _, leased := range []bool{false, true} {
			t.Run("int32="+strconv.FormatBool(int32Selection)+"/leased="+strconv.FormatBool(leased), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				})
				values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), nil, []byte("d"), []byte("e"), []byte("f"), []byte("g")}
				_, destination := readonlyAliasVectors(t, mp, values, leased, nil)
				destination.GetGrouping().Add(1)
				require.NoError(t, destination.SetStringSourcesWithMP([]types.StringSource{
					types.StringSourceLiteral, types.StringSourceUserVariable, types.StringSourceSQLPrepare,
					types.StringSourceExpression, types.StringSourceExpression, types.StringSourceExpression, types.StringSourceExpression,
				}, mp))
				destination.Shrink([]int64{3, 4, 5, 6}, true)
				require.GreaterOrEqual(t, cap(destination.GetData()), 7*types.VarlenaSize)
				input, err := destination.Window(0, 3)
				require.NoError(t, err)
				t.Cleanup(func() { input.Free(mp) })
				if int32Selection {
					err = destination.UnionInt32(input, []int32{1, 0, 2, 1}, mp)
				} else {
					err = destination.Union(input, []int64{1, 0, 2, 1}, mp)
				}
				require.NoError(t, err)
				require.Equal(t, 7, destination.Length())
				for row, original := range []int{0, 1, 2, 1, 0, 2, 1} {
					require.Equal(t, original == 2, destination.IsNull(uint64(row)))
					require.Equal(t, original == 1, destination.GetGrouping().Contains(uint64(row)))
					require.Equal(t, []types.StringSource{
						types.StringSourceLiteral, types.StringSourceUserVariable, types.StringSourceSQLPrepare,
					}[original], destination.GetStringSourceAt(row))
					if original != 2 {
						require.Equal(t, values[original], destination.GetBytesAt(row))
					}
				}
			})
		}
	}
}

func TestReadonlyWindowSelfAndRetainedSource(t *testing.T) {
	for _, mutation := range readonlyAreaMutations() {
		if mutation.copy || mutation.name == "preflighted" {
			continue
		}
		for _, retained := range []bool{false, true} {
			t.Run(mutation.name+"/retained="+strconv.FormatBool(retained), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				})
				values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), []byte("c"), []byte("d"), []byte("e")}
				_, destination := readonlyAliasVectors(t, mp, values, true, nil)
				destination.Shrink([]int64{2, 3, 4}, true)
				require.GreaterOrEqual(t, cap(destination.GetData()), 4*types.VarlenaSize)
				input := destination
				if retained {
					var err error
					input, err = destination.RetainedReadonlyViewWithMP(mp)
					require.NoError(t, err)
					t.Cleanup(func() { input.Free(mp) })
					require.False(t, byteSlicesOverlap(destination.data, input.data))
				}
				want := [][]byte{values[0], values[1], values[0]}
				var err error
				switch mutation.name {
				case "one":
					err = destination.UnionOne(input, 1, mp)
					want[2] = values[1]
				case "multi":
					err = destination.UnionMulti(input, 1, 2, mp)
					want = [][]byte{values[0], values[1], values[1], values[1]}
				default:
					err = mutation.run(destination, input, mp)
					if mutation.name == "all" {
						want = append(want, values[1])
					}
				}
				require.NoError(t, err)
				require.Equal(t, len(want), destination.Length())
				for row, value := range want {
					require.Equal(t, value, destination.GetBytesAt(row))
				}
				if retained {
					for row := range 2 {
						require.Equal(t, values[row], input.GetBytesAt(row))
					}
				}
			})
		}
	}
}

func TestReadonlyFixedWindowMutations(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	for _, mutation := range readonlyAreaMutations() {
		if mutation.name == "preflighted" {
			continue
		}
		for _, validityOnly := range []bool{false, true} {
			t.Run(mutation.name+"/validity-only="+strconv.FormatBool(validityOnly), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				})
				values := []int64{11, 22, 33}
				var destination *Vector
				if validityOnly {
					destination = NewOffHeapVecWithType(types.T_int64.ToType())
					t.Cleanup(func() { destination.Free(mp) })
					for _, value := range values {
						require.NoError(t, AppendFixed(destination, value, false, mp))
					}
					destination.Shrink([]int64{2}, true)
					require.NoError(t, destination.PrepareBorrowedValidity(2, mp))
					validity := []byte{0b01}
					lease, err := NewRefCountedBufferLease(validity, 1, nil)
					require.NoError(t, err)
					t.Cleanup(lease.Release)
					require.NoError(t, destination.GetNulls().InstallBorrowedValidity(validity, 0, 2, 1, lease))
				} else {
					data := types.EncodeSlice(values)
					lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
					require.NoError(t, err)
					t.Cleanup(lease.Release)
					destination, err = NewBorrowedFixedVector(types.T_int64.ToType(), 2, data[:16], lease)
					require.NoError(t, err)
					t.Cleanup(func() { destination.Free(mp) })
				}
				input, err := destination.Window(0, 1)
				require.NoError(t, err)
				t.Cleanup(func() { input.Free(mp) })
				require.True(t, destination.HasBorrowedBacking())
				require.NoError(t, mutation.run(destination, input, mp))
				want := []int64{11, 22, 11}
				if mutation.copy {
					want = want[:2]
				}
				require.Equal(t, want, MustFixedColNoTypeCheck[int64](destination))
				require.Equal(t, validityOnly, destination.IsNull(1))
				require.Equal(t, []int64{11, 22, 33}, values)
			})
		}
	}
}

func TestReadonlyWindowMutationFailure(t *testing.T) {
	for _, mutation := range readonlyAreaMutations() {
		if mutation.name == "preflighted" {
			continue
		}
		for _, failAfter := range []int{1, 2, 3} {
			t.Run(mutation.name+"/allocation="+strconv.Itoa(failAfter), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() {
					require.Zero(t, mp.CurrNB())
					mpool.DeleteMPool(mp)
				})
				state := newTestVectorAllocationAccount(t, 1<<20, 64)
				t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
				controller := &rejectNextVectorAllocation{}
				class, err := state.account.RegisterCapacityController(controller)
				require.NoError(t, err)
				state.selection, err = NewAllocationAccountSelectionWithCapacityClass(
					state.account, testVectorAllocationOwner, testVectorDataAllocationSite,
					testVectorAreaAllocationSite, testVectorNullAllocationSite, testVectorGroupAllocationSite, class,
				)
				require.NoError(t, err)
				values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), bytes.Repeat([]byte{'c'}, 64)}
				_, destination := readonlyAliasVectors(t, mp, values, true, state.selection)
				destination.Shrink([]int64{2}, true)
				input, err := destination.Window(0, 1)
				require.NoError(t, err)
				t.Cleanup(func() { input.Free(mp) })
				beforeMP := mp.CurrNB()
				beforeAccount := state.account.Snapshot().Used
				beforeController := controller.used
				controller.failAt = controller.calls + failAfter
				require.ErrorIs(t, mutation.run(destination, input, mp), mpool.ErrAllocationAccountCapacity)
				require.True(t, controller.rejected)
				require.Equal(t, beforeMP, mp.CurrNB())
				require.Equal(t, beforeAccount, state.account.Snapshot().Used)
				require.Equal(t, beforeController, controller.used)
				require.True(t, destination.HasBorrowedBacking())
				require.Equal(t, 2, destination.Length())
				require.Equal(t, values[0], input.GetBytesAt(0))
				for row := range 2 {
					require.Equal(t, values[row], destination.GetBytesAt(row))
				}
				controller.failAt = 0
				require.NoError(t, mutation.run(destination, input, mp))
				if mutation.copy {
					require.Equal(t, 2, destination.Length())
					require.Equal(t, values[0], destination.GetBytesAt(0))
				} else {
					require.Equal(t, 3, destination.Length())
					require.Equal(t, values[0], destination.GetBytesAt(2))
				}
			})
		}
	}
}

func TestReadonlyWindowPreflightRejectsAlias(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), bytes.Repeat([]byte{'c'}, 64)}
	_, destination := readonlyAliasVectors(t, mp, values, true, nil)
	input, err := destination.Window(0, 1)
	require.NoError(t, err)
	t.Cleanup(func() { input.Free(mp) })
	before := mp.CurrNB()
	require.ErrorIs(t, destination.PreExtendSelectedBatch(input, 0, 1, []uint8{1}, 4, mp), mpool.ErrAllocationAccountInvalid)
	require.Equal(t, before, mp.CurrNB())
	require.True(t, destination.HasBorrowedBacking())
	require.Equal(t, values[0], input.GetBytesAt(0))
}
