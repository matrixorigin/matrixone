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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestShuffleAllNullAny(t *testing.T) {
	cases := []struct {
		name             string
		initial          int
		sels             []int64
		expectedGrouping []uint64
	}{
		{name: "permutation", initial: 4, sels: []int64{3, 1, 2, 0}, expectedGrouping: []uint64{2, 3}},
		{name: "subset", initial: 4, sels: []int64{2, 0}, expectedGrouping: []uint64{0, 1}},
		{name: "duplicate", initial: 4, sels: []int64{1, 1, 1}},
		{name: "empty_output", initial: 4, sels: []int64{}},
		{name: "empty_input", initial: 0, sels: []int64{}},
	}

	for _, withBuf := range []bool{false, true} {
		for _, tc := range cases {
			name := tc.name
			if withBuf {
				name += "/with_buf"
			}
			t.Run(name, func(t *testing.T) {
				mp := mpool.MustNewZero()
				func() {
					v := NewVec(types.T_any.ToType())
					defer v.Free(mp)
					v.SetAllNulls(tc.initial)
					v.setLengthAfterExtend(tc.initial)
					if tc.initial == 4 {
						v.GetGrouping().Set(0)
						v.GetGrouping().Set(2)
					}

					var err error
					var buf []byte
					require.NotPanics(t, func() {
						if withBuf {
							err = v.ShuffleWithBuf(tc.sels, mp, &buf)
						} else {
							err = v.Shuffle(tc.sels, mp)
						}
					})
					require.NoError(t, err)
					require.Equal(t, len(tc.sels), v.Length())
					require.True(t, v.IsConstNull())
					require.ElementsMatch(t, tc.expectedGrouping, v.GetGrouping().ToArray())
					expectedNulls := make([]uint64, len(tc.sels))
					for i := range tc.sels {
						expectedNulls[i] = uint64(i)
						require.True(t, v.IsNull(uint64(i)))
					}
					require.ElementsMatch(t, expectedNulls, v.GetNulls().ToArray())
				}()
				require.Equal(t, int64(0), mp.CurrNB())
			})
		}
	}
}

func TestShuffleRejectsNonNullAny(t *testing.T) {
	for _, withBuf := range []bool{false, true} {
		mp := mpool.MustNewZero()
		func() {
			v := NewVec(types.T_any.ToType())
			defer v.Free(mp)
			v.setLengthAfterExtend(1)
			var buf []byte
			require.Panics(t, func() {
				if withBuf {
					_ = v.ShuffleWithBuf([]int64{0}, mp, &buf)
				} else {
					_ = v.Shuffle([]int64{0}, mp)
				}
			})
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestShrinkAllNullAny(t *testing.T) {
	cases := []struct {
		name     string
		initial  int
		sels     []int64
		negate   bool
		length   int
		grouping []uint64
	}{
		{name: "keep", initial: 4, sels: []int64{1, 2}, length: 2, grouping: []uint64{1}},
		{name: "remove", initial: 4, sels: []int64{1, 2}, negate: true, length: 2, grouping: []uint64{0}},
		{name: "keep_none", initial: 4, sels: []int64{}, length: 0},
		{name: "remove_none", initial: 4, sels: []int64{}, negate: true, length: 4, grouping: []uint64{0, 2}},
		{name: "empty_input", initial: 0, sels: []int64{}, length: 0},
	}

	for _, byMask := range []bool{false, true} {
		for _, tc := range cases {
			name := tc.name
			if byMask {
				name += "/by_mask"
			}
			t.Run(name, func(t *testing.T) {
				mp := mpool.MustNewZero()
				func() {
					v := NewVec(types.T_any.ToType())
					defer v.Free(mp)
					v.SetAllNulls(tc.initial)
					v.setLengthAfterExtend(tc.initial)
					if tc.initial == 4 {
						v.GetGrouping().Set(0)
						v.GetGrouping().Set(2)
					}

					var err error
					require.NotPanics(t, func() {
						if byMask {
							var mask bitmap.Bitmap
							mask.InitWithSize(int64(len(tc.sels)))
							positions := make([]uint64, len(tc.sels))
							for i, sel := range tc.sels {
								positions[i] = uint64(sel)
							}
							mask.AddMany(positions)
							err = v.ShrinkByMask(&mask, tc.negate, 0, mp)
						} else {
							err = v.Shrink(tc.sels, tc.negate, mp)
						}
					})
					require.NoError(t, err)
					require.Equal(t, tc.length, v.Length())
					require.True(t, v.AllNull())
					require.ElementsMatch(t, tc.grouping, v.GetGrouping().ToArray())
					expectedNulls := make([]uint64, tc.length)
					for i := range expectedNulls {
						expectedNulls[i] = uint64(i)
					}
					require.ElementsMatch(t, expectedNulls, v.GetNulls().ToArray())
				}()
				require.Equal(t, int64(0), mp.CurrNB())
			})
		}
	}
}

func TestShrinkRejectsNonNullAny(t *testing.T) {
	for _, byMask := range []bool{false, true} {
		name := "slice"
		if byMask {
			name = "mask"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			func() {
				v := NewVec(types.T_any.ToType())
				defer v.Free(mp)
				v.setLengthAfterExtend(1)
				require.Panics(t, func() {
					if byMask {
						var mask bitmap.Bitmap
						mask.InitWithSize(1)
						mask.AddMany([]uint64{0})
						_ = v.ShrinkByMask(&mask, false, 0, mp)
					} else {
						_ = v.Shrink([]int64{0}, false, mp)
					}
				})
			}()
			require.Equal(t, int64(0), mp.CurrNB())
		})
	}
}

func TestAllNullUsesLogicalLength(t *testing.T) {
	t.Run("shortened_all_null", func(t *testing.T) {
		mp := mpool.MustNewZero()
		func() {
			v := NewVec(types.T_any.ToType())
			defer v.Free(mp)
			v.SetAllNulls(4)
			v.SetLength(4)
			v.SetLength(2)
			require.True(t, v.AllNull())
			var err error
			require.NotPanics(t, func() {
				err = v.Shuffle([]int64{0, 1}, mp)
			})
			require.NoError(t, err)
			require.Equal(t, 2, v.Length())
			require.True(t, v.AllNull())
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("non_null_in_logical_range", func(t *testing.T) {
		mp := mpool.MustNewZero()
		func() {
			v := NewVec(types.T_any.ToType())
			defer v.Free(mp)
			v.SetAllNulls(4)
			v.SetLength(4)
			v.UnsetNull(1)
			v.SetLength(2)
			require.False(t, v.AllNull())
			require.Panics(t, func() {
				_ = v.Shuffle([]int64{0, 1}, mp)
			})
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("nulls_only_outside_logical_range", func(t *testing.T) {
		mp := mpool.MustNewZero()
		func() {
			v := NewVec(types.T_any.ToType())
			defer v.Free(mp)
			v.SetAllNulls(4)
			v.SetLength(4)
			v.UnsetNull(0)
			v.UnsetNull(1)
			v.SetLength(2)
			require.False(t, v.AllNull())
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	})
}
