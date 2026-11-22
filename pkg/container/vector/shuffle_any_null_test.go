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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestUnionAllAnyNull(t *testing.T) {
	for _, constant := range []bool{false, true} {
		t.Run(map[bool]string{false: "flat", true: "constant"}[constant], func(t *testing.T) {
			mp := mpool.MustNewZero()
			func() {
				typ := types.T_any.ToType()
				dst := NewVec(typ, mp)
				defer dst.Free(mp)
				dst.SetAllNulls(2)
				dst.setLengthAfterExtend(2)
				var src *Vector
				if constant {
					src = NewConstNull(typ, 3, mp)
				} else {
					src = NewVec(typ, mp)
					src.SetAllNulls(3)
					src.setLengthAfterExtend(3)
				}
				defer src.Free(mp)
				appendAll := GetUnionAllFunction(typ, mp)
				require.NoError(t, appendAll(dst, src))
				require.Equal(t, 5, dst.Length())
				require.True(t, dst.AllNull())
				require.NoError(t, appendAll(dst, dst))
				require.Equal(t, 10, dst.Length())
				require.True(t, dst.AllNull())
				empty := NewVec(typ, mp)
				defer empty.Free(mp)
				require.NoError(t, appendAll(dst, empty))
				require.Equal(t, 10, dst.Length())
			}()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestUnionAllAnyRejectsNonNull(t *testing.T) {
	mp := mpool.MustNewZero()
	func() {
		typ := types.T_any.ToType()
		dst := NewVec(typ, mp)
		defer dst.Free(mp)
		dst.SetAllNulls(1)
		dst.setLengthAfterExtend(1)
		src := NewVec(typ, mp)
		defer src.Free(mp)
		src.setLengthAfterExtend(1)
		require.Error(t, GetUnionAllFunction(typ, mp)(dst, src))
		require.Equal(t, 1, dst.Length())
		require.True(t, dst.AllNull())
	}()
	require.Zero(t, mp.CurrNB())
}

func TestUnionAllAnyOwnedDecodedDestination(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_any.ToType()
	source := NewVec(typ, mp)
	dst := NewVec(typ, mp)
	sibling := NewVec(typ, mp)
	input := NewConstNull(typ, 2, mp)
	t.Cleanup(func() {
		source.Free(mp)
		dst.Free(mp)
		sibling.Free(mp)
		input.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	for range 4 {
		require.NoError(t, AppendNull(source, mp))
	}
	wire, err := source.MarshalBinary()
	require.NoError(t, err)
	before := bytes.Clone(wire)
	require.NoError(t, dst.UnmarshalBinary(wire))
	require.NoError(t, sibling.UnmarshalBinary(wire))
	require.Error(t, GetUnionAllFunction(typ, nil)(dst, input))
	require.Equal(t, 4, dst.Length())
	require.Equal(t, before, wire)
	require.NoError(t, GetUnionAllFunction(typ, mp)(dst, input))
	require.Equal(t, 6, dst.Length())
	require.True(t, dst.AllNull())
	require.Equal(t, before, wire)
	require.Equal(t, 4, sibling.Length())
	require.True(t, sibling.AllNull())
}

func TestUnionAllAnyReusedDestinationClearsStaleRows(t *testing.T) {
	mp := mpool.MustNewZero()
	dst := NewVec(types.T_any.ToType(), mp)
	src := NewConstNull(types.T_any.ToType(), 1, mp)
	t.Cleanup(func() {
		dst.Free(mp)
		src.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	dst.SetAllNulls(4)
	dst.SetLength(4)
	dst.SetLength(2)
	require.NoError(t, GetUnionAllFunction(types.T_any.ToType(), mp)(dst, src))
	require.Equal(t, 3, dst.Length())
	require.Equal(t, []uint64{0, 1, 2}, dst.GetNulls().ToArray())
}

func TestShuffleAllNullAny(t *testing.T) {
	cases := []struct {
		name    string
		initial int
		sels    []int64
	}{
		{name: "permutation", initial: 4, sels: []int64{3, 1, 2, 0}},
		{name: "subset", initial: 4, sels: []int64{2, 0}},
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
					v := NewVec(types.T_any.ToType(), mp)
					defer v.Free(mp)
					v.SetAllNulls(tc.initial)
					v.setLengthAfterExtend(tc.initial)
					if tc.initial == 4 {
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
					require.False(t, v.IsConst())
					if len(tc.sels) > 0 {
						require.True(t, v.AllNull())
					}
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
			v := NewVec(types.T_any.ToType(), mp)
			defer v.Free(mp)
			v.setLengthAfterExtend(1)
			var buf []byte
			if withBuf {
				require.Error(t, v.ShuffleWithBuf([]int64{0}, mp, &buf))
			} else {
				require.Error(t, v.Shuffle([]int64{0}, mp))
			}
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	}
}

func TestShrinkAllNullAny(t *testing.T) {
	cases := []struct {
		name    string
		initial int
		sels    []int64
		negate  bool
		length  int
	}{
		{name: "keep", initial: 4, sels: []int64{1, 2}, length: 2},
		{name: "remove", initial: 4, sels: []int64{1, 2}, negate: true, length: 2},
		{name: "keep_none", initial: 4, sels: []int64{}, length: 0},
		{name: "remove_none", initial: 4, sels: []int64{}, negate: true, length: 4},
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
					v := NewVec(types.T_any.ToType(), mp)
					defer v.Free(mp)
					v.SetAllNulls(tc.initial)
					v.setLengthAfterExtend(tc.initial)
					if tc.initial == 4 {
					}

					var err error
					require.NotPanics(t, func() {
						if byMask {
							var mask bitmap.Bitmap
							require.NoError(t, mask.InitWithSize(int64(len(tc.sels)), mp))
							defer mask.Reset()
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
					require.False(t, v.IsConst())
					if tc.length > 0 {
						require.True(t, v.AllNull())
					}
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
				v := NewVec(types.T_any.ToType(), mp)
				defer v.Free(mp)
				v.setLengthAfterExtend(1)
				if byMask {
					var mask bitmap.Bitmap
					require.NoError(t, mask.InitWithSize(1, mp))
					defer mask.Reset()
					mask.AddMany([]uint64{0})
					require.Error(t, v.ShrinkByMask(&mask, false, 0, mp))
				} else {
					require.Error(t, v.Shrink([]int64{0}, false, mp))
				}
			}()
			require.Equal(t, int64(0), mp.CurrNB())
		})
	}
}

func TestAllNullUsesLogicalLength(t *testing.T) {
	t.Run("shortened_all_null", func(t *testing.T) {
		mp := mpool.MustNewZero()
		func() {
			v := NewVec(types.T_any.ToType(), mp)
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
			v := NewVec(types.T_any.ToType(), mp)
			defer v.Free(mp)
			v.SetAllNulls(4)
			v.SetLength(4)
			v.UnsetNull(1)
			v.SetLength(2)
			require.False(t, v.AllNull())
			require.Error(t, v.Shuffle([]int64{0, 1}, mp))
		}()
		require.Equal(t, int64(0), mp.CurrNB())
	})

	t.Run("nulls_only_outside_logical_range", func(t *testing.T) {
		mp := mpool.MustNewZero()
		func() {
			v := NewVec(types.T_any.ToType(), mp)
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

func TestNullAnySelectionsPreserveRowMetadata(t *testing.T) {
	for _, constant := range []bool{false, true} {
		name := "flat"
		if constant {
			name = "constant"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_any.ToType(), mp)
			if constant {
				v = NewConstNull(types.T_any.ToType(), 4, mp)
				// Some existing constant representations retain the scalar null bit.
				v.GetNulls().Add(0)
			}
			t.Cleanup(func() {
				v.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
			if !constant {
				for range 4 {
					require.NoError(t, AppendNull(v, mp))
				}
			}
			require.NoError(t, v.Shrink([]int64{1, 2, 3}, false, mp))
			var mask bitmap.Bitmap
			require.NoError(t, mask.InitWithSize(1, mp))
			defer mask.Reset()
			mask.Add(0)
			require.NoError(t, v.ShrinkByMask(&mask, false, 1, mp))
			require.Equal(t, 1, v.Length())
			require.True(t, v.IsNull(0))
			require.Equal(t, constant, v.IsConst())
		})
	}
}

func TestNullAnyShuffleComposition(t *testing.T) {
	for _, constant := range []bool{false, true} {
		t.Run(map[bool]string{false: "flat", true: "constant"}[constant], func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_any.ToType(), mp)
			if constant {
				v = NewConstNull(types.T_any.ToType(), 4, mp)
			}
			t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
			if !constant {
				for range 4 {
					require.NoError(t, AppendNull(v, mp))
				}
			}
			require.NoError(t, v.Shuffle([]int64{3, 0, 1}, mp))
			var buf []byte
			require.NoError(t, v.ShuffleWithBuf([]int64{1, 0, 1}, mp, &buf))
			require.NoError(t, v.Shuffle([]int64{2}, mp))
			require.Equal(t, 1, v.Length())
			require.True(t, v.IsNull(0))
			require.NoError(t, v.Shuffle(nil, mp))
			require.Zero(t, v.Length())
		})
	}
}

func TestNullAnyComplementTrimsStaleBitmapRows(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_any.ToType(), mp)
	t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
	for range 5 {
		require.NoError(t, AppendNull(v, mp))
	}
	v.SetLength(3)
	var mask bitmap.Bitmap
	require.NoError(t, mask.InitWithSize(1, mp))
	defer mask.Reset()
	mask.Add(0)
	require.NoError(t, v.ShrinkByMask(&mask, true, 1, mp))
	require.Equal(t, 2, v.Length())
	require.Equal(t, []uint64{0, 1}, v.GetNulls().ToArray())
}

func TestNullAnySelectionKeepsDecodedWireUnchanged(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_any.ToType(), mp)
	first := NewVec(types.T_any.ToType(), mp)
	second := NewVec(types.T_any.ToType(), mp)
	t.Cleanup(func() {
		first.Free(mp)
		second.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	for range 4 {
		require.NoError(t, AppendNull(source, mp))
	}
	wire, err := source.MarshalBinary()
	require.NoError(t, err)
	before := bytes.Clone(wire)
	require.NoError(t, first.UnmarshalBinary(wire))
	require.NoError(t, second.UnmarshalBinary(wire))
	require.Error(t, first.Shuffle([]int64{2, 1}, nil))
	require.Equal(t, 4, first.Length())
	require.Equal(t, before, wire)
	require.NoError(t, first.Shuffle([]int64{2, 1}, mp))
	require.Equal(t, before, wire)
	require.Equal(t, 4, second.Length())
	for row := range 4 {
		require.True(t, second.IsNull(uint64(row)))
	}
}

func TestDomainlessAnyRejectsReadOnlyOrForeignPool(t *testing.T) {
	mp := mpool.MustNewZero()
	other := mpool.MustNewZero()
	src := NewVec(types.T_any.ToType(), mp)
	t.Cleanup(func() {
		src.Free(mp)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, other.CurrNB())
	})
	for range 3 {
		require.NoError(t, AppendNull(src, mp))
	}
	ro, err := src.Window(0, 3)
	require.NoError(t, err)
	t.Cleanup(func() { ro.Free(nil) })
	require.True(t, ro.IsReadOnly())
	require.Error(t, ro.Shuffle([]int64{2, 0}, mp))
	require.Error(t, ro.Shrink([]int64{0}, false, mp))
	require.Equal(t, 3, ro.Length())
	require.True(t, ro.AllNull())
	require.Error(t, src.Shuffle([]int64{2, 0}, other))
	require.Equal(t, 3, src.Length())
	require.NoError(t, src.Shuffle([]int64{2, 0}, mp))
	require.Equal(t, 2, src.Length())
}

func TestVisiblePrefixPredicates(t *testing.T) {
	mp := mpool.MustNewZero()
	flat := NewVec(types.T_any.ToType(), mp)
	empty := NewConstNull(types.T_any.ToType(), 0, mp)
	scalar, err := NewConstFixed(types.T_int32.ToType(), int32(7), 1, mp)
	require.NoError(t, err)
	t.Cleanup(func() {
		flat.Free(mp)
		empty.Free(mp)
		scalar.Free(mp)
		require.Zero(t, mp.CurrNB())
	})
	require.False(t, flat.AllNull())
	require.True(t, empty.AllNull())

	flat.SetLength(4)
	flat.GetNulls().Add(0, 2)
	flat.SetLength(2)
	require.False(t, flat.AllNull()) // total count equals length, but row 1 is live
	flat.GetNulls().Add(1)
	require.True(t, flat.AllNull())
	flat.GetNulls().Del(0)
	require.False(t, flat.AllNull())

	scalar.GetNulls().Add(1)
	require.False(t, scalar.AllNull()) // const nullness belongs to scalar row 0
}
