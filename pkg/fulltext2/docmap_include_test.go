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

package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestDocmapIncludeRoundTrip pins the INCLUDE-values docmap section: a build-side segment
// with per-doc include values (mixed int64 + varchar, including NULLs) encodes and decodes
// back to the same typed values via the loaded-side offset-table accessor, without
// disturbing the pk / docLen sections.
func TestDocmapIncludeRoundTrip(t *testing.T) {
	build := &Segment{
		PkType:       int32(types.T_int64),
		pks:          []any{int64(10), int64(20), int64(30)},
		docLen:       []int32{3, 5, 7},
		includeTypes: []int32{int32(types.T_int64), int32(types.T_varchar)},
		includeVals: [][]any{
			{int64(100), []byte("active")},
			{int64(200), nil},         // NULL varchar
			{nil, []byte("archived")}, // NULL int
		},
	}
	data, err := build.encodeDocmap()
	require.NoError(t, err)

	loaded := &Segment{}
	require.NoError(t, loaded.decodeDocmap(data))
	require.EqualValues(t, 3, loaded.N)
	require.Equal(t, 2, loaded.nIncludeCols())

	check := func(ord int64, col int, wantVal any, wantNull bool) {
		v, isNull, verr := loaded.includeVal(ord, col)
		require.NoError(t, verr)
		require.Equal(t, wantNull, isNull, "ord=%d col=%d null", ord, col)
		if !wantNull {
			require.Equal(t, wantVal, v, "ord=%d col=%d val", ord, col)
		}
	}
	check(0, 0, int64(100), false)
	check(0, 1, []byte("active"), false)
	check(1, 0, int64(200), false)
	check(1, 1, nil, true) // NULL varchar
	check(2, 0, nil, true) // NULL int
	check(2, 1, []byte("archived"), false)

	// pk + docLen sections are unaffected by the trailing INCLUDE section.
	require.Equal(t, int64(20), loaded.pk(1))
	require.Equal(t, int32(7), loaded.docLen[2])

	// out-of-range column returns null, not a panic.
	_, isNull, verr := loaded.includeVal(0, 5)
	require.NoError(t, verr)
	require.True(t, isNull)
}

// TestDocmapNoIncludeRoundTrip: a segment with no INCLUDE columns produces a docmap with no
// trailing section (byte-identical to before), and decode leaves the include fields nil.
func TestDocmapNoIncludeRoundTrip(t *testing.T) {
	build := &Segment{
		PkType: int32(types.T_int64),
		pks:    []any{int64(1), int64(2)},
		docLen: []int32{4, 4},
	}
	data, err := build.encodeDocmap()
	require.NoError(t, err)

	loaded := &Segment{}
	require.NoError(t, loaded.decodeDocmap(data))
	require.Equal(t, 0, loaded.nIncludeCols())
	require.Nil(t, loaded.includeTypes)

	v, isNull, err := loaded.includeVal(0, 0)
	require.NoError(t, err)
	require.Nil(t, v)
	require.True(t, isNull)
}

// TestDocmapIncludeMismatchLen: encode rejects a build-side segment whose includeVals count
// does not match the doc count (a builder bug), rather than writing a corrupt section.
func TestDocmapIncludeMismatchLen(t *testing.T) {
	build := &Segment{
		PkType:       int32(types.T_int64),
		pks:          []any{int64(1), int64(2)},
		docLen:       []int32{1, 1},
		includeTypes: []int32{int32(types.T_int64)},
		includeVals:  [][]any{{int64(9)}}, // only 1 row for 2 docs
	}
	_, err := build.encodeDocmap()
	require.Error(t, err)
}

// TestDocmapIncludeFixedNoOffsetTable pins the fixed-width optimization: an all-integer
// INCLUDE index builds NO resident varlena offset table (fixed cols are stride-addressed),
// while an index with a varchar col builds an offset table only for the varlena column(s).
func TestDocmapIncludeFixedNoOffsetTable(t *testing.T) {
	// All fixed (int64 + int32): zero varlena offsets.
	allFixed := &Segment{
		PkType:       int32(types.T_int64),
		pks:          []any{int64(1), int64(2)},
		docLen:       []int32{1, 1},
		includeTypes: []int32{int32(types.T_int64), int32(types.T_int32)},
		includeVals:  [][]any{{int64(10), int32(100)}, {nil, int32(200)}},
	}
	data, err := allFixed.encodeDocmap()
	require.NoError(t, err)
	loaded := &Segment{}
	require.NoError(t, loaded.decodeDocmap(data))
	require.Nil(t, loaded.includeVarOffsets, "numeric-only INCLUDE index must build no offset table")
	require.Equal(t, 2, loaded.includeLay.nFixed)
	require.Equal(t, 0, loaded.includeLay.nVarlena)
	// values round-trip (incl. NULL int64 at ord1 col0).
	v, isNull, _ := loaded.includeVal(1, 0)
	require.True(t, isNull)
	v, _, _ = loaded.includeVal(1, 1)
	require.Equal(t, int32(200), v)
	v, _, _ = loaded.includeVal(0, 0)
	require.Equal(t, int64(10), v)

	// Mixed (int64 + varchar): offset table sized only for the 1 varlena column.
	mixed := &Segment{
		PkType:       int32(types.T_int64),
		pks:          []any{int64(1), int64(2)},
		docLen:       []int32{1, 1},
		includeTypes: []int32{int32(types.T_int64), int32(types.T_varchar)},
		includeVals:  [][]any{{int64(10), []byte("a")}, {int64(20), nil}},
	}
	data, err = mixed.encodeDocmap()
	require.NoError(t, err)
	lm := &Segment{}
	require.NoError(t, lm.decodeDocmap(data))
	require.Len(t, lm.includeVarOffsets, 2, "offset table sized N*nVarlena = 2*1") // only the varchar col
	require.Equal(t, 1, lm.includeLay.nFixed)
	require.Equal(t, 1, lm.includeLay.nVarlena)
	v, _, _ = lm.includeVal(0, 1)
	require.Equal(t, []byte("a"), v)
	_, isNull, _ = lm.includeVal(1, 1)
	require.True(t, isNull)
}
