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

package vector

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func testPayloadBytes(vec *Vector) int {
	total := 0
	for row := 0; row < vec.Length(); row++ {
		if !vec.IsNull(uint64(row)) {
			if size := len(vec.GetBytesAt(row)); size > types.VarlenaInlineSize {
				total += size
			}
		}
	}
	return total
}

func TestUnionWholeWithPayloadSize(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_varchar.ToType()
	source := NewVec(typ)
	defer source.Free(mp)
	for i := 0; i < 8; i++ {
		value := fmt.Sprintf("row-%d-%s", i, strings.Repeat("x", 128))
		if i == 1 {
			value = "short"
		}
		require.NoError(t, AppendBytes(source, []byte(value), i == 3, mp))
	}
	require.NoError(t, source.Shuffle([]int64{7, 6, 5, 4, 3, 2, 1, 0}, mp))
	headers := MustFixedColNoTypeCheck[types.Varlena](source)
	require.NoError(t, SetFixedAtNoTypeCheck(source, 1, headers[0]))
	source.GetGrouping().Add(2)
	window, err := source.Window(1, 7)
	require.NoError(t, err)
	defer window.Free(mp)

	for _, tc := range []struct {
		name string
		src  *Vector
	}{
		{"aliased and reordered", source},
		{"borrowed window with dead area", window},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinary, hinted := NewVec(typ), NewVec(typ)
			defer ordinary.Free(mp)
			defer hinted.Free(mp)
			prefix := []byte(strings.Repeat("p", 80))
			require.NoError(t, AppendBytes(ordinary, prefix, false, mp))
			require.NoError(t, AppendBytes(hinted, prefix, false, mp))
			require.NoError(t, ordinary.UnionBatch(tc.src, 0, tc.src.Length(), nil, mp))
			require.NoError(t, hinted.UnionWholeWithPayloadSize(tc.src, tc.src.Length(), testPayloadBytes(tc.src), mp))
			require.Equal(t, ordinary.Length(), hinted.Length())
			for i := 0; i < ordinary.Length(); i++ {
				require.Equal(t, ordinary.IsNull(uint64(i)), hinted.IsNull(uint64(i)))
				require.Equal(t, ordinary.GetGrouping().Contains(uint64(i)), hinted.GetGrouping().Contains(uint64(i)))
				if !ordinary.IsNull(uint64(i)) {
					require.Equal(t, ordinary.GetBytesAt(i), hinted.GetBytesAt(i))
				}
			}
		})
	}

	// A self-append must clone an overlapping source before either buffer grows.
	ordinary, err := source.Dup(mp)
	require.NoError(t, err)
	defer ordinary.Free(mp)
	hinted, err := source.Dup(mp)
	require.NoError(t, err)
	defer hinted.Free(mp)
	count := ordinary.Length()
	require.NoError(t, ordinary.UnionBatch(ordinary, 0, count, nil, mp))
	require.NoError(t, hinted.UnionWholeWithPayloadSize(hinted, count, testPayloadBytes(hinted), mp))
	for i := 0; i < ordinary.Length(); i++ {
		require.Equal(t, ordinary.IsNull(uint64(i)), hinted.IsNull(uint64(i)))
		if !ordinary.IsNull(uint64(i)) {
			require.Equal(t, ordinary.GetBytesAt(i), hinted.GetBytesAt(i))
		}
	}

	// Invalid hints and sources outside the full non-constant contract fall back.
	partial := NewVec(typ)
	defer partial.Free(mp)
	require.NoError(t, partial.UnionWholeWithPayloadSize(source, 2, 0, mp))
	require.Equal(t, source.GetBytesAt(0), partial.GetBytesAt(0))
	constant, err := NewConstBytes(typ, []byte(strings.Repeat("c", 80)), 1, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	constantDst := NewVec(typ)
	defer constantDst.Free(mp)
	require.NoError(t, constantDst.UnionWholeWithPayloadSize(constant, 4, 0, mp))
	for i := 0; i < 4; i++ {
		require.Equal(t, constant.GetBytesAt(0), constantDst.GetBytesAt(i))
	}
	// An impossible hint cannot overflow destination length or trigger an
	// artificial allocation failure; it falls back to measuring the source.
	require.NoError(t, hinted.UnionWholeWithPayloadSize(source, source.Length(), math.MaxInt, mp))
	require.ErrorContains(t, pregrowVarlenaArea(hinted, math.MaxInt, mp), "varlena area size exceeds supported range")
}

func TestUnionWholeWithPayloadSizeAccountFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_varchar.ToType()
	source := NewVec(typ)
	defer source.Free(mp)
	for i := 0; i < 2; i++ {
		require.NoError(t, AppendBytes(source, []byte(strings.Repeat("x", 800<<10)), false, mp))
	}
	require.NoError(t, source.Shuffle([]int64{1, 0}, mp))
	for _, hint := range []bool{false, true} {
		t.Run(fmt.Sprintf("hint=%t", hint), func(t *testing.T) {
			account := newTestVectorAllocationAccount(t, 1<<20, 16)
			dst := newAccountedTestVector(t, typ, account.selection)
			var err error
			if hint {
				err = dst.UnionWholeWithPayloadSize(source, source.Length(), testPayloadBytes(source), mp)
			} else {
				err = dst.UnionBatch(source, 0, source.Length(), nil, mp)
			}
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			dst.Free(mp)
			finalizeTestVectorAllocationAccount(t, account)
		})
	}
}
