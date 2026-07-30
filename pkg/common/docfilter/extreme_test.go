// Copyright 2022 Matrix Origin
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

package docfilter

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// TestCbitmapExtremeValues is a destructive guard against the "empty exact
// filter for negative / max integer PKs" bug class: a value that zero-extends to
// a huge uint64 (negatives, math.MinInt64, math.MaxUint64) must NEVER produce a
// cbitmap with nbits==0 (which would make existing keys false negatives). Each
// case must either build a correct dense cbitmap (single value or bounded span)
// or fall back to an exact CRoaring — and membership must stay exact either way.
// It also pins the MaxCbitmapBits feasibility boundary.
func TestCbitmapExtremeValues(t *testing.T) {
	mp := mpool.MustNewZero()

	// Feasibility helper must not wrap at the top of the uint64 range.
	require.False(t, CbitmapFeasible(math.MaxUint64), "MaxUint64 must be infeasible (no +1 wrap)")
	require.True(t, CbitmapFeasible(MaxCbitmapBits-1))
	require.False(t, CbitmapFeasible(MaxCbitmapBits))

	// check builds via the production docfilter.Build (tag + payload), rebuilds
	// with New, and asserts the filter is exact: every present value matches
	// (single + vector), every absent value does not, and the routing tag is as
	// expected (an over-cap range must fall back to CRoaring, not build empty).
	check := func(name string, present, absent *vector.Vector, wantTag byte) {
		t.Run(name, func(t *testing.T) {
			defer present.Free(mp)
			defer absent.Free(mp)

			payload, err := Build(present)
			require.NoError(t, err)
			require.NotEmpty(t, payload)
			require.Equal(t, wantTag, payload[0], "routing tag")

			f, err := New(payload)
			require.NoError(t, err)
			defer f.Free()
			require.True(t, f.Exact(), "integer PK filter must be exact")

			res := f.TestVector(present, nil)
			require.Len(t, res, present.Length())
			for i := 0; i < present.Length(); i++ {
				require.True(t, f.Test(present.GetRawBytesAt(i)), "present row %d (single)", i)
				require.Equal(t, uint8(1), res[i], "present row %d (vector)", i)
			}
			for i := 0; i < absent.Length(); i++ {
				require.False(t, f.Test(absent.GetRawBytesAt(i)), "absent row %d", i)
			}
		})
	}

	i64 := types.T_int64.ToType()
	u64 := types.T_uint64.ToType()

	// Negatives incl. MinInt64: zero-extend to a huge span -> CRoaring (exact).
	check("int64_negatives_minint64",
		buildIntVec(t, mp, i64, []int64{math.MinInt64, -1000, -1}, nil),
		buildIntVec(t, mp, i64, []int64{0, 1, -2, math.MinInt64 + 1}, nil),
		TagCRoaring)

	// MaxUint64 alone: span 0 with the base offset -> a 1-bit dense cbitmap.
	check("uint64_maxuint64_single",
		buildIntVec(t, mp, u64, []uint64{math.MaxUint64}, nil),
		buildIntVec(t, mp, u64, []uint64{0, math.MaxUint64 - 1}, nil),
		TagCbitmap)

	// {0, MaxUint64}: full-width span -> infeasible -> CRoaring (exact).
	check("uint64_full_span",
		buildIntVec(t, mp, u64, []uint64{0, math.MaxUint64}, nil),
		buildIntVec(t, mp, u64, []uint64{1, math.MaxUint64 - 1}, nil),
		TagCRoaring)

	// Boundary: span == MaxCbitmapBits-1 is feasible (dense cbitmap)...
	check("boundary_span_below_cap",
		buildIntVec(t, mp, i64, []int64{0, int64(MaxCbitmapBits) - 1}, nil),
		buildIntVec(t, mp, i64, []int64{1, int64(MaxCbitmapBits) - 2}, nil),
		TagCbitmap)
	// ...span == MaxCbitmapBits is not (falls back to CRoaring).
	check("boundary_span_at_cap",
		buildIntVec(t, mp, i64, []int64{0, int64(MaxCbitmapBits)}, nil),
		buildIntVec(t, mp, i64, []int64{1, int64(MaxCbitmapBits) - 1}, nil),
		TagCRoaring)
}
