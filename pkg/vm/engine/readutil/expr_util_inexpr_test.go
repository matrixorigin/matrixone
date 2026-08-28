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

package readutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Zone-map pruning binary-searches this payload, so it must leave here ordered
// and flagged -- otherwise the consumer refuses to prune and the caller silently
// loses block filtering.
func TestConstructInExprPublishesOrderedPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_int64.ToType())
	defer src.Free(mp)
	for _, n := range []int64{30, 10, 20} {
		require.NoError(t, vector.AppendFixed(src, n, false, mp))
	}
	require.False(t, src.GetSorted())

	expr, err := ConstructInExpr(context.Background(), "pk", src)
	require.NoError(t, err)
	literal := expr.GetF().Args[1].GetVec()
	require.NotNil(t, literal)

	got := vector.NewVec(types.T_any.ToType())
	defer got.Free(mp)
	require.NoError(t, got.UnmarshalBinary(literal.Data))

	require.True(t, got.GetSorted(), "payload must be flagged so the consumer can prune")
	require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](got))
	require.Equal(t, int32(got.Length()), literal.Len)
}

// The caller's vector must be untouched: disttae's transfer pairs searchPKColumn
// with searchEntryPos and searchBatPos by index, so reordering it in place would
// mis-associate rows.
func TestConstructInExprDoesNotReorderCallerVector(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_int64.ToType())
	defer src.Free(mp)
	for _, n := range []int64{30, 10, 20} {
		require.NoError(t, vector.AppendFixed(src, n, false, mp))
	}

	_, err := ConstructInExpr(context.Background(), "pk", src)
	require.NoError(t, err)

	require.Equal(t, []int64{30, 10, 20}, vector.MustFixedColWithTypeCheck[int64](src),
		"caller's vector was reordered; positional pairings would break")
	require.False(t, src.GetSorted(), "caller's flag must not be set either")
}

// Duplicates are compacted, so the published Len must describe the payload.
func TestConstructInExprLenMatchesCompactedPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_int64.ToType())
	defer src.Free(mp)
	for _, n := range []int64{7, 3, 7, 3, 5} {
		require.NoError(t, vector.AppendFixed(src, n, false, mp))
	}

	expr, err := ConstructInExpr(context.Background(), "pk", src)
	require.NoError(t, err)
	literal := expr.GetF().Args[1].GetVec()

	got := vector.NewVec(types.T_any.ToType())
	defer got.Free(mp)
	require.NoError(t, got.UnmarshalBinary(literal.Data))
	require.Equal(t, []int64{3, 5, 7}, vector.MustFixedColWithTypeCheck[int64](got))
	require.Equal(t, int32(3), literal.Len)
}

func TestNormalizeInPayloadOrdersAndFlags(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_int64.ToType())
	defer src.Free(mp)
	for _, n := range []int64{9, 2, 9, 5} {
		require.NoError(t, vector.AppendFixed(src, n, false, mp))
	}
	data, err := src.MarshalBinary()
	require.NoError(t, err)

	out, length, err := normalizeInPayload(data)
	require.NoError(t, err)
	require.Equal(t, 3, length, "duplicates are compacted")

	got := vector.NewVec(types.T_any.ToType())
	defer got.Free(mp)
	require.NoError(t, got.UnmarshalBinary(out))
	require.True(t, got.GetSorted())
	require.Equal(t, []int64{2, 5, 9}, vector.MustFixedColWithTypeCheck[int64](got))
}

// A payload that cannot be decoded must surface as an error, not be published as
// a filter that would then prune against garbage.
func TestNormalizeInPayloadRejectsCorruptData(t *testing.T) {
	for _, bad := range [][]byte{nil, {}, []byte("not a marshalled vector")} {
		_, _, err := normalizeInPayload(bad)
		require.Error(t, err, "corrupt payload len=%d must be reported", len(bad))
	}
}

// InplaceSortAndCompact permutes only the value column and drops NULLs outright
// when compaction fires, so a nullable payload must bypass it: the rows have to
// survive intact even though that costs the sorted flag, and therefore pruning.
// Unreachable from today's PK-derived callers -- this pins the guard for the next
// caller, which would otherwise publish a filter with its NULLs silently removed.
func TestConstructInExprPreservesNullablePayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_int64.ToType())
	defer src.Free(mp)
	// The duplicate matters: without it compaction never fires and only the
	// misalignment shows. With it, an unguarded sort rebuilds the vector through
	// appendList(..., nil) and the NULL disappears entirely.
	require.NoError(t, vector.AppendFixed(src, int64(30), false, mp))
	require.NoError(t, vector.AppendFixed(src, int64(0), true, mp))
	require.NoError(t, vector.AppendFixed(src, int64(30), false, mp))
	require.NoError(t, vector.AppendFixed(src, int64(10), false, mp))

	expr, err := ConstructInExpr(context.Background(), "pk", src)
	require.NoError(t, err)
	literal := expr.GetF().Args[1].GetVec()
	require.NotNil(t, literal)

	got := vector.NewVec(types.T_any.ToType())
	defer got.Free(mp)
	require.NoError(t, got.UnmarshalBinary(literal.Data))

	require.Equal(t, 4, got.Length(), "the NULL row must not be dropped")
	require.True(t, got.GetNulls().Contains(1), "the NULL must stay at its original position")
	require.False(t, got.GetSorted(), "a nullable payload cannot claim sorted order")
	require.Equal(t, int32(4), literal.Len)

	col := vector.MustFixedColWithTypeCheck[int64](got)
	require.Equal(t, int64(30), col[0], "values must keep their bitmap-aligned order")
	require.Equal(t, int64(30), col[2])
	require.Equal(t, int64(10), col[3])
}
