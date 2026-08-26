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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// boxfreeSeg builds a 4-doc segment with two INCLUDE columns (int64, varchar) and NULLs
// arranged so both the "first value is NULL after non-nulls" (backfill) and "non-null after
// NULL" (parallel append) branches of cbAppendNull/cbAppendVal are exercised:
//
//	col0 int64:   [100,  NULL, 300, NULL]
//	col1 varchar: ["active", "b", NULL, NULL]
func boxfreeSeg() *Segment {
	return &Segment{
		PkType:       int32(types.T_int64),
		pks:          []any{int64(1), int64(2), int64(3), int64(4)},
		docLen:       []int32{1, 1, 1, 1},
		includeTypes: []int32{int32(types.T_int64), int32(types.T_varchar)},
		includeVals: [][]any{
			{int64(100), []byte("active")},
			{nil, []byte("b")},
			{int64(300), nil},
			{nil, nil},
		},
	}
}

// TestAppendIncludeToBoxFree pins the box-free covered-output path (F4): appendIncludeTo
// encodes each INCLUDE column into a nullable ColumnBuffer without boxing, and
// AppendColumnBuffer decodes it straight into a typed vector preserving values AND NULLs.
// Runs on BOTH a build-side segment (includeVals resident) and a loaded segment
// (includeRaw / offset tables), which take the two arms of appendIncludeTo.
func TestAppendIncludeToBoxFree(t *testing.T) {
	mp := mpool.MustNewZero()

	run := func(t *testing.T, seg *Segment) {
		// col0: int64 with NULLs at ord 1 and 3.
		k0 := &vectorindex.ColumnBuffer{Type: types.T_int64}
		for ord := int64(0); ord < 4; ord++ {
			require.NoError(t, seg.appendIncludeTo(k0, 0, ord))
		}
		require.Equal(t, 4, k0.N)
		require.Equal(t, []bool{false, true, false, true}, k0.Nulls)
		vec0 := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vectorindex.AppendColumnBuffer(k0, vec0, mp))
		require.Equal(t, 4, vec0.Length())
		vals0 := vector.MustFixedColWithTypeCheck[int64](vec0)
		require.Equal(t, int64(100), vals0[0])
		require.Equal(t, int64(300), vals0[2])
		require.False(t, vec0.IsNull(0))
		require.True(t, vec0.IsNull(1))
		require.False(t, vec0.IsNull(2))
		require.True(t, vec0.IsNull(3))

		// col1: varchar with NULLs at ord 2 and 3 (first NULL comes AFTER two non-nulls,
		// exercising the cbAppendNull backfill of the prior all-non-null Nulls slice).
		k1 := &vectorindex.ColumnBuffer{Type: types.T_varchar}
		for ord := int64(0); ord < 4; ord++ {
			require.NoError(t, seg.appendIncludeTo(k1, 1, ord))
		}
		require.Equal(t, []bool{false, false, true, true}, k1.Nulls)
		vec1 := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vectorindex.AppendColumnBuffer(k1, vec1, mp))
		require.Equal(t, "active", vec1.GetStringAt(0))
		require.Equal(t, "b", vec1.GetStringAt(1))
		require.False(t, vec1.IsNull(0))
		require.False(t, vec1.IsNull(1))
		require.True(t, vec1.IsNull(2))
		require.True(t, vec1.IsNull(3))
	}

	t.Run("build-side", func(t *testing.T) { run(t, boxfreeSeg()) })

	t.Run("loaded-side", func(t *testing.T) {
		data, err := boxfreeSeg().encodeDocmap()
		require.NoError(t, err)
		loaded := &Segment{}
		require.NoError(t, loaded.decodeDocmap(data))
		run(t, loaded)
	})
}

// TestColumnBufferAllNonNull: a column with NO NULLs never materializes k.Nulls (stays nil,
// so AppendColumnBuffer's nul(i) is always false — the pk path is byte-for-byte unchanged).
func TestColumnBufferAllNonNull(t *testing.T) {
	mp := mpool.MustNewZero()
	seg := boxfreeSeg()
	k := &vectorindex.ColumnBuffer{Type: types.T_varchar}
	// ords 0,1 have non-null varchar; append only those.
	require.NoError(t, seg.appendIncludeTo(k, 1, 0))
	require.NoError(t, seg.appendIncludeTo(k, 1, 1))
	require.Nil(t, k.Nulls, "no NULL seen -> Nulls stays nil")
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vectorindex.AppendColumnBuffer(k, vec, mp))
	require.False(t, vec.IsNull(0))
	require.False(t, vec.IsNull(1))
}

// TestIncludeRawAtBounds pins the belt-and-suspenders bounds (F3): a malformed / truncated
// loaded segment makes includeRawAt (and thus includeVal / appendIncludeTo) return an error
// instead of panicking on an out-of-range slice.
func TestIncludeRawAtBounds(t *testing.T) {
	data, err := boxfreeSeg().encodeDocmap()
	require.NoError(t, err)

	// Fixed column (col0 int64): truncate includeRaw so the stride-computed row overruns.
	trunc := &Segment{}
	require.NoError(t, trunc.decodeDocmap(data))
	trunc.includeRaw = trunc.includeRaw[:2]
	_, _, err = trunc.includeRawAt(3, 0)
	require.Error(t, err)
	_, _, err = trunc.includeVal(3, 0)
	require.Error(t, err)
	require.Error(t, trunc.appendIncludeTo(&vectorindex.ColumnBuffer{Type: types.T_int64}, 0, 3))

	// Varlena column (col1 varchar): corrupt the offset table so it points past includeRaw.
	badoff := &Segment{}
	require.NoError(t, badoff.decodeDocmap(data))
	for i := range badoff.includeVarOffsets {
		badoff.includeVarOffsets[i] = int32(len(badoff.includeRaw) + 1000)
	}
	_, _, err = badoff.includeRawAt(0, 1)
	require.Error(t, err)
	require.Error(t, badoff.appendIncludeTo(&vectorindex.ColumnBuffer{Type: types.T_varchar}, 1, 0))

	// Out-of-range column / negative ord are rejected, not panicked.
	good := &Segment{}
	require.NoError(t, good.decodeDocmap(data))
	_, _, err = good.includeRawAt(0, 99)
	require.Error(t, err)
	_, _, err = good.includeRawAt(-1, 0)
	require.Error(t, err)
}
