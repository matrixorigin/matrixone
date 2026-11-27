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

package aggexec

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFirstRowMergePreservesWholeNullableTuple(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	a, b := vector.NewVec(types.T_int32.ToType()), vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { a.Free(mp); b.Free(mp) })
	require.NoError(t, vector.AppendFixed(a, int32(0), true, mp))
	require.NoError(t, vector.AppendFixed(a, int32(1), false, mp))
	require.NoError(t, vector.AppendFixed(b, int32(2), false, mp))
	require.NoError(t, vector.AppendFixed(b, int32(0), true, mp))

	// Each partial has one of the two possible source rows. Round-trip the
	// partial states before merging, as the group spill path does.
	partial := func(input *vector.Vector, row int) *firstRowExec {
		exec := makeFirstRowExec(mp, AggIdOfFirstRow, types.T_int32.ToType()).(*firstRowExec)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.Fill(0, row, []*vector.Vector{input}))
		var buf bytes.Buffer
		require.NoError(t, exec.SaveIntermediateResultOfChunk(0, &buf))
		restored := makeFirstRowExec(mp, AggIdOfFirstRow, types.T_int32.ToType()).(*firstRowExec)
		require.NoError(t, restored.UnmarshalFromReader(&buf, mp))
		exec.Free()
		t.Cleanup(restored.Free)
		return restored
	}
	firstA, secondA := partial(a, 0), partial(a, 1)
	firstB, secondB := partial(b, 0), partial(b, 1)
	for _, tc := range []struct {
		name     string
		leftA    *firstRowExec
		rightA   *firstRowExec
		leftB    *firstRowExec
		rightB   *firstRowExec
		firstRow bool
	}{
		{"first partial", firstA, secondA, firstB, secondB, true},
		{"second partial", secondA, firstA, secondB, firstB, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outA := makeFirstRowExec(mp, AggIdOfFirstRow, types.T_int32.ToType()).(*firstRowExec)
			outB := makeFirstRowExec(mp, AggIdOfFirstRow, types.T_int32.ToType()).(*firstRowExec)
			defer outA.Free()
			defer outB.Free()
			require.NoError(t, outA.GroupGrow(1))
			require.NoError(t, outB.GroupGrow(1))
			require.NoError(t, outA.Merge(tc.leftA, 0, 0))
			require.NoError(t, outB.Merge(tc.leftB, 0, 0))
			require.NoError(t, outA.Merge(tc.rightA, 0, 0))
			require.NoError(t, outB.Merge(tc.rightB, 0, 0))
			va, err := outA.Flush()
			require.NoError(t, err)
			vb, err := outB.Flush()
			require.NoError(t, err)
			defer va[0].Free(mp)
			defer vb[0].Free(mp)
			if tc.firstRow {
				require.True(t, va[0].IsNull(0))
				require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](vb[0], 0))
			} else {
				require.Equal(t, int32(1), vector.GetFixedAtNoTypeCheck[int32](va[0], 0))
				require.True(t, vb[0].IsNull(0))
			}
		})
	}
}

func TestFirstRowPreservesPhysicalType(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	for _, oid := range []types.T{types.T_geometry, types.T_geometry32, types.T_array_float32} {
		t.Run(oid.String(), func(t *testing.T) {
			typ := oid.ToType()
			input := vector.NewVec(typ)
			defer input.Free(mp)
			payload := []byte("physical-value")
			require.NoError(t, vector.AppendBytes(input, payload, false, mp))
			exec := makeFirstRowExec(mp, AggIdOfFirstRow, typ).(*firstRowExec)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
			result, err := exec.Flush()
			require.NoError(t, err)
			defer result[0].Free(mp)
			require.Equal(t, typ, *result[0].GetType())
			require.Equal(t, payload, result[0].GetRawBytesAt(0))
		})
	}
}
