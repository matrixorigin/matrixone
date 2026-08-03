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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestTextMinMaxUsesGeneralCICollation(t *testing.T) {
	values := []string{"a", "b", "c", "E", "C", "D"}
	testCases := []struct {
		name   string
		oid    types.T
		aggID  int64
		expect string
	}{
		{name: "char min", oid: types.T_char, aggID: AggIdOfMin, expect: "a"},
		{name: "char max", oid: types.T_char, aggID: AggIdOfMax, expect: "E"},
		{name: "varchar min", oid: types.T_varchar, aggID: AggIdOfMin, expect: "a"},
		{name: "varchar max", oid: types.T_varchar, aggID: AggIdOfMax, expect: "E"},
		{name: "text min", oid: types.T_text, aggID: AggIdOfMin, expect: "a"},
		{name: "text max", oid: types.T_text, aggID: AggIdOfMax, expect: "E"},
		{name: "binary min", oid: types.T_binary, aggID: AggIdOfMin, expect: "C"},
		{name: "binary max", oid: types.T_binary, aggID: AggIdOfMax, expect: "c"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.New(tc.oid, 10, 0)
			vec := vector.NewVec(typ)
			for _, value := range values {
				require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
			}
			defer vec.Free(mp)

			agg := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.expect, string(results[0].GetBytesAt(0)))

			agg.Free()
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}

func TestTextMinMaxGeneralCIEquivalence(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newTextMinMaxExec(
		mp, AggIdOfMin, true, types.New(types.T_varchar, 10, 0),
	).(*minMaxExecBytes)
	defer exec.Free()

	require.Zero(t, exec.comp([]byte("A"), []byte("a")))
	require.Zero(t, exec.comp([]byte("å"), []byte("a")))
	require.Zero(t, exec.comp([]byte("ａ"), []byte("a")))
	require.Zero(t, exec.comp([]byte("a "), []byte("a")))
}

func TestTextMinMaxGeneralCIMerge(t *testing.T) {
	testCases := []struct {
		name   string
		aggID  int64
		expect string
	}{
		{name: "min", aggID: AggIdOfMin, expect: "a"},
		{name: "max", aggID: AggIdOfMax, expect: "E"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.New(types.T_varchar, 10, 0)
			newInput := func(values ...string) *vector.Vector {
				vec := vector.NewVec(typ)
				for _, value := range values {
					require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
				}
				return vec
			}

			leftVec := newInput("C", "E")
			rightVec := newInput("a", "c")
			defer leftVec.Free(mp)
			defer rightVec.Free(mp)

			left := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			right := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			defer left.Free()
			defer right.Free()

			require.NoError(t, left.GroupGrow(1))
			require.NoError(t, right.GroupGrow(1))
			require.NoError(t, left.BulkFill(0, []*vector.Vector{leftVec}))
			require.NoError(t, right.BulkFill(0, []*vector.Vector{rightVec}))
			require.NoError(t, left.Merge(right, 0, 0))

			results, err := left.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(mp)
			require.Equal(t, tc.expect, string(results[0].GetBytesAt(0)))
		})
	}
}

func mustParseAggDecimal256(t *testing.T, value string, scale int32) types.Decimal256 {
	t.Helper()
	dec, err := types.ParseDecimal256(value, 65, scale)
	require.NoError(t, err)
	return dec
}

func TestDecimal256MinMax(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 65, 4)
	values := []types.Decimal256{
		mustParseAggDecimal256(t, "12.3412", 4),
		mustParseAggDecimal256(t, "-9.8765", 4),
		mustParseAggDecimal256(t, "7.7777", 4),
	}

	vec := vector.NewVec(typ)
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	defer vec.Free(mp)

	testCases := []struct {
		name   string
		aggID  int64
		expect types.Decimal256
	}{
		{
			name:   "min",
			aggID:  AggIdOfMin,
			expect: mustParseAggDecimal256(t, "-9.8765", 4),
		},
		{
			name:   "max",
			aggID:  AggIdOfMax,
			expect: mustParseAggDecimal256(t, "12.3412", 4),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[types.Decimal256](results[0])[0])

			agg.Free()
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}
