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

package function

import (
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONRowStreamsRowsAndResetsAfterError(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcessWithMPool(t, "", mp)

	ints := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		ints,
		[]int64{1, 2, 3},
		[]bool{false, true, false},
		mp,
	))
	defer ints.Free(mp)
	strings := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytesList(
		strings,
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		nil,
		mp,
	))
	defer strings.Free(mp)
	bools := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixedList(
		bools,
		[]bool{true, false, true},
		nil,
		mp,
	))
	defer bools.Free(mp)
	uints := vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixedList(
		uints,
		[]uint64{^uint64(0), 2, 3},
		nil,
		mp,
	))
	defer uints.Free(mp)

	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	defer result.Free()
	op := newOpBuiltInJsonRow()
	require.NoError(t, result.PreExtendAndReset(3))
	require.NoError(t, op.jsonRow(
		[]*vector.Vector{ints, strings, bools, uints},
		result,
		proc,
		3,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}},
	))
	out := result.GetResultVector()
	require.Equal(
		t,
		[]byte(`[1,"a",true,18446744073709551615]`),
		out.GetBytesAt(0),
	)
	require.True(t, out.IsNull(1))
	require.Equal(t, []byte(`[3,"c",true,3]`), out.GetBytesAt(2))

	binary := vector.NewVec(types.T_binary.ToType())
	require.NoError(t, vector.AppendBytesList(binary, [][]byte{[]byte("x")}, nil, mp))
	defer binary.Free(mp)
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, op.jsonRow(
		[]*vector.Vector{binary},
		result,
		proc,
		1,
		nil,
	))
	for _, column := range op.columns {
		require.Nil(t, column)
	}
	require.Zero(t, op.enc.w.Len())

	require.NoError(t, result.PreExtendAndReset(3))
	require.NoError(t, op.jsonRow(
		[]*vector.Vector{ints, strings},
		result,
		proc,
		3,
		nil,
	))
	out = result.GetResultVector()
	require.Equal(t, []byte(`[1,"a"]`), out.GetBytesAt(0))
	require.Equal(t, []byte(`[null,"b"]`), out.GetBytesAt(1))
	require.Equal(t, []byte(`[3,"c"]`), out.GetBytesAt(2))
}

func newJSONRowBenchmarkParameters(
	b *testing.B,
	mp *mpool.MPool,
	rows int,
) []*vector.Vector {
	b.Helper()
	ints := vector.NewVec(types.T_int64.ToType())
	intValues := make([]int64, rows)
	for i := range intValues {
		intValues[i] = int64(i)
	}
	require.NoError(b, vector.AppendFixedList(ints, intValues, nil, mp))

	strings := vector.NewVec(types.T_varchar.ToType())
	stringValues := make([][]byte, rows)
	for i := range stringValues {
		stringValues[i] = []byte("value-" + strconv.Itoa(i%100))
	}
	require.NoError(b, vector.AppendBytesList(strings, stringValues, nil, mp))
	return []*vector.Vector{ints, strings}
}

func BenchmarkJSONRowFreshOperator8192(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcessWithMPool(b, "", mp)
	params := newJSONRowBenchmarkParameters(b, mp, 8192)
	defer params[0].Free(mp)
	defer params[1].Free(mp)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	defer result.Free()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, result.PreExtendAndReset(8192))
		require.NoError(b, newOpBuiltInJsonRow().jsonRow(
			params,
			result,
			proc,
			8192,
			nil,
		))
	}
}

func BenchmarkJSONRowReusedOperator8192(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	proc := testutil.NewProcessWithMPool(b, "", mp)
	params := newJSONRowBenchmarkParameters(b, mp, 8192)
	defer params[0].Free(mp)
	defer params[1].Free(mp)
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	defer result.Free()
	op := newOpBuiltInJsonRow()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, result.PreExtendAndReset(8192))
		require.NoError(b, op.jsonRow(params, result, proc, 8192, nil))
	}
}
