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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAvgTwCacheNumericRoundTrip(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	input := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(input, []int32{3, 5}, nil, mp))

	exec := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{input}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	vecs, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, vecs, 1)
	require.False(t, vecs[0].IsNull(0))
	payload := vecs[0].GetBytesAt(0)
	require.Len(t, payload, 16)
	require.Equal(t, 8.0, types.DecodeFloat64(payload[0:]))
	require.Equal(t, int64(2), types.DecodeInt64(payload[8:]))

	vecs[0].Free(mp)
	exec.Free()
	restored.Free()
	input.Free(mp)
}

func TestAvgTwResultFloatRoundTrip(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	input := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(input, []int32{2, 4, 6}, nil, mp))

	cacheExec := newAvgTwCacheNumericExec[int32](mp, 1, types.T_int32.ToType())
	require.NoError(t, cacheExec.GroupGrow(1))
	require.NoError(t, cacheExec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{input}))
	cacheVecs, err := cacheExec.Flush()
	require.NoError(t, err)

	resultExec := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	require.NoError(t, resultExec.GroupGrow(1))
	require.NoError(t, resultExec.BatchFill(0, []uint64{1}, cacheVecs))

	var buf bytes.Buffer
	require.NoError(t, resultExec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored := newAvgTwResultFloatExec(mp, 2, types.T_char.ToType())
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	resultVecs, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, resultVecs, 1)
	require.False(t, resultVecs[0].IsNull(0))
	require.Equal(t, 4.0, vector.GetFixedAtNoTypeCheck[float64](resultVecs[0], 0))

	resultVecs[0].Free(mp)
	cacheVecs[0].Free(mp)
	cacheExec.Free()
	resultExec.Free()
	restored.Free()
	input.Free(mp)
}

func TestAvgTwDecimalRoundTrip(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	typ := types.New(types.T_decimal64, 10, 2)
	v1, err := types.ParseDecimal64("1.10", 10, 2)
	require.NoError(t, err)
	v2, err := types.ParseDecimal64("2.30", 10, 2)
	require.NoError(t, err)

	input := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixedList(input, []types.Decimal64{v1, v2}, nil, mp))

	cacheExec := newAvgTwCacheDecimalExec[types.Decimal64](mp, 1, typ)
	require.NoError(t, cacheExec.GroupGrow(1))
	require.NoError(t, cacheExec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{input}))
	cacheVecs, err := cacheExec.Flush()
	require.NoError(t, err)

	resultExec := newAvgTwResultDecimalExec(mp, 2, AvgTwCacheReturnType([]types.Type{typ}))
	require.NoError(t, resultExec.GroupGrow(1))
	require.NoError(t, resultExec.BatchFill(0, []uint64{1}, cacheVecs))
	resultVecs, err := resultExec.Flush()
	require.NoError(t, err)
	require.Len(t, resultVecs, 1)
	require.False(t, resultVecs[0].IsNull(0))
	require.Equal(t, "1.70000000", vector.GetFixedAtNoTypeCheck[types.Decimal128](resultVecs[0], 0).Format(8))

	resultVecs[0].Free(mp)
	cacheVecs[0].Free(mp)
	cacheExec.Free()
	resultExec.Free()
	input.Free(mp)
}
