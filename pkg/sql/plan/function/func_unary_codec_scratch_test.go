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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCompressUncompressDirectOutput(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	values := []string{
		"",
		"a",
		strings.Repeat("compressible-value-", 4096),
	}
	source := newVectorByType(mp, types.T_blob.ToType(), values, nil)
	defer source.Free(mp)

	compressed := vector.NewFunctionResultWrapper(types.T_blob.ToType(), mp)
	defer compressed.Free()
	require.NoError(t, compressed.PreExtendAndReset(len(values)))
	require.NoError(t, Compress(
		[]*vector.Vector{source},
		compressed,
		proc,
		len(values),
		nil,
	))

	decoded := vector.NewFunctionResultWrapper(types.T_blob.ToType(), mp)
	defer decoded.Free()
	require.NoError(t, decoded.PreExtendAndReset(len(values)))
	require.NoError(t, Uncompress(
		[]*vector.Vector{compressed.GetResultVector()},
		decoded,
		proc,
		len(values),
		nil,
	))
	for row, value := range values {
		require.Equal(t, []byte(value), decoded.GetResultVector().GetBytesAt(row))
	}
}

func TestCompressDirectOutputBound(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	for _, size := range []int{
		0, 1, 15, 16, 17, 127, 128, 255, 256,
		4095, 4096, 4097, 16383, 16384, 16385,
		65534, 65535, 65536, 1 << 20,
	} {
		value := make([]byte, size)
		state := uint64(size) + 1
		for idx := range value {
			state = state*6364136223846793005 + 1442695040888963407
			value[idx] = byte(state >> 56)
		}
		source := newVectorByType(
			mp,
			types.T_blob.ToType(),
			[]string{string(value)},
			nil,
		)
		compressed := vector.NewFunctionResultWrapper(types.T_blob.ToType(), mp)
		require.NoError(t, compressed.PreExtendAndReset(1))
		require.NoErrorf(t, Compress(
			[]*vector.Vector{source},
			compressed,
			proc,
			1,
			nil,
		), "size %d", size)

		decoded := vector.NewFunctionResultWrapper(types.T_blob.ToType(), mp)
		require.NoError(t, decoded.PreExtendAndReset(1))
		require.NoErrorf(t, Uncompress(
			[]*vector.Vector{compressed.GetResultVector()},
			decoded,
			proc,
			1,
			nil,
		), "size %d", size)
		require.Equalf(t, value, decoded.GetResultVector().GetBytesAt(0), "size %d", size)
		decoded.Free()
		compressed.Free()
		source.Free(mp)
	}
}

func TestFromBase64DirectOutputNullAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	source := vector.NewVec(types.T_varchar.ToType())
	defer source.Free(mp)
	require.NoError(t, vector.AppendBytes(source, []byte("YWJj"), false, mp))
	require.NoError(t, vector.AppendBytes(source, nil, true, mp))
	require.NoError(t, vector.AppendBytes(source, []byte("ZGVm"), false, mp))

	result := vector.NewFunctionResultWrapper(types.T_blob.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(3))
	require.NoError(t, FromBase64(
		[]*vector.Vector{source},
		result,
		proc,
		3,
		&FunctionSelectList{
			AnyNull:    true,
			SelectList: []bool{true, true, false},
		},
	))
	require.Equal(t, []byte("abc"), result.GetResultVector().GetBytesAt(0))
	require.True(t, result.GetResultVector().IsNull(1))
	require.True(t, result.GetResultVector().IsNull(2))
}

func TestRandomBytesDirectOutput(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	source := newVectorByType(
		mp,
		types.T_int64.ToType(),
		[]int64{1, 1024, 0},
		nil,
	)
	defer source.Free(mp)

	result := vector.NewFunctionResultWrapper(types.T_blob.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(3))
	require.NoError(t, RandomBytes(
		[]*vector.Vector{source},
		result,
		proc,
		3,
		nil,
	))
	require.Len(t, result.GetResultVector().GetBytesAt(0), 1)
	require.Len(t, result.GetResultVector().GetBytesAt(1), 1024)
	require.True(t, result.GetResultVector().IsNull(2))
}
