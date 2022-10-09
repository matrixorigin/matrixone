// Copyright 2022 Matrix Origin
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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func makeInt64Vector(values []int64, nsp []uint64) *vector.Vector {
	ns := nulls.Build(len(values), nsp...)
	vec := vector.NewWithFixed(types.T_int64.ToType(), values, ns, testutil.TestUtilMp)
	return vec
}

func makeUint64Vector(values []uint64, nsp []uint64) *vector.Vector {
	ns := nulls.Build(len(values), nsp...)
	vec := vector.NewWithFixed(types.T_uint64.ToType(), values, ns, testutil.TestUtilMp)
	return vec
}

func makeFloat64Vector(values []float64, nsp []uint64) *vector.Vector {
	ns := nulls.Build(len(values), nsp...)
	vec := vector.NewWithFixed(types.T_float64.ToType(), values, ns, testutil.TestUtilMp)
	return vec
}

func TestSpaceUint64(t *testing.T) {
	inputVector := makeUint64Vector([]uint64{1, 2, 3, 0, 8000}, []uint64{4})
	proc := testutil.NewProc()
	output, err := SpaceNumber[uint64]([]*vector.Vector{inputVector}, proc)
	require.NoError(t, err)
	require.Equal(t, output.GetString(0), " ")
	require.Equal(t, output.GetString(1), "  ")
	require.Equal(t, output.GetString(2), "   ")
	require.Equal(t, output.GetString(3), "")
	require.True(t, nulls.Contains(output.Nsp, 4))
}

func TestSpaceInt64(t *testing.T) {
	inputVector := makeInt64Vector([]int64{1, 2, 3, 0, -1, 8000}, []uint64{4})
	proc := testutil.NewProc()
	output, err := SpaceNumber[int64]([]*vector.Vector{inputVector}, proc)
	require.NoError(t, err)
	require.Equal(t, output.GetString(0), " ")
	require.Equal(t, output.GetString(1), "  ")
	require.Equal(t, output.GetString(2), "   ")
	require.Equal(t, output.GetString(3), "")
	// XXX should have failed instead returning null
	require.True(t, nulls.Contains(output.Nsp, 4))
}

func TestSpaceFloat64(t *testing.T) {
	inputVector := makeFloat64Vector([]float64{1.4, 1.6, 3.3, 0, -1, 8000}, []uint64{4})
	proc := testutil.NewProc()
	output, err := SpaceNumber[float64]([]*vector.Vector{inputVector}, proc)
	require.NoError(t, err)
	require.Equal(t, output.GetString(0), " ")
	require.Equal(t, output.GetString(1), " ")
	require.Equal(t, output.GetString(2), "   ")
	require.Equal(t, output.GetString(3), "")
	// XXX should have failed instead returning null
	require.True(t, nulls.Contains(output.Nsp, 4))
}
