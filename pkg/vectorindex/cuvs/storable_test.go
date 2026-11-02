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

package cuvs

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func requireCode(t *testing.T, err error, code uint16) {
	t.Helper()
	require.Error(t, err)
	got, ok := moerr.GetMoErrCode(err)
	require.True(t, ok, err.Error())
	require.Equal(t, code, got, err.Error())
}

func TestCheckStorable(t *testing.T) {
	require.NoError(t, CheckStorable([]float32{1, -2, 3}, false))
	require.NoError(t, CheckStorable([]float32{1, -2, 3}, true))

	// float16 storage: the largest value that rounds to a finite float16 is stored
	require.NoError(t, CheckStorable([]float32{65519.996, 1}, true))
	requireCode(t, CheckStorable([]float32{65520, 1}, true), moerr.ErrIndexVectorOutOfRange)
	requireCode(t, CheckStorable([]float32{1, -100000}, true), moerr.ErrIndexVectorOutOfRange)
	// float32 storage keeps it
	require.NoError(t, CheckStorable([]float32{100000, 1}, false))

	// the squared norm bounds every storage type
	limit := float32(math.Sqrt(MaxSquaredNorm))
	require.NoError(t, CheckStorable([]float32{limit * 0.999}, false))
	requireCode(t, CheckStorable([]float32{1e20, 1}, false), moerr.ErrIndexVectorOutOfRange)
	// two elements each within the bound whose squares sum past it
	requireCode(t, CheckStorable([]float32{limit * 0.8, limit * 0.8}, false), moerr.ErrIndexVectorOutOfRange)

	requireCode(t, CheckStorable([]float32{float32(math.NaN())}, false), moerr.ErrIndexVectorNonFinite)
	requireCode(t, CheckStorable([]float32{float32(math.Inf(-1))}, true), moerr.ErrIndexVectorNonFinite)
	requireCode(t, CheckStorable([]float32{}, false), moerr.ErrIndexVectorDimension)

	// a vecf16 base holds no value either rule rejects
	require.NoError(t, CheckStorable([]types.Float16{types.Float16FromFloat32(65504), types.Float16FromFloat32(-65504)}, false))
	requireCode(t, CheckStorable([]types.Float16{types.Float16FromFloat32(float32(math.Inf(1)))}, false), moerr.ErrIndexVectorNonFinite)
}

// A query vector the index cannot store is rejected.
func TestCheckQuery(t *testing.T) {
	require.NoError(t, CheckQuery(types.ArrayToBytes([]float32{1, 2}), types.T_array_float32, true))
	requireCode(t, CheckQuery(types.ArrayToBytes([]float32{100000, 2}), types.T_array_float32, true), moerr.ErrIndexVectorOutOfRange)
	require.NoError(t, CheckQuery(types.ArrayToBytes([]float32{100000, 2}), types.T_array_float32, false))
	requireCode(t, CheckQuery(types.ArrayToBytes([]float32{1e20, 2}), types.T_array_float32, false), moerr.ErrIndexVectorOutOfRange)
	require.NoError(t, CheckQuery(types.ArrayToBytes([]types.Float16{types.Float16FromFloat32(65504)}), types.T_array_float16, false))
	requireCode(t, CheckQuery(nil, types.T_array_float32, false), moerr.ErrIndexVectorDimension)
}
