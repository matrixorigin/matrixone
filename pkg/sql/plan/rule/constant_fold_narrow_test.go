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

package rule

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Narrow vector constants must fold to a VecVal literal (carrying the raw bytes),
// like float32/float64 — else the ivfflat narrow ORDER BY pushdown can't fold the
// query and the const executor nil-panics on materialization.
func TestGetConstantValueNarrowVec(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		oid  types.T
		data []byte
	}{
		{types.T_array_float32, types.ArrayToBytes([]float32{1, 2, 3})},
		{types.T_array_float64, types.ArrayToBytes([]float64{1, 2, 3})},
		{types.T_array_bf16, types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1, 2, 3}))},
		{types.T_array_float16, types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, 2, 3}))},
		{types.T_array_int8, types.ArrayToBytes([]int8{1, 2, 3})},
		{types.T_array_uint8, types.ArrayToBytes([]uint8{1, 2, 3})},
	}
	for _, c := range cases {
		vec := vector.NewVec(c.oid.ToType())
		require.NoError(t, vector.AppendBytes(vec, c.data, false, mp))
		lit := GetConstantValue(vec, true, 0)
		require.NotNilf(t, lit, "%s should fold", c.oid)
		vv, ok := lit.Value.(*plan.Literal_VecVal)
		require.Truef(t, ok, "%s -> VecVal literal", c.oid)
		require.Equalf(t, c.data, []byte(vv.VecVal), "%s bytes preserved", c.oid)
		vec.Free(mp)
	}
}
