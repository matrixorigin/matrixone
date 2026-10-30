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

// TestAppendValueToVectorLowPrecisionFloat checks that a window value function appends
// bf16, float16, float8 and float4 values as fixed-size values.
func TestAppendValueToVectorLowPrecisionFloat(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, tc := range []struct {
		oid  types.T
		data []byte
	}{
		{types.T_bf16, types.EncodeFixed(types.BF16FromFloat32(1.5))},
		{types.T_float16, types.EncodeFixed(types.Float16FromFloat32(1.5))},
		{types.T_float8, types.EncodeFixed(types.Float8FromFloat32(1.5))},
		{types.T_float4, types.EncodeFixed(types.Float4FromFloat32(1.5))},
	} {
		vec := vector.NewVec(tc.oid.ToType())
		require.NoError(t, appendValueToVector(vec, tc.data, tc.oid.ToType(), mp))
		require.Equal(t, 1, vec.Length())
		f, ok := vector.GetLowPrecisionFloatAt(vec, 0)
		require.True(t, ok)
		require.Equal(t, float32(1.5), f, tc.oid.String())
		vec.Free(mp)
	}
}
