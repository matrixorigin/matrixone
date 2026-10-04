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

package common

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TypeStringValue backs mo_ctl inspect dumps and MoVectorToString log lines.
// It listed vecf32/vecf64 only, so a narrow vector printed as hex bytes (or
// fell through) where vecf32 printed a readable array.
func TestTypeStringValueNarrowVectors(t *testing.T) {
	cases := []struct {
		name string
		oid  types.T
		buf  []byte
		want string
	}{
		{"bf16", types.T_array_bf16,
			types.ArrayToBytes[types.BF16](types.Float32ToBF16Slice([]float32{1, 2, 3})), "[1, 2, 3]"},
		{"f16", types.T_array_float16,
			types.ArrayToBytes[types.Float16](types.Float32ToFloat16Slice([]float32{1, 2, 3})), "[1, 2, 3]"},
		{"int8", types.T_array_int8,
			types.ArrayToBytes[int8]([]int8{-1, 0, 7}), "[-1, 0, 7]"},
		{"uint8", types.T_array_uint8,
			types.ArrayToBytes[uint8]([]uint8{1, 128, 255}), "[1, 128, 255]"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := TypeStringValue(c.oid.ToType(), c.buf, false)
			require.Equal(t, c.want, got)
		})
	}
	// null still prints as null for these types.
	require.Equal(t, "null", TypeStringValue(types.T_array_int8.ToType(), nil, true))
}
