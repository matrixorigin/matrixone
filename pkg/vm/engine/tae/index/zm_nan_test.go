// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestZoneMapLegacyNaNScalarPruning(t *testing.T) {
	t.Run("float32", func(t *testing.T) { testLegacyNaNScalarPruning(t, types.T_float32, float32(math.NaN())) })
	t.Run("float64", func(t *testing.T) { testLegacyNaNScalarPruning(t, types.T_float64, math.NaN()) })
}

func testLegacyNaNScalarPruning[T ~float32 | ~float64](t *testing.T, oid types.T, nan T) {
	t.Helper()
	seven := types.EncodeValue(T(7), oid)
	for _, bounds := range [][2]T{{nan, nan}, {nan, 7}, {7, nan}} {
		zm := BuildZM(oid, seven)
		copy(zm.GetMinBuf(), types.EncodeValue(bounds[0], oid))
		copy(zm.GetMaxBuf(), types.EncodeValue(bounds[1], oid))
		require.True(t, zm.IsInited())
		require.True(t, zm.ContainsKey(seven), "NaN 摘要不能证明普通值不存在")
		require.True(t, zm.AnyLTByValue(seven))
		require.True(t, zm.AnyLEByValue(seven))
		require.True(t, zm.AnyGTByValue(seven))
		require.True(t, zm.AnyGEByValue(seven))
		require.True(t, zm.Between(seven, seven))
		for hint := uint8(0); hint < 4; hint++ {
			require.True(t, zm.InRange(seven, seven, hint))
		}
	}
	// 普通有限值和无穷不是未知摘要，仍须正确裁剪。
	for _, value := range []T{6, 7, 8, T(math.Inf(-1)), T(math.Inf(1))} {
		zm := BuildZM(oid, types.EncodeValue(value, oid))
		require.Equal(t, value == 7, zm.ContainsKey(seven))
		require.Equal(t, value < 7, zm.AnyLTByValue(seven))
		require.Equal(t, value <= 7, zm.AnyLEByValue(seven))
		require.Equal(t, value > 7, zm.AnyGTByValue(seven))
		require.Equal(t, value >= 7, zm.AnyGEByValue(seven))
		require.Equal(t, value == 7, zm.Between(seven, seven))
		for hint := uint8(0); hint < 4; hint++ {
			require.Equal(t, hint == 0 && value == 7, zm.InRange(seven, seven, hint))
		}
	}
	// 本修复不改变未初始化摘要在底层比较接口中的既有语义。
	uninitialized := NewZM(oid, 0)
	require.False(t, uninitialized.ContainsKey(seven))
	require.False(t, uninitialized.AnyGTByValue(seven))
	require.False(t, uninitialized.Between(seven, seven))
}
