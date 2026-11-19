// Copyright 2021 - 2024 Matrix Origin
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

package compute

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCompareVecBlock(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f, _ := oid.BlockScaledFormat()
		a, err := types.AppendBlockScaled(nil, f, []float32{-1, 5, 5})
		require.NoError(t, err)
		b, err := types.AppendBlockScaled(nil, f, []float32{3, 0, 0})
		require.NoError(t, err)
		require.Equal(t, -1, Compare(a, b, oid, 0, 0))
		require.Equal(t, 1, CompareGeneric(b, a, oid))
		require.Equal(t, 0, CompareGeneric(a, a, oid))
	}
}
