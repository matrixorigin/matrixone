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

package sort

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func vecBlockOrderVector(t *testing.T, oid types.T, mp *mpool.MPool, rows [][]float32) *vector.Vector {
	t.Helper()
	f, _ := oid.BlockScaledFormat()
	vec := vector.NewVec(types.New(oid, 3, 0))
	for _, r := range rows {
		if r == nil {
			require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
			continue
		}
		cell, err := types.AppendBlockScaled(nil, f, r)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vec, cell, false, mp))
	}
	return vec
}

// Rows whose byte order differs from their value order: [-1,...] encodes a
// sign bit in its elements, so bytes alone would misorder it.
var vecBlockOrderRows = [][]float32{{3, 0, 0}, {-1, 5, 5}, {1, 2, 3}, {1, 2, 2}}

func TestSortVecBlockByValue(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		require.True(t, IsSupportedType(oid))
		vec := vecBlockOrderVector(t, oid, mp, vecBlockOrderRows)
		os := []int64{0, 1, 2, 3}
		Sort(false, false, false, os, vec)
		require.Equal(t, []int64{1, 3, 2, 0}, os, oid.String())
		os = []int64{0, 1, 2, 3}
		Sort(true, false, false, os, vec)
		require.Equal(t, []int64{0, 2, 3, 1}, os, oid.String())
	}
}
