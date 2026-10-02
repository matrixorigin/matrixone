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

package cdc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

var lowPrecTypes = []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

// lowPrecVector holds 1.5 and -0.5, exact in every low-precision type, then a NULL.
func lowPrecVector(t *testing.T, oid types.T, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(oid.ToType())
	for i, v := range []float32{1.5, -0.5, 0} {
		null := i == 2
		var err error
		switch oid {
		case types.T_bf16:
			err = vector.AppendFixed(vec, types.BF16FromFloat32(v), null, mp)
		case types.T_float16:
			err = vector.AppendFixed(vec, types.Float16FromFloat32(v), null, mp)
		case types.T_float8:
			err = vector.AppendFixed(vec, types.Float8FromFloat32(v), null, mp)
		case types.T_float4:
			err = vector.AppendFixed(vec, types.Float4FromFloat32(v), null, mp)
		}
		require.NoError(t, err)
	}
	return vec
}

func TestLowPrecisionFloatColumnSQL(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	for _, oid := range lowPrecTypes {
		vec := lowPrecVector(t, oid, mp)
		for r, want := range []string{"1.5", "-0.5"} {
			row := make([]any, 1)
			require.NoError(t, extractRowFromVector(ctx, vec, 0, row, r))
			require.Equal(t, []any{float32(map[int]float32{0: 1.5, 1: -0.5}[r])}, row, oid.String())
			sql, err := convertColIntoSql(ctx, row[0], vec.GetType(), nil)
			require.NoError(t, err)
			require.Equal(t, want, string(sql), oid.String())
		}
		vec.Free(mp)
	}
}
