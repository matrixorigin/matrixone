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

package jsonvalue

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func vecBlockTestVector(t *testing.T, oid types.T, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	f, _ := oid.BlockScaledFormat()
	cell, err := types.AppendBlockScaled(nil, f, []float32{1, -3, 0, 6})
	require.NoError(t, err)
	vec := vector.NewVec(types.New(oid, 4, 0))
	require.NoError(t, vector.AppendBytes(vec, cell, false, mp))
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
	return vec
}

func TestFromVectorVecBlock(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		vec := vecBlockTestVector(t, oid, mp)
		v, err := FromVector(context.Background(), vec, 0, time.UTC, 0, nil)
		require.NoError(t, err)
		require.Equal(t, []any{float64(1), float64(-3), float64(0), float64(6)}, v)
		v, err = FromVector(context.Background(), vec, 1, time.UTC, 0, nil)
		require.NoError(t, err)
		require.Nil(t, v)
	}
}
