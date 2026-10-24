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

package logtailreplay

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// lpVec builds a vector of oid (bf16, float16, float8 or float4) from vals.
func lpVec(t *testing.T, oid types.T, mp *mpool.MPool, vals ...float32) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(oid.ToType())
	for _, v := range vals {
		var err error
		switch oid {
		case types.T_bf16:
			err = vector.AppendFixed(vec, types.BF16FromFloat32(v), false, mp)
		case types.T_float16:
			err = vector.AppendFixed(vec, types.Float16FromFloat32(v), false, mp)
		case types.T_float8:
			err = vector.AppendFixed(vec, types.Float8FromFloat32(v), false, mp)
		case types.T_float4:
			err = vector.AppendFixed(vec, types.Float4FromFloat32(v), false, mp)
		}
		require.NoError(t, err)
	}
	return vec
}

var lpTypes = []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

// TestAppendFromEntryLowPrecisionFloat checks that the change reader (table_changes, CDC,
// data branch) copies bf16, float16, float8 and float4 values and NULLs.
func TestAppendFromEntryLowPrecisionFloat(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range lpTypes {
		src := lpVec(t, oid, mp, 1.5, 2)
		require.NoError(t, vector.AppendAny(src, nil, true, mp))
		dst := vector.NewVec(oid.ToType())
		for i := 0; i < 3; i++ {
			appendFromEntry(src, dst, i, mp)
		}
		f, _ := vector.GetLowPrecisionFloatAt(dst, 1)
		require.Equal(t, float32(2), f, oid.String())
		require.True(t, dst.IsNull(2))
		src.Free(mp)
		dst.Free(mp)
	}
}
