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

package function

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestBlockScaledVectorParity checks that vecf8 and vecf4 resolve every function shape
// vecbf16 resolves (comparisons, NULL handling, element math, JSON, ...), so the
// block-scaled vectors have no exceptions among the narrow vector types.
func TestBlockScaledVectorParity(t *testing.T) {
	ref := types.New(types.T_array_bf16, 4, 0)
	str := types.T_varchar.ToType()
	i64 := types.T_int64.ToType()
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		bs := types.New(oid, 4, 0)
		var missing []string
		for name := range functionIdRegister {
			for _, shape := range []struct {
				label string
				r, b  []types.Type
			}{
				{"(v)", []types.Type{ref}, []types.Type{bs}},
				{"(v,v)", []types.Type{ref, ref}, []types.Type{bs, bs}},
				{"(v,'s')", []types.Type{ref, str}, []types.Type{bs, str}},
				{"('s',v)", []types.Type{str, ref}, []types.Type{str, bs}},
				{"(v,i)", []types.Type{ref, i64}, []types.Type{bs, i64}},
				{"(v,v,v)", []types.Type{ref, ref, ref}, []types.Type{bs, bs, bs}},
			} {
				_, okRef := GetFunctionByNameWithoutError(name, shape.r)
				_, okBS := GetFunctionByNameWithoutError(name, shape.b)
				if okRef && !okBS {
					missing = append(missing, name+shape.label)
				}
			}
		}
		sort.Strings(missing)
		require.Empty(t, missing, "%s lacks shapes vecbf16 has", oid)

		r, ok := GetFunctionByNameWithoutError("summation", []types.Type{bs})
		require.True(t, ok)
		require.True(t, r.needCast)
		require.Equal(t, types.T_array_float32, r.targetTypes[0].Oid)
		require.Equal(t, int32(4), r.targetTypes[0].Width)
		r, ok = GetFunctionByNameWithoutError("=", []types.Type{bs, str})
		require.True(t, ok)
		require.True(t, r.needCast)
		require.Equal(t, oid, r.targetTypes[1].Oid, "a string literal is cast to the column type")
	}
}
