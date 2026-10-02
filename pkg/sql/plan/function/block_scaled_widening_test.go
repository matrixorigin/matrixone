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

// TestBlockScaledWideningScope pins which functions accept vecf32 but reject vecf8/vecf4:
// exactly the comparisons, the byte encodings hex/to_base64 and the IVF-internal distance
// helpers. The allowlisted functions resolve with the vecf8/vecf4 argument cast to vecf32.
func TestBlockScaledWideningScope(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		f32 := types.New(types.T_array_float32, 4, 0)
		bs := types.New(oid, 4, 0)
		str := types.T_varchar.ToType()
		var rejected []string
		for name := range functionIdRegister {
			for _, shape := range []struct {
				label string
				a, b  []types.Type
			}{
				{"(v)", []types.Type{f32}, []types.Type{bs}},
				{"(v,v)", []types.Type{f32, f32}, []types.Type{bs, bs}},
				{"('k',v)", []types.Type{str, f32}, []types.Type{str, bs}},
			} {
				_, ok32 := GetFunctionByNameWithoutError(name, shape.a)
				_, okBS := GetFunctionByNameWithoutError(name, shape.b)
				if ok32 && !okBS {
					rejected = append(rejected, name+shape.label)
				}
			}
		}
		sort.Strings(rejected)
		require.Equal(t, []string{
			"!=('k',v)", "!=(v,v)", "<('k',v)", "<(v,v)", "<=('k',v)", "<=(v,v)",
			"<=>('k',v)", "<=>(v,v)", "<>('k',v)", "<>(v,v)", "=('k',v)", "=(v,v)",
			">('k',v)", ">(v,v)", ">=('k',v)", ">=(v,v)",
			"hex(v)", "in(v,v)",
			"l2_distance_sq_xc('k',v)", "l2_distance_sq_xc(v,v)", "l2_distance_xc('k',v)", "l2_distance_xc(v,v)",
			"to_base64(v)",
		}, rejected, oid.String())

		r, ok := GetFunctionByNameWithoutError("summation", []types.Type{bs})
		require.True(t, ok)
		require.True(t, r.needCast)
		require.Equal(t, types.T_array_float32, r.targetTypes[0].Oid)
		require.Equal(t, int32(4), r.targetTypes[0].Width)
	}
}
