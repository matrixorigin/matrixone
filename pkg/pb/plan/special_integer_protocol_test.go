// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSpecialIntegerConsumerFeatureSignatures(t *testing.T) {
	for _, tc := range []struct {
		id, overload int32
		args         []int32
		special      bool
	}{
		{262, 0, []int32{61, 61}, false}, {262, 1, []int32{61, 61, 61}, false},
		{262, 2, []int32{61, 23}, true}, {262, 3, []int32{33, 23, 61}, true},
		{202, 0, []int32{61, 61}, false}, {202, 1, []int32{23, 23}, true},
		{373, 0, []int32{23, 23, 23}, false}, {373, 35, []int32{33, 34, 61}, false},
		{373, 36, []int32{23, 23, 31}, true}, {373, 37, []int32{23, 23, 61}, true},
		{373, 38, []int32{23, 23, 28}, true},
	} {
		t.Run(fmt.Sprintf("%d/%d", tc.id, tc.overload), func(t *testing.T) {
			fn := &Function{Func: &ObjectRef{Obj: int64(tc.id)<<32 | int64(tc.overload)}}
			for _, typ := range tc.args {
				fn.Args = append(fn.Args, &Expr{Typ: Type{Id: typ}, Expr: &Expr_Col{Col: &ColRef{}}})
			}
			expr := &Expr{Expr: &Expr_F{F: fn}}
			data, err := expr.Marshal()
			require.NoError(t, err)
			decoded := new(Expr)
			require.NoError(t, decoded.Unmarshal(data))
			features, err := RequiredRemoteExpressionFeatures(decoded)
			require.NoError(t, err)
			require.Equal(t, tc.special, features.SpecialIntegerConsumers)
			require.False(t, features.IntegerParameterCoercion)
			if !tc.special {
				return
			}
			require.True(t, features.Any())
			saved := fn.Args[1]
			fn.Args[1] = nil
			_, err = RequiredRemoteExpressionFeatures(expr)
			require.Error(t, err)
			fn.Args[1] = saved
			saved.Typ.Id = 31
			_, err = RequiredRemoteExpressionFeatures(expr)
			require.Error(t, err)
			saved.Typ.Id = 23
			if tc.id == 262 {
				first := fn.Args[0].Typ.Id
				fn.Args[0].Typ.Id = 50
				_, err = RequiredRemoteExpressionFeatures(expr)
				require.Error(t, err)
				fn.Args[0].Typ.Id = first
				if len(fn.Args) == 3 {
					fn.Args[2].Typ.Id = 23
					_, err = RequiredRemoteExpressionFeatures(expr)
					require.Error(t, err)
				}
			}
			if tc.id == 373 {
				fn.Args[2].Typ.Id = 23
				_, err = RequiredRemoteExpressionFeatures(expr)
				require.Error(t, err)
			}
			fn.Args = fn.Args[:1]
			_, err = RequiredRemoteExpressionFeatures(expr)
			require.Error(t, err)
		})
	}
}
