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

package function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// MySQL treats TRUE/FALSE as the integers 1/0, so a CASE/IF/COALESCE that
// mixes a BOOL branch with a numeric branch returns the numeric type
// (COALESCE(2, TRUE) = 2). Resolving the common type to BOOL turns every
// numeric value into 0/1.
func TestBoolNumericValueBranchesResolveToNumeric(t *testing.T) {
	ctx := context.Background()
	b := types.T_bool.ToType()
	i32 := types.T_int32.ToType()
	i64 := types.T_int64.ToType()
	f64 := types.T_float64.ToType()

	for _, tc := range []struct {
		name string
		fn   string
		args []types.Type
		ret  types.T
	}{
		{"coalesce(int, bool)", "coalesce", []types.Type{i64, b}, types.T_int64},
		{"coalesce(bool, int)", "coalesce", []types.Type{b, i64}, types.T_int64},
		{"coalesce(double, bool)", "coalesce", []types.Type{f64, b}, types.T_float64},
		{"coalesce(null, int32, bool)", "coalesce", []types.Type{types.T_any.ToType(), i32, b}, types.T_int64},
		{"case when c then int else bool", "case", []types.Type{b, i64, b}, types.T_int64},
		{"case when c then bool when c then double", "case", []types.Type{b, b, b, f64}, types.T_float64},
		{"if(c, double, bool)", "iff", []types.Type{b, f64, b}, types.T_float64},
		{"if(c, bool, int)", "iff", []types.Type{b, b, i64}, types.T_int64},
		// BOOL-only branches keep BOOL.
		{"coalesce(bool, bool)", "coalesce", []types.Type{b, b}, types.T_bool},
		{"case when c then bool else bool", "case", []types.Type{b, b, b}, types.T_bool},
		{"if(c, bool, bool)", "iff", []types.Type{b, b, b}, types.T_bool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, tc.fn, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.ret, get.GetReturnType().Oid)
			if targets, shouldCast := get.ShouldDoImplicitTypeCast(); shouldCast {
				for i, arg := range tc.args {
					// A BOOL value branch must be cast to the numeric result type,
					// not the other way round; CASE/IF conditions stay BOOL.
					isCondition := (tc.fn == "case" && i%2 == 0 && !(len(tc.args)%2 == 1 && i == len(tc.args)-1)) ||
						(tc.fn == "iff" && i == 0)
					if isCondition {
						require.Equal(t, types.T_bool, targets[i].Oid, "arg %d", i)
					} else if arg.Oid != types.T_any {
						require.Equal(t, tc.ret, targets[i].Oid, "arg %d", i)
					}
				}
			}
		})
	}
}
