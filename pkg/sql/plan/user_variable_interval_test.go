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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A text user variable in `@v +/- INTERVAL` is a date string, as with a
// literal or DATE_ADD(@v, ...); it must not be forced into a DOUBLE numeric
// context (issue #31708).
func TestUserVariableIntervalArithmeticKeepsTemporalText(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	optimizer.ctxt.ResolveVariableFunc = func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if name == "dt" && !isSystemVar {
			return "2026-01-15", nil
		}
		return (&MockCompilerContext{ctx: optimizer.ctxt.ctx}).ResolveVariable(name, isSystemVar, isGlobalVar)
	}
	optimizer.ctxt.ResolveVariableTypeFunc = func(name string, isSystemVar, isGlobalVar bool) (Type, error) {
		if name == "dt" && !isSystemVar {
			return makeSimplePlan2Type(types.T_varchar), nil
		}
		return (&MockCompilerContext{ctx: optimizer.ctxt.ctx}).ResolveVariableType(name, isSystemVar, isGlobalVar)
	}
	for _, tc := range []struct {
		sql string
		fn  string
	}{
		{"select @dt + interval 1 day", "date_add"},
		{"select interval 1 day + @dt", "date_add"},
		{"select @dt - interval 1 month", "date_sub"},
		{"select (@dt) + interval 2 hour", "date_add"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(optimizer, t, tc.sql)
			require.NoError(t, err)
			require.NotNil(t, findPlanFunctionExpr(p, tc.fn), p.String())
		})
	}

	// Plain numeric arithmetic on the same variable still uses the numeric
	// context.
	p, err := runOneStmt(optimizer, t, "select @dt + 0")
	require.NoError(t, err)
	vt, ok := userVariableEffectiveTypes(p)["dt"]
	require.True(t, ok)
	require.Equal(t, types.T_float64, types.T(vt.Id))
}
