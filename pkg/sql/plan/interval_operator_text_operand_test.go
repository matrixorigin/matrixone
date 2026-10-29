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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// `expr +/- INTERVAL n unit` must accept every operand DATE_ADD/DATE_SUB
// accepts: CHAR/TEXT/BLOB strings, not only VARCHAR.
func TestIntervalOperatorAcceptsAllStringOperands(t *testing.T) {
	for _, tc := range []struct {
		sql string
		fn  string
	}{
		{"select cast('2026-01-31' as char(10)) + interval 1 day", "date_add"},
		{"select interval 1 month + cast('2026-01-31' as char(10))", "date_add"},
		{"select cast('2026-01-31' as text) + interval 1 month", "date_add"},
		{"select cast('2026-01-31' as text) - interval 1 hour", "date_sub"},
		{"select cast('2026-01-31' as char(10)) - interval 1 day", "date_sub"},
		{"select cast('2026-01-31' as binary(10)) + interval 1 day", "date_add"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			require.NotNil(t, findPlanFunctionExpr(p, tc.fn), p.String())
		})
	}
}

// A parameter marker next to INTERVAL is bound like DATE_ADD(?, ...): it must
// prepare, and every execution must rebind to the internal date function.
func TestPreparedIntervalOperatorAcceptsMarkerOperand(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		sql string
		fn  string
	}{
		{"prepare s1 from select ? + interval 1 day", "date_add"},
		{"prepare s2 from select interval 1 month + ?", "date_add"},
		{"prepare s3 from select ? - interval 1 hour", "date_sub"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			preparedPlan := prepared.GetDcl().GetPrepare().GetPlan()
			require.NotNil(t, findPlanFunctionExpr(preparedPlan, tc.fn), preparedPlan.String())

			for _, value := range []ParamValue{
				{Value: "2026-02-28", SourceType: types.T_varchar.ToType(), HasSourceType: true},
				{Value: "2024-02-29 13:14:15", SourceType: types.T_varchar.ToType(), HasSourceType: true},
				{SourceType: types.T_varchar.ToType(), HasSourceType: true},
				{Value: "2026-02-28", RuntimeType: types.T_text.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
			} {
				filled, _, fillErr := FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{value})
				require.NoError(t, fillErr)
				fn := findPlanFunctionExpr(filled, tc.fn)
				require.NotNil(t, fn, filled.String())
				require.Len(t, fn.GetF().GetArgs(), 3, fn.String())
			}
		})
	}
}
