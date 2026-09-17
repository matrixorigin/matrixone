// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportSetNumericProducerBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, source, value, want string
		typ                       types.Type
	}{
		{"arithmetic_decimal", "ABS(?)+0", "2.5", "YYNN", types.New(types.T_decimal64, 3, 1)},
		{"arithmetic_decimal_precision", "ABS(?)+0", "9007199254740993.0", "YNNN", types.New(types.T_decimal64, 17, 1)},
		{"arithmetic_real_peer", "ABS(?)+0e0", "2.5", "NYNN", types.New(types.T_decimal64, 3, 1)},
		{"scalar_explicit_decimal", "(SELECT ABS(CAST(? AS DECIMAL(3,1))))", "2.5", "YYNN", types.T_text.ToType()},
		{"explicit_real", "CAST(ABS(?) AS DOUBLE)+0", "2.5", "NYNN", types.New(types.T_decimal64, 3, 1)},
		{"direct_text", "ABS(?)", "2.5", "YYNN", types.T_text.ToType()},
		{"scalar_text", "(SELECT ABS(?))", "2.5", "NYNN", types.T_text.ToType()},
		{"arithmetic_text", "ABS(?)+0", "2.5", "YYNN", types.T_text.ToType()},
		{"nested_abs_text", "ABS(ABS(?))", "2.5", "YYNN", types.T_text.ToType()},
		{"scalar_decimal", "(SELECT ABS(?))", "2.5", "NYNN", types.New(types.T_decimal64, 3, 1)},
		{"conditional_real_peer_decimal", "IF(TRUE,ABS(?),0e0)", "2.5", "NYNN", types.New(types.T_decimal64, 3, 1)},
		{"conditional_real_peer", "IF(TRUE,ABS(?),0e0)", "2.5", "NYNN", types.T_text.ToType()},
		{"conditional_integer_peer", "IF(TRUE,ABS(?),0)", "2.5", "YYNN", types.T_text.ToType()},
		{"coalesce_integer_peer", "COALESCE(ABS(?),0)", "2.5", "YYNN", types.T_text.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stmt, cw, _ := newPreparedExecuteEnvForSQL(t, 470, `select export_set(`+tc.source+`,'Y','N','',4)`)
			defer stmt.Close()
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			encoded, err := cached.Marshal()
			require.NoError(t, err)
			var restored planpb.Plan
			require.NoError(t, restored.Unmarshal(encoded))
			stmt.refreshExportSetParamPositions(&restored, 1)
			vals := []any{plan2.ParamValue{Value: tc.value, SourceType: tc.typ, HasSourceType: true, EnableNumericPrefix: true}}
			stmt.applyExportSetNullRuntimeTypes(vals)
			filled, err := plan2.FillValuesOfParamsInPlan(cw.proc.Ctx, plan2.DeepCopyPlan(&restored), vals)
			require.NoError(t, err)
			expr := filled.GetQuery().Nodes[filled.GetQuery().Steps[0]].ProjectList[0]
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0), expr.String())
		})
	}
}
