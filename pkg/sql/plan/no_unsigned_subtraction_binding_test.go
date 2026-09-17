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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestUnsignedSubtractionModeBindsAtExpressionBoundary protects the narrow
// contract: the SQL mode changes the subtraction overload while the
// expression is bound. Nested arithmetic must expose its already-bound
// unsigned result to the parent subtraction; no execution-time marker or DDL
// propagation is needed.
func TestUnsignedSubtractionModeBindsAtExpressionBoundary(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		defaultType types.T
		flagType    types.T
	}{
		{name: "direct", sql: "select cast(0 as unsigned) - 1", defaultType: types.T_uint64, flagType: types.T_int64},
		{name: "reverse operands", sql: "select 1 - cast(0 as unsigned)", defaultType: types.T_uint64, flagType: types.T_int64},
		{name: "both unsigned", sql: "select cast(0 as unsigned) - cast(1 as unsigned)", defaultType: types.T_uint64, flagType: types.T_int64},
		{name: "both signed", sql: "select 0 - 1", defaultType: types.T_int64, flagType: types.T_int64},
		{name: "nested add", sql: "select (cast(0 as unsigned) + 0) - 1", defaultType: types.T_uint64, flagType: types.T_int64},
		{name: "nested multiply", sql: "select (cast(0 as unsigned) * 1) - 1", defaultType: types.T_uint64, flagType: types.T_int64},
		// DIV produces a signed integer here.  The mode must not change an
		// already-signed expression into a different domain.
		{name: "nested div", sql: "select (cast(0 as unsigned) div 1) - 1", defaultType: types.T_int64, flagType: types.T_int64},
		{name: "fractional operand", sql: "select cast(0 as unsigned) - 1.5", defaultType: types.T_decimal128, flagType: types.T_decimal128},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, mode := range []struct {
				name string
				text string
				want types.T
			}{
				{name: "unsigned default", want: test.defaultType},
				{name: "signed flag", text: mysql.SQLModeNoUnsignedSubtraction, want: test.flagType},
				{name: "signed flag with other modes", text: "STRICT_TRANS_TABLES," + mysql.SQLModeNoUnsignedSubtraction, want: test.flagType},
				{name: "signed flag case insensitive", text: "no_unsigned_subtraction", want: test.flagType},
			} {
				t.Run(mode.name, func(t *testing.T) {
					ctx := NewMockCompilerContext(false)
					ctx.SetSqlModeOverride(mode.text)
					stmt, err := mysql.ParseOne(ctx.GetContext(), test.sql, 1)
					require.NoError(t, err)
					defer stmt.Free()

					built, err := BuildPlan(ctx, stmt, false)
					require.NoError(t, err)
					projectExpr := firstProjectExpr(built)
					require.NotNil(t, projectExpr)
					require.Equal(t, int32(mode.want), projectExpr.Typ.Id)
				})
			}
		})
	}
}

func TestUnsignedSubtractionModeBindsExecutionDomain(t *testing.T) {
	for _, test := range []struct {
		name       string
		mode       string
		wantResult bool
	}{
		{name: "default unsigned underflow", wantResult: false},
		{name: "flag permits negative result", mode: mysql.SQLModeNoUnsignedSubtraction, wantResult: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			ctx.SetSqlModeOverride(test.mode)
			stmt, err := mysql.ParseOne(ctx.GetContext(), "select cast(0 as unsigned) - 1", 1)
			require.NoError(t, err)
			defer stmt.Free()

			built, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			expr := firstProjectExpr(built)
			require.NotNil(t, expr)

			proc := testutil.NewProcess(t)
			defer proc.Free()
			got, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, true, true)
			if !test.wantResult {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(types.T_int64), got.Typ.Id)
			require.Equal(t, int64(-1), got.GetLit().GetI64Val())
		})
	}
}

func firstProjectExpr(built *planpb.Plan) *planpb.Expr {
	for _, node := range built.GetQuery().Nodes {
		if node.NodeType == planpb.Node_PROJECT && len(node.ProjectList) > 0 {
			return node.ProjectList[0]
		}
	}
	return nil
}
