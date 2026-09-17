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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExportSetFoldedRealRetainsIntegerContract(t *testing.T) {
	for _, mode := range []string{"binder", "optimizer"} {
		for _, op := range []string{"+", "*", "unary_minus", "unary_plus"} {
			t.Run(mode+"/"+op, func(t *testing.T) {
				proc := testutil.NewProc(t)
				defer proc.Free()
				args := []*Expr{makePlan2Float64ConstExprWithType(1e100)}
				if op == "+" {
					args = append(args, makePlan2Float64ConstExprWithType(0))
				}
				if op == "*" {
					args = append(args, makePlan2Float64ConstExprWithType(1))
				}
				source, err := BindFuncExprImplByPlanExpr(context.Background(), op, args)
				require.NoError(t, err)
				if mode == "binder" {
					source, err = ConstantFold(batch.EmptyForConstFoldBatch, source, proc, false, true)
					require.NoError(t, err)
				} else {
					node := &planpb.Node{ProjectList: []*Expr{source}}
					rule.NewConstantFold(false).Apply(node, &planpb.Query{}, proc)
					source = node.ProjectList[0]
				}
				require.NotNil(t, source.GetLit())
				require.NotNil(t, source.GetLit().GetSrc().GetF(), "folding must retain computed REAL provenance")
				require.Equal(t, op, source.GetLit().GetSrc().GetF().GetFunc().GetObjName())
				expr, err := BindFuncExprImplByPlanExpr(context.Background(), "export_set", []*Expr{source, makePlan2StringConstExprWithType("Y"), makePlan2StringConstExprWithType("N"), makePlan2StringConstExprWithType(""), makePlan2Int64ConstExprWithType(4)})
				require.NoError(t, err)
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				if free != nil {
					defer free()
				}
				if op == "unary_plus" {
					require.NoError(t, err)
					require.Equal(t, "YYYY", result.GetStringAt(0))
				} else {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "got %v", err)
				}
			})
		}
	}
}

func TestExportSetConditionalAndRelationalScalarOutputs(t *testing.T) {
	for _, tc := range []struct {
		source  string
		integer bool
	}{
		{"if(true,(select 1e100),0e0)", true},
		{"case when true then (select 1e100) else 0e0 end", true},
		{"(select abs(cast(n_regionkey as double)) from nation limit 1)", true},
		{"if(true,(select abs(cast(n_regionkey as double)) from nation limit 1),0e0)", true},
		{"(select abs(1e100))", false},
		{"ifnull((select 1e100),0e0)", false},
	} {
		t.Run(tc.source, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t, `select export_set(`+tc.source+`,'Y','N','',4)`)
			require.NoError(t, err)
			expr := findPlanFunctionExpr(p, "export_set")
			require.NotNil(t, expr)
			require.Equal(t, tc.integer, expr.GetF().Args[0].Typ.Id == int32(types.T_int64), expr.String())
		})
	}
}

func TestExportSetOrdinaryScalarRealConversion(t *testing.T) {
	for _, tc := range []struct {
		source    string
		saturates bool
	}{
		{"1e100", true}, {"+1e100", true}, {"1e100+0", false}, {"1e100*1", false}, {"-1e100", false}, {"+(1e100+0)", false}, {"abs(1e100)", false},
	} {
		t.Run(tc.source, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t, `select export_set((select `+tc.source+`),'Y','N','',4)`)
			require.NoError(t, err)
			expr := findPlanFunctionExpr(p, "export_set")
			require.NotNil(t, expr)
			require.Equal(t, tc.saturates, isBitwiseAggregatePrivateCast(expr.GetF().Args[0]), expr.String())
		})
	}
}
