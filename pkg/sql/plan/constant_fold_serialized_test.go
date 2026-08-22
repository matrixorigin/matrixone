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

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestConstantFoldPreservesSerializedLiteralProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)

	tests := []struct {
		name           string
		function       string
		inputType      types.Type
		input          *planpb.Literal
		wantNull       bool
		wantSerialized bool
	}{
		{
			name:           "serial",
			function:       function.SerialFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			wantSerialized: true,
		},
		{
			name:           "serial null",
			function:       function.SerialFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Isnull: true},
			wantNull:       true,
			wantSerialized: false,
		},
		{
			name:           "serial full",
			function:       function.SerialFullFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			wantSerialized: true,
		},
		{
			name:           "serial full null",
			function:       function.SerialFullFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Isnull: true},
			wantSerialized: true,
		},
		{
			name:           "ordinary string function control",
			function:       "lower",
			inputType:      types.T_varchar.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "VISIBLE"}},
			wantSerialized: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registered, err := function.GetFunctionByName(context.Background(), test.function, []types.Type{test.inputType})
			require.NoError(t, err)

			expr := &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_varchar)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: test.function},
					Args: []*planpb.Expr{{
						Typ:  planpb.Type{Id: int32(test.inputType.Oid)},
						Expr: &planpb.Expr_Lit{Lit: test.input},
					}},
				}},
			}

			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
			require.NoError(t, err)
			literal := folded.GetLit()
			require.NotNil(t, literal)
			require.Equal(t, test.wantNull, literal.GetIsnull())
			require.False(t, literal.GetIsBin(), "folding must not acquire SQL hex/bit semantics")
			require.Equal(t, test.wantSerialized, literal.GetIsSerialized())

			copied := DeepCopyExpr(folded)
			require.Equal(t, test.wantSerialized, copied.GetLit().GetIsSerialized())

			payload, err := proto.Marshal(folded)
			require.NoError(t, err)
			decoded := new(planpb.Expr)
			require.NoError(t, proto.Unmarshal(payload, decoded))
			require.Equal(t, test.wantSerialized, decoded.GetLit().GetIsSerialized())
		})
	}
}

func TestConstantFoldPreservesSerialCastSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	registered, err := function.GetFunctionByName(
		context.Background(), function.SerialFunctionName, []types.Type{types.T_bool.ToType()},
	)
	require.NoError(t, err)

	serialExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     registered.GetEncodedOverloadID(),
				ObjName: function.SerialFunctionName,
			},
			Args: []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}},
			}},
		}},
	}
	castExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "cast", []*planpb.Expr{
		serialExpr,
		{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
		},
	})
	require.NoError(t, err)

	runtimeResult, runtimeFree, runtimeErr := colexec.GetReadonlyResultFromExpression(
		proc, DeepCopyExpr(castExpr), []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	if runtimeFree != nil {
		defer runtimeFree()
	}

	folded, foldErr := ConstantFold(
		batch.EmptyForConstFoldBatch, DeepCopyExpr(castExpr), proc, false, true,
	)
	require.Equal(t, runtimeErr != nil, foldErr != nil, "constant folding changed whether the expression fails")
	if runtimeErr != nil {
		return
	}

	foldedResult, foldedFree, err := colexec.GetReadonlyResultFromExpression(
		proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, err)
	defer foldedFree()
	require.Equal(
		t,
		rule.GetConstantValue(runtimeResult, false, 0),
		rule.GetConstantValue(foldedResult, false, 0),
		"constant folding changed the expression value",
	)
}

func TestOptimizerPreservesByteIdenticalSerializedProvenance(t *testing.T) {
	for _, test := range []struct {
		name           string
		sql            string
		wantValue      string
		wantSerialized bool
	}{
		{
			name:           "lossless cast",
			sql:            "select cast(serial(true) as blob)",
			wantValue:      string([]byte{0x27}),
			wantSerialized: true,
		},
		{
			name:           "transformed value control",
			sql:            "select hex(serial(true))",
			wantValue:      "27",
			wantSerialized: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)

			query, err := NewBaseOptimizer(NewMockCompilerContext(true)).Optimize(stmt, false)
			require.NoError(t, err)
			require.NotEmpty(t, query.Steps)
			root := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.NotEmpty(t, root.ProjectList)

			literal := root.ProjectList[0].GetLit()
			require.NotNil(t, literal)
			require.Equal(t, test.wantValue, literal.GetSval())
			require.Equal(t, test.wantSerialized, literal.GetIsSerialized())
		})
	}
}

func TestOptimizerPreservesSerializedListProvenance(t *testing.T) {
	for _, test := range []struct {
		name           string
		sql            string
		wantSerialized bool
	}{
		{
			name: "serialized decimal list",
			sql: "select n_name from nation where n_name in (" +
				"serial(cast(99999 as decimal(38,0))), " +
				"serial(cast(100000 as decimal(38,0))))",
			wantSerialized: true,
		},
		{
			name:           "ordinary unicode list control",
			sql:            "select n_name from nation where n_name in ('Résumé', '東京')",
			wantSerialized: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			query, err := NewBaseOptimizer(NewMockCompilerContext(true)).Optimize(stmt, false)
			require.NoError(t, err)

			literalVecExpr := findFirstLiteralVecExpr(query)
			require.NotNil(t, literalVecExpr)
			require.Equal(t, test.wantSerialized, literalVecExpr.GetVec().GetIsSerialized())

			copied := DeepCopyExpr(literalVecExpr)
			require.Equal(t, test.wantSerialized, copied.GetVec().GetIsSerialized())

			payload, err := proto.Marshal(literalVecExpr)
			require.NoError(t, err)
			decoded := new(planpb.Expr)
			require.NoError(t, proto.Unmarshal(payload, decoded))
			require.Equal(t, test.wantSerialized, decoded.GetVec().GetIsSerialized())
		})
	}
}

func TestOptimizerDoesNotTreatSerializedProvenanceAsFilterValue(t *testing.T) {
	for _, test := range []struct {
		name       string
		serialized string
		wantFalse  bool
	}{
		{name: "byte-identical values", serialized: "true", wantFalse: false},
		{name: "different values control", serialized: "false", wantFalse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(
				t.Context(),
				"select n_name from nation where n_name = serial("+test.serialized+") and n_name = ''''",
				1,
			)
			require.NoError(t, err)

			query, err := NewBaseOptimizer(NewMockCompilerContext(true)).Optimize(stmt, false)
			require.NoError(t, err)

			seenScan := false
			for _, node := range query.Nodes {
				if node.NodeType != planpb.Node_TABLE_SCAN {
					continue
				}
				seenScan = true
				require.NotEmpty(t, node.FilterList)
				hasFalse := false
				for _, filter := range node.FilterList {
					hasFalse = hasFalse || IsFalseExpr(filter)
				}
				require.Equal(t, test.wantFalse, hasFalse,
					"filter-domain normalization changed serialized byte-value semantics")
			}
			require.True(t, seenScan)
		})
	}
}

func TestConstantFoldPreservesSerializedListProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringLiteral := func(value string, serialized bool) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:        &planpb.Literal_Sval{Sval: value},
				IsSerialized: serialized,
			}},
		}
	}
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			stringLiteral(string([]byte{0x27}), true),
			stringLiteral("ordinary", false),
		}}},
	}

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetVec())
	require.True(t, folded.GetVec().GetIsSerialized())
}

func TestConstantListFoldPreservesPerItemStringProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryType := planpb.Type{
		Id:      int32(types.T_varbinary),
		Charset: uint32(types.CharsetBinary),
	}
	literal := func(form planpb.StringLiteralForm) *planpb.Expr {
		return &planpb.Expr{
			Typ: binaryType,
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:       &planpb.Literal_Sval{Sval: "same-bytes"},
				LiteralForm: form,
			}},
		}
	}
	crossDomainList := &planpb.Expr{
		Typ: binaryType,
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			literal(planpb.StringLiteralForm_STRING_LITERAL_TEXT),
			literal(planpb.StringLiteralForm_STRING_LITERAL_NONE),
		}}},
	}

	required, err := planpb.RequiresMORPCVersion23StringProvenance(crossDomainList)
	require.NoError(t, err)
	require.True(t, required)

	foldWithRule := func() *planpb.Expr {
		node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(crossDomainList)}}
		rule.NewConstantFold(false).Apply(node, nil, proc)
		return node.ProjectList[0]
	}
	foldWithPublicAPI := func() *planpb.Expr {
		folded, foldErr := ConstantFold(
			batch.EmptyForConstFoldBatch, DeepCopyExpr(crossDomainList), proc, false, true,
		)
		require.NoError(t, foldErr)
		return folded
	}

	for name, folded := range map[string]*planpb.Expr{
		"rule":   foldWithRule(),
		"public": foldWithPublicAPI(),
	} {
		t.Run(name, func(t *testing.T) {
			require.NotNil(t, folded.GetList(),
				"LiteralVec cannot encode per-item runtime string domains")
			requiredAfterFold, requireErr := planpb.RequiresMORPCVersion23StringProvenance(folded)
			require.NoError(t, requireErr)
			require.True(t, requiredAfterFold)

			result, free, evalErr := colexec.GetReadonlyResultFromExpression(
				proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch},
			)
			require.NoError(t, evalErr)
			defer free()
			require.Equal(t, types.RuntimeStringText, result.GetRuntimeStringDomainAt(0))
			require.Equal(t, types.RuntimeStringInherit, result.GetRuntimeStringDomainAt(1))
		})
	}

	textType := planpb.Type{Id: int32(types.T_varchar)}
	ordinaryList := &planpb.Expr{
		Typ: textType,
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{{
			Typ: textType,
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:       &planpb.Literal_Sval{Sval: "ordinary"},
				LiteralForm: planpb.StringLiteralForm_STRING_LITERAL_TEXT,
			}},
		}}}},
	}
	foldedControl, err := ConstantFold(
		batch.EmptyForConstFoldBatch, ordinaryList, proc, false, true,
	)
	require.NoError(t, err)
	require.NotNil(t, foldedControl.GetVec(), "same-domain list keeps the existing fold fast path")
	require.Equal(t, uint32(types.StringSourceLiteral), foldedControl.GetVec().GetStringSource())
	require.Equal(t, foldedControl.GetVec().GetStringSource(),
		DeepCopyExpr(foldedControl).GetVec().GetStringSource())
	payload, err := proto.Marshal(foldedControl)
	require.NoError(t, err)
	decodedControl := new(planpb.Expr)
	require.NoError(t, proto.Unmarshal(payload, decodedControl))
	require.Equal(t, foldedControl.GetVec().GetStringSource(),
		decodedControl.GetVec().GetStringSource())
	result, free, evalErr := colexec.GetReadonlyResultFromExpression(
		proc, foldedControl, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, evalErr)
	defer free()
	require.Equal(t, types.StringSourceLiteral, result.GetStringSourceAt(0))
}

func TestMakeInExprRuntimePayloadKeepsExpressionSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	scanKeys := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(scanKeys, []byte("runtime-key"), false, proc.Mp()))
	require.Equal(t, types.StringSourceExpression, scanKeys.GetStringSourceAt(0))
	data, err := scanKeys.MarshalBinary()
	require.NoError(t, err)
	scanKeys.Free(proc.Mp())

	typ := planpb.Type{Id: int32(types.T_varchar)}
	inExpr := MakeInExpr(t.Context(), &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}, 1, data, false)
	runtimePayload := inExpr.GetF().Args[1]
	require.Zero(t, runtimePayload.GetVec().GetStringSource())

	result, free, evalErr := colexec.GetReadonlyResultFromExpression(
		proc, runtimePayload, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, evalErr)
	defer free()
	require.Equal(t, types.StringSourceExpression, result.GetStringSourceAt(0))
}

func TestConstantFoldPreservesSelectedStringDomain(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantDomain types.RuntimeStringDomain
		wantFold   bool
	}{
		{name: "if text to binary", sql: "select if(true, 'selected', cast('fallback' as varbinary))", wantDomain: types.RuntimeStringText, wantFold: true},
		{name: "case runtime control", sql: "select case when true then 'selected' else cast('fallback' as varbinary) end", wantDomain: types.RuntimeStringText},
		{name: "coalesce binary to text", sql: "select coalesce(_binary'selected', cast('fallback' as char))", wantDomain: types.RuntimeStringBinary, wantFold: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			pl, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			require.NotEmpty(t, query.Steps)
			root := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.Len(t, root.ProjectList, 1)
			expr := root.ProjectList[0]

			foldWithRule := func() *planpb.Expr {
				node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(expr)}}
				rule.NewConstantFold(false).Apply(node, nil, ctx.GetProcess())
				return node.ProjectList[0]
			}
			foldWithPublicAPI := func() *planpb.Expr {
				folded, foldErr := ConstantFold(
					batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), ctx.GetProcess(), false, true)
				require.NoError(t, foldErr)
				return folded
			}

			for name, folded := range map[string]*planpb.Expr{
				"rule":   foldWithRule(),
				"public": foldWithPublicAPI(),
			} {
				t.Run(name, func(t *testing.T) {
					if test.wantFold {
						require.NotNil(t, folded.GetLit())
					} else {
						require.NotNil(t, folded.GetF())
					}
					result, free, evalErr := colexec.GetReadonlyResultFromExpression(
						ctx.GetProcess(), folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, evalErr)
					defer free()
					require.Equal(t, "selected", result.GetStringAt(0))
					require.Equal(t, test.wantDomain, result.GetRuntimeStringDomainAt(0))
				})
			}
		})
	}
}

func findFirstLiteralVecExpr(query *planpb.Query) *planpb.Expr {
	var found *planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil || found != nil {
			return
		}
		if expr.GetVec() != nil {
			found = expr
			return
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				visit(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
	}
	for _, node := range query.Nodes {
		for _, filter := range node.FilterList {
			visit(filter)
		}
	}
	return found
}
