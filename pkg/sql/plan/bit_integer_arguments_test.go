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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitIntegerArgumentSourceEvaluation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		sql, want         string
		wantNull, wantErr bool
	}{
		{"export_set(cast(1.4 as decimal(4,1)),'Y','N','',4)", "YNNN", false, false},
		{"export_set(cast(1.40 as decimal(4,2)),'Y','N','',4)", "YNNN", false, false},
		{"export_set(cast(1.400 as decimal(5,3)),'Y','N','',4)", "YNNN", false, false},
		{"export_set(1.5e0,'Y','N','',4)", "NYNN", false, false},
		{"export_set(2.5e0,'Y','N','',4)", "NYNN", false, false},
		{"export_set(-1.5e0,'Y','N','',4)", "NYYY", false, false},
		{"export_set(2.5,'Y','N','',4)", "YYNN", false, false},
		{"export_set(cast(1.5 as double),'Y','N','',4)", "YNNN", false, false},
		{"export_set(true,'Y','N','',4)", "YNNN", false, false},
		{"export_set(false,'Y','N','',4)", "NNNN", false, false},
		{"export_set(NULL,'Y','N','',4)", "", true, false},
		{"export_set('-2tail','Y','N','',4)", "NYYY", false, false},
		{"export_set('18446744073709551615','Y','N','',4)", "YYYY", false, false},
		{"export_set(cast('18446744073709551615' as unsigned),'Y','N','',4)", "YYYY", false, false},
		{"export_set(if(true,-2,cast('18446744073709551615' as unsigned)),'Y','N','',4)", "NYYY", false, false},
		{"export_set(if(false,-2,cast('18446744073709551615' as unsigned)),'Y','N','',4)", "YYYY", false, false},
		{"export_set(if(true,2.5,2.5e0),'Y','N','',4)", "YYNN", false, false},
		{"export_set(if(false,cast('9223372036854775808' as decimal(20,0)),1),'Y','N','',4)", "YNNN", false, false},
		{"export_set(cast('9223372036854775808' as decimal(20,0)),'Y','N','',4)", "", false, true},
		{"export_set('18446744073709551616','Y','N','',4)", "", false, true},
		{"export_set('-9223372036854775809','Y','N','',4)", "", false, true},
		{"make_set(1.5e0,'a','b','c')", "b", false, false},
		{"make_set('-2tail','a','b','c')", "b,c", false, false},
		{"hex(char(65.5e0,'67.9',true))", "424301", false, false},
		{"hex(char(cast('18446744073709551615' as unsigned)))", "FFFFFFFF", false, false},
		{"hex(cast('9007199254740993' as decimal(20,0)))", "20000000000001", false, false},
		{"hex(1.5e0)", "2", false, false},
		{"hex(if(true,2.5,2.5e0))", "3", false, false},
		{"hex(if(false,2.5,2.5e0))", "2", false, false},
		{"hex(if(true,-2,cast('18446744073709551615' as unsigned)))", "FFFFFFFFFFFFFFFE", false, false},
		{"hex(if(false,-2,cast('18446744073709551615' as unsigned)))", "FFFFFFFFFFFFFFFF", false, false},
		{"hex(if(true,'1.5',2.5e0))", "312E35", false, false},
		{"hex(cast(1.5 as double))", "1", false, false},
		{"hex('1.5')", "312E35", false, false},
		{"hex(0x41)", "41", false, false},
		{"conv('ff',15.5,9.5)", "255", false, false},
		{"conv('1.9',10,16)", "1", false, false},
		{"bin(-1)", "1111111111111111111111111111111111111111111111111111111111111111", false, false},
		{"oct(-1)", "1777777777777777777777", false, false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			if err != nil {
				require.True(t, tc.wantErr, "%v", err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				return
			}
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
			if tc.wantErr {
				if err == nil {
					defer free()
				}
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				return
			}
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.wantNull, result.IsConstNull() || result.IsNull(0))
			if !tc.wantNull {
				require.Equal(t, tc.want, result.GetStringAt(0))
			}
		})
	}
}

func TestBitIntegerArgumentBoundDomains(t *testing.T) {
	for _, name := range []string{"hex", "char", "make_set", "export_set"} {
		for _, source := range []types.T{types.T_int64, types.T_uint64, types.T_bit, types.T_enum, types.T_bool, types.T_float64, types.T_decimal128, types.T_varchar} {
			t.Run(name+"/"+source.String(), func(t *testing.T) {
				column := &Expr{Typ: planpb.Type{Id: int32(source), Scale: 1}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
				args := []*Expr{column}
				if name == "make_set" || name == "export_set" {
					args = append(args, makePlan2StringConstExprWithType("Y"), makePlan2StringConstExprWithType("N"))
				}
				bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
				require.NoError(t, err)
				arg := bound.GetF().Args[0]
				if name == "hex" && source == types.T_varchar {
					require.Equal(t, int32(source), arg.Typ.Id)
					return
				}
				target := types.T_int64
				overload := function.IntegerArgumentCastOverload
				if source == types.T_uint64 || source == types.T_bit || source == types.T_varchar {
					target = types.T_uint64
				}
				if source == types.T_varchar {
					overload = function.TextIntegerBitsCastOverload
				}
				require.Equal(t, int32(target), arg.Typ.Id)
				if source != target {
					require.NotNil(t, arg.GetF())
					_, id := function.DecodeOverloadID(arg.GetF().Func.Obj)
					require.Equal(t, overload, id)
					require.Same(t, column, arg.GetF().Args[0])
				}
				require.Same(t, column, args[0])
			})
		}
	}
}

func TestBitIntegerArgumentSelectorMasksOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimalType := types.New(types.T_decimal128, 38, 1)
	decimal, err := types.ParseDecimal128("1.5", 38, 1)
	require.NoError(t, err)
	overflow, err := types.ParseDecimal128("9223372036854775808.0", 38, 1)
	require.NoError(t, err)
	for _, tc := range []struct {
		expression string
		want       []string
	}{
		{"export_set(if(p,d,u),'Y','N','',4)", []string{"NYNN", "YYYY"}},
		{"hex(if(p,d,u))", []string{"2", "FFFFFFFFFFFFFFFF"}},
	} {
		for _, selectedOverflow := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/overflow=%v", tc.expression, selectedOverflow), func(t *testing.T) {
				stmt, err := parsers.ParseOne(proc.Ctx, dialect.MYSQL, "select "+tc.expression, 1)
				require.NoError(t, err)
				defer stmt.Free()
				ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
				bound, err := NewGeneratedColBinder(proc.Ctx, []string{"p", "d", "u"}, []planpb.Type{
					{Id: int32(types.T_bool)}, makePlan2Type(&decimalType), {Id: int32(types.T_uint64)},
				}).BindExpr(ast, 0, false)
				require.NoError(t, err)
				input := batch.NewWithSize(3)
				defer input.Clean(proc.Mp())
				input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
				input.Vecs[1] = vector.NewVec(decimalType)
				input.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{true, selectedOverflow}, nil, proc.Mp()))
				require.NoError(t, vector.AppendFixedList(input.Vecs[1], []types.Decimal128{decimal, overflow}, nil, proc.Mp()))
				require.NoError(t, vector.AppendFixedList(input.Vecs[2], []uint64{^uint64(0), ^uint64(0)}, nil, proc.Mp()))
				input.SetRowCount(2)
				executor, err := colexec.NewExpressionExecutor(proc, bound)
				require.NoError(t, err)
				defer executor.Free()
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				if selectedOverflow {
					require.Error(t, err)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
					return
				}
				require.NoError(t, err)
				for i, want := range tc.want {
					require.Equal(t, want, result.GetStringAt(i))
				}
			})
		}
	}
}

func TestBitIntegerArgumentPreparedDomains(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, query := range []string{
		`select export_set(?,"Y","N","",4)`,
		`select export_set(if(true,?,0),"Y","N","",4)`,
		`select export_set((select ?),"Y","N","",4)`,
	} {
		t.Run(query, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare bit_source from '"+query+"'")
			require.NoError(t, err)
			template := prepared.GetDcl().GetPrepare().Plan
			snapshot, err := template.Marshal()
			require.NoError(t, err)
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(template))
			for _, tc := range []struct {
				value any
				typ   types.Type
				want  string
				null  bool
			}{
				{nil, types.T_any.ToType(), "", true},
				{"1.5", types.T_float64.ToType(), "NYNN", false},
				{"1.5tail", types.T_varchar.ToType(), "YNNN", false},
				{"18446744073709551615", types.T_uint64.ToType(), "YYYY", false},
				{"-1.5", types.New(types.T_decimal128, 20, 1), "NYYY", false},
				{true, types.T_bool.ToType(), "YNNN", false},
			} {
				t.Run(fmt.Sprint(tc.value)+"/"+tc.typ.String(), func(t *testing.T) {
					bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, template, []any{ParamValue{Value: tc.value, IsBinaryProtocol: true, RuntimeType: tc.typ, HasRuntimeType: tc.typ.Oid != types.T_any}})
					require.NoError(t, err)
					require.True(t, changed)
					params := vector.NewVec(types.T_text.ToType())
					defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
					require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(tc.value)), tc.null, proc.Mp()))
					proc.SetPrepareParams(params)
					q := bound.GetQuery()
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, q.Nodes[q.Steps[0]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					defer free()
					require.Equal(t, tc.null, result.IsConstNull() || result.IsNull(0))
					if !tc.null {
						require.Equal(t, tc.want, result.GetStringAt(0))
					}
					after, err := template.Marshal()
					require.NoError(t, err)
					require.Equal(t, snapshot, after)
				})
			}
		})
	}
}
