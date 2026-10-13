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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// LAG/LEAD must reconcile the value argument (1st arg) and the default value
// (3rd arg) into one common result type. Narrowing the default to the value
// type truncates longer string defaults and rounds decimal ones (#29974).
func TestBindLagLeadDefaultCapacity(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				value, def   *planpb.Expr
				oid          types.T
				width, scale int32
			}{
				{"text boundary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 65535}}, makePlan2Int64ConstExprWithType(0), types.T_text, 65535, 0},
				{"mediumtext", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 16777215}}, makePlan2Int64ConstExprWithType(0), types.T_text, 16777215, 0},
				{"unbounded text", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}}, makePlan2Int64ConstExprWithType(0), types.T_text, 0, 0},
				{"text default", makePlan2Int64ConstExprWithType(0), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 16777215}}, types.T_text, 16777215, 0},
				{"mediumblob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, makePlan2Int64ConstExprWithType(0), types.T_blob, 16777215, 0},
				{"unbounded blob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob)}}, makePlan2Int64ConstExprWithType(0), types.T_blob, 0, 0},
				{"blob default", makePlan2Int64ConstExprWithType(0), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, types.T_blob, 16777215, 0},
				{"blob text", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 65535}}, types.T_blob, 16777215, 0},
				{"text blob", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text), Width: 65535}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_blob), Width: 16777215}}, types.T_blob, 0, 0},
				{"decimal signed zero", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2Int64ConstExprWithType(0), types.T_decimal256, 65, 65},
				{"decimal unsigned zero", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2Uint64ConstExprWithType(0), types.T_decimal256, 65, 65},
				{"decimal typed null", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, types.T_decimal256, 65, 65},
				{"decimal public boundary", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 64, Scale: 63}}, makePlan2Int64ConstExprWithType(10), types.T_decimal256, 65, 63},
				{"varchar numeric", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}}, makePlan2Int64ConstExprWithType(0), types.T_varchar, 10, 0},
				{"decimal integer literal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}}, makePlan2Int64ConstExprWithType(1000), types.T_decimal64, 5, 1},
				{"decimal negative literal", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}}, makePlan2Int64ConstExprWithType(-1000), types.T_decimal64, 5, 1},
				{"decimal integer column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}, types.T_decimal128, 20, 1},
				{"decimal uint64 column", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 4}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}}, types.T_decimal128, 24, 4},
				{"decimal256 promotion", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 30}}, makePlan2Int64ConstExprWithType(999999999999), types.T_decimal256, 42, 30},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{tc.value, makePlan2Int64ConstExprWithType(1), tc.def})
					require.NoError(t, err)
					for i, typ := range []planpb.Type{expr.Typ, expr.GetF().Args[0].Typ, expr.GetF().Args[2].Typ} {
						require.Equal(t, int32(tc.oid), typ.Id)
						require.Equal(t, tc.scale, typ.Scale)
						// Same-family decimals and BLOBs need no metadata-only
						// cast when their original capacity fits the result.
						if i == 0 || (!tc.oid.IsDecimal() && tc.oid != types.T_blob) {
							require.Equal(t, tc.width, typ.Width)
						}
					}
				})
			}
			_, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}},
				makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(999999999999),
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
			for _, tc := range []struct {
				name       string
				value, def *planpb.Expr
			}{
				{"nonzero integer precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, makePlan2Int64ConstExprWithType(1)},
				{"integer domain precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}},
				{"aligned decimal SQL precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 64}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 3, Scale: 1}}},
				{"aligned decimal physical precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 0}}},
				{"integer value decimal default precision overflow", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_decimal256), Width: 65, Scale: 65}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{tc.value, makePlan2Int64ConstExprWithType(1), tc.def})
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				})
			}
		})
	}
}

func TestLagLeadPersistedDDLRebuild(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, tc := range []struct {
			sourceType   string
			oid          types.T
			width, scale int32
		}{
			{"mediumblob", types.T_blob, 16777215, 0},
			{"decimal(65,65)", types.T_decimal256, 65, 65},
		} {
			t.Run(name+tc.sourceType, func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				source, err := buildTestCreateTableStmt(mock, "create table lag_lead_source(v "+tc.sourceType+")")
				require.NoError(t, err)
				mock.ctxt.tables["lag_lead_source"] = source
				mock.ctxt.objects["lag_lead_source"] = &ObjectRef{DbName: mock.ctxt.DefaultDatabase(), ObjName: "lag_lead_source"}
				p, err := runOneStmt(mock, t, "create table lag_lead_persisted as select "+name+"(v,0,0) over () as v from lag_lead_source")
				require.NoError(t, err)
				persisted := p.GetDdl().GetCreateTable().GetTableDef()
				require.NotNil(t, persisted)
				require.Equal(t, int32(tc.oid), persisted.Cols[0].Typ.Id)
				require.Equal(t, tc.width, persisted.Cols[0].Typ.Width)
				require.Equal(t, tc.scale, persisted.Cols[0].Typ.Scale)
				// Use the same exporter as SHOW CREATE TABLE, then bind its
				// actual output rather than a hand-written equivalent schema.
				persisted.Name = "lag_lead_rebuilt"
				ddl, _, err := ConstructCreateTableSQL(&mock.ctxt, persisted, nil, false, nil)
				require.NoError(t, err)
				p, err = runOneStmt(mock, t, ddl)
				require.NoError(t, err, ddl)
				rebuilt := p.GetDdl().GetCreateTable().GetTableDef()
				require.NotNil(t, rebuilt)
				require.Equal(t, persisted.Cols[0].Typ.Id, rebuilt.Cols[0].Typ.Id)
				require.Equal(t, persisted.Cols[0].Typ.Width, rebuilt.Cols[0].Typ.Width)
				require.Equal(t, persisted.Cols[0].Typ.Scale, rebuilt.Cols[0].Typ.Scale)
				require.Equal(t, persisted.Cols[0].Typ.Charset, rebuilt.Cols[0].Typ.Charset)
			})
		}
	}
}

func TestLagLeadWindowLiteralIdentity(t *testing.T) {
	for _, name := range []string{"lag", "lead"} {
		for _, tc := range []struct {
			args    string
			oids    []types.T
			windows int
		}{
			{"(7,0,0.5) over (), " + name + "(7,0,'0.5') over ()", []types.T{types.T_decimal128, types.T_varchar}, 2},
			{"(7,0,'0.5') over (), " + name + "(7,0,0.5) over ()", []types.T{types.T_varchar, types.T_decimal128}, 2},
			{"(7,0,0.5) over ()", []types.T{types.T_decimal128}, 1},
			{"(7,0,'0.5') over ()", []types.T{types.T_varchar}, 1},
			{"(7,0,'0.5') over (), " + name + "(7,0,'0.5') over ()", []types.T{types.T_varchar, types.T_varchar}, 1},
		} {
			t.Run(name+tc.args, func(t *testing.T) {
				p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+name+tc.args)
				require.NoError(t, err)
				specs := allWindowSpecs(p)
				require.Len(t, specs, tc.windows)
				q := p.GetQuery()
				outputs := q.Nodes[q.Steps[0]].ProjectList
				require.Len(t, outputs, len(tc.oids))
				for i, oid := range tc.oids {
					require.Equal(t, int32(oid), outputs[i].Typ.Id)
				}
			})
		}
	}
}

func TestBindLagLeadDefaultCommonType(t *testing.T) {
	t.Run("long string default widens varchar result", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id, name)
			require.Equal(t, int32(12), expr.Typ.Width, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id, name)
			require.Equal(t, int32(12), fn.Args[0].Typ.Width, name)
			require.Equal(t, int32(types.T_varchar), fn.Args[2].Typ.Id, name)
			require.Equal(t, int32(12), fn.Args[2].Typ.Width, name)
		}
	})

	t.Run("decimal default keeps scale over integer input", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_decimal64), expr.Typ.Id, name)
			require.Equal(t, int32(1), expr.Typ.Scale, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_decimal64), fn.Args[0].Typ.Id, name)
			require.Equal(t, int32(1), fn.Args[0].Typ.Scale, name)
		}
	})

	t.Run("decimal value keeps declared precision with integer default", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 4}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				makePlan2Int64ConstExprWithType(-1),
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			// -1 fits the declared integer capacity; do not widen to all of BIGINT.
			require.Equal(t, int32(types.T_decimal128), expr.Typ.Id, name)
			require.Equal(t, int32(10), expr.Typ.Width, name)
			require.Equal(t, int32(4), expr.Typ.Scale, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_decimal128), fn.Args[2].Typ.Id, name)
			require.Equal(t, int32(4), fn.Args[2].Typ.Scale, name)
		}
	})

	t.Run("domainless null default keeps value type", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Typ: planpb.Type{Id: int32(types.T_any)}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lag", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(10), expr.Typ.Width)
	})

	t.Run("shorter default keeps value type", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 3}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lag", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(10), expr.Typ.Width)
	})

	t.Run("two arg form keeps value type", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id, name)
			require.Equal(t, int32(10), expr.Typ.Width, name)
		}
	})
}
