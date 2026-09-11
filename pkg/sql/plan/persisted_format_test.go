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
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPersistedFormatCatalogCompatibility(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	old, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	for _, version := range []int64{defines.MORPCVersion58, defines.MORPCVersion59} {
		for _, numericType := range []string{"bigint", "decimal(20, 1)", "double"} {
			for _, call := range []string{"format(a, 0)", "format(a, 0, 'en_US')", "lower(format(abs(a), 0))"} {
				t.Run(fmt.Sprintf("v%d/%s/%s", version, numericType, call), func(t *testing.T) {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
					sql := fmt.Sprintf("create table t(a %s, g varchar(100) generated always as (%s) stored, check (%s = '1'))", numericType, call, call)
					stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
					require.NoError(t, err)
					defer stmt.Free()
					built, err := BuildPlan(ctx, stmt, false)
					require.NoError(t, err)
					data, err := proto.Marshal(built.GetDdl().GetCreateTable().GetTableDef())
					require.NoError(t, err)
					// The catalog expression is reused unchanged, even after a downgrade.
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion58)
					var loaded planpb.TableDef
					require.NoError(t, proto.Unmarshal(data, &loaded))
					count := 0
					require.NoError(t, planpb.VisitExpressionsInOwner(&loaded, func(expr *planpb.Expr) error {
						return planpb.VisitExprTree(expr, func(current *planpb.Expr) error {
							fn := current.GetF()
							if fn != nil && fn.Func.GetObjName() == "format" {
								count++
								require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id)
								require.LessOrEqual(t, uint32(fn.Func.Obj), uint32(1))
							}
							return nil
						})
					}))
					require.GreaterOrEqual(t, count, 2, "both CHECK and generated expressions must survive serialization")
					needsV59, err := planpb.RequiresMORPCVersion59NumericFormatArguments(&loaded)
					require.NoError(t, err)
					require.False(t, needsV59)
				})
			}
		}
	}
}

func TestPersistedFormatRoundingAndProjection(t *testing.T) {
	proc := testutil.NewProcess(t)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select format(a, 0)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(proc.Ctx, []string{"a"}, []planpb.Type{{Id: int32(types.T_decimal64), Width: 10, Scale: 1}})
	projection, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)
	persisted := DeepCopyExpr(projection)
	require.NoError(t, preservePersistedFormatCompatibility(proc.Ctx, persisted))
	data, err := proto.Marshal(persisted)
	require.NoError(t, err)
	var loaded planpb.Expr
	require.NoError(t, proto.Unmarshal(data, &loaded))
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 10, 1))
	defer input.Clean(proc.Mp())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(25), false, proc.Mp()))
	input.SetRowCount(1)
	for _, tc := range []struct {
		name     string
		expr     *planpb.Expr
		want     string
		needsV59 bool
	}{
		{"projection retains exact half-up", projection, "3", true},
		{"catalog retains legacy ties-to-even", &loaded, "2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			needs, err := planpb.RequiresMORPCVersion59NumericFormatArguments(tc.expr)
			require.NoError(t, err)
			require.Equal(t, tc.needsV59, needs)
			executor, err := colexec.NewExpressionExecutor(proc, tc.expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestPersistedFormatDefaultCompatibility(t *testing.T) {
	proc := testutil.NewProcess(t)
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"create table t(a varchar(100) default (format(2.5, 0)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	// Public ON UPDATE grammar currently allows only datetime functions.
	// Exercise the catalog builder defensively for callers constructing an AST.
	col.Attributes = append(col.Attributes, tree.NewAttributeOnUpdate(col.Attributes[0].(*tree.AttributeDefault).Expr))
	typ := planpb.Type{Id: int32(types.T_varchar), Width: 100}
	def, err := buildDefaultExpr(col, typ, proc, false)
	require.NoError(t, err)
	update, err := buildOnUpdate(col, typ, proc, false)
	require.NoError(t, err)
	ctas, err := buildCTASDefaultFromOrigin(NewMockCompilerContext(false), typ, true, "format(2.5, 0)")
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
	}{
		{"default", def.Expr}, {"on update", update.Expr}, {"CTAS default", ctas.Expr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.expr)
			require.NoError(t, err)
			var loaded planpb.Expr
			require.NoError(t, proto.Unmarshal(data, &loaded))
			needs, err := planpb.RequiresMORPCVersion59NumericFormatArguments(&loaded)
			require.NoError(t, err)
			require.False(t, needs)
			executor, err := colexec.NewExpressionExecutor(proc, &loaded)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, "2", result.GetStringAt(0))
		})
	}
}
