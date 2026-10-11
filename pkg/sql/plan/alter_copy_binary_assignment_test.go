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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyBinaryAssignmentBoundary(t *testing.T) {
	ordinary := t.Context()
	copyContext := context.WithValue(ordinary, defines.AlterCopyOpt{}, &pbplan.AlterCopyOpt{TargetTableName: "copy_target"})
	for _, oid := range []types.T{types.T_binary, types.T_varbinary} {
		t.Run(oid.String(), func(t *testing.T) {
			physicalType := types.New(oid, 4, 0)
			target := makePlan2Type(&physicalType)
			source := MakePlan2StringConstExprWithType("🧪")
			require.Equal(t, "cast", assignmentCastFunctionNameForSource(ordinary, source, target, false, nil))
			expression, err := forceAssignmentCastExprWithProcess(copyContext, source, target, false, nil)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
			// Even an equal inferred type must validate the generated value.
			source.Typ = target
			expression, err = forceAssignmentCastExprWithProcess(copyContext, source, target, false, nil)
			require.NoError(t, err)
			require.NotSame(t, source, expression)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
			targetExpr := &pbplan.Expr{Typ: target, Expr: &pbplan.Expr_T{T: &pbplan.TargetType{}}}
			expression, err = forceCastExpr2WithProcess(copyContext, source, makeTypeByPlan2Type(target), targetExpr, false, nil)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)

			compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
			compiler.SetContext(copyContext)
			builder := NewQueryBuilder(pbplan.Query_SELECT, compiler, false, false)
			stored := &pbplan.Expr{Typ: target, Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 1}}}
			projection := DeepCopyExpr(stored)
			expression, err = builder.forceProjectedAssignmentCastExpr(projection, stored, target, false)
			require.NoError(t, err)
			require.Same(t, projection, expression, "unchanged stored binary values must not be revalidated")
			// A target change, or a computed value with the same inferred type,
			// is a new assignment rather than an unchanged source-column COPY.
			wider := target
			wider.Width++
			expression, err = builder.forceProjectedAssignmentCastExpr(projection, stored, wider, false)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
			expression, err = builder.forceProjectedAssignmentCastExpr(projection, source, target, false)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
			expression, err = builder.applyGeneratedColumnAssignmentCast(stored, false)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
		})
	}
	target := Type{Id: int32(types.T_varbinary), Width: 4}
	for _, value := range []any{nil, "not a COPY option", (*pbplan.AlterCopyOpt)(nil), &pbplan.AlterCopyOpt{}} {
		ctx := ordinary
		if value != nil {
			ctx = context.WithValue(ctx, defines.AlterCopyOpt{}, value)
		}
		require.False(t, alterCopyBinaryAssignment(ctx, target))
	}
	require.False(t, alterCopyBinaryAssignment(copyContext, Type{Id: int32(types.T_varchar), Width: 4}))
}

func TestAlterCopyUnchangedBinaryProjection(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	compiler := &mock.ctxt
	for i, name := range []string{"src", "dst"} {
		def, err := buildTestCreateTableStmt(mock, "create table "+name+"(v binary(1))")
		require.NoError(t, err)
		def.TblId = uint64(42 + i)
		// Engine resolution adds row identity to persisted CREATE definitions.
		def.Cols = append(def.Cols, &pbplan.ColDef{Name: catalog.Row_ID, Hidden: true, Typ: pbplan.Type{Id: int32(types.T_Rowid)}})
		compiler.tables[name] = def
		compiler.objects[name] = &pbplan.ObjectRef{SchemaName: "tpch", ObjName: name, Obj: int64(def.TblId)}
	}
	compiler.SetContext(context.WithValue(compiler.GetContext(), defines.AlterCopyOpt{}, &pbplan.AlterCopyOpt{TargetTableName: "dst"}))
	stmt, err := mysql.ParseOne(t.Context(), "insert into dst select v from src", 1)
	require.NoError(t, err)
	defer stmt.Free()
	built, err := BuildPlan(compiler, stmt, false)
	require.NoError(t, err)
	casts := 0
	var visit func(*pbplan.Expr)
	visit = func(e *pbplan.Expr) {
		if f := e.GetF(); f != nil {
			if f.Func.ObjName == "cast_strict" {
				casts++
			}
			for _, arg := range f.Args {
				visit(arg)
			}
		}
	}
	for _, node := range built.GetQuery().Nodes {
		for _, e := range node.ProjectList {
			visit(e)
		}
	}
	require.Zero(t, casts, "unchanged binary COPY must preserve the source representation")
}
