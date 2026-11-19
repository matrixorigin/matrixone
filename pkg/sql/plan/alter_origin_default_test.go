// Copyright 2022 Matrix Origin
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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAlterOriginDefault(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	compiler := mock.CurrentContext()
	frozen := &pb.Literal{Value: &pb.Literal_Bval{Bval: false}}
	col := &pb.ColDef{Name: "flag", OriginName: "flag", Typ: types.T_bool.ToType().PlanType(), Seqnum: 7,
		Default: &pb.Default{NullAbility: true, Expr: MakePlan2BoolConstExprWithType(false), HasOriginDefault: true, OriginDefault: frozen}}
	table := &pb.TableDef{Name: "t1", FeatureFlag: features.SchemaEvolved, Pkey: &pb.PrimaryKeyDef{}, Cols: []*pb.ColDef{col}}
	assertFrozen := func(t *testing.T, def, original *pb.Default) {
		t.Helper()
		require.True(t, def.HasOriginDefault)
		require.Equal(t, frozen, def.OriginDefault)
		require.NotSame(t, original.OriginDefault, def.OriginDefault)
		require.NotSame(t, original.OriginDefault.GetValue(), def.OriginDefault.GetValue())
	}

	t.Run("deep copy", func(t *testing.T) {
		assertFrozen(t, DeepCopyDefault(col.Default), col.Default)
	})
	t.Run("SET then DROP", func(t *testing.T) {
		current := DeepCopyTableDef(table, true)
		alter := &pb.AlterTable{TableDef: table, CopyTableDef: current}
		alterCtx := initAlterTableContext(table, current, "")
		for _, step := range []struct {
			sql  string
			drop bool
		}{
			{"alter table t1 alter column flag set default true", false},
			{"alter table t1 alter column flag drop default", true},
		} {
			previous := current.Cols[0].Default
			clause := mustParseAlterColumnClause(t, compiler, step.sql)
			_, err := AlterColumn(compiler, alter, clause, alterCtx)
			require.NoError(t, err)
			def := current.Cols[0].Default
			assertFrozen(t, def, previous)
			if step.drop {
				require.Nil(t, def.Expr)
				require.Empty(t, def.OriginString)
			} else {
				require.True(t, def.Expr.GetLit().GetBval())
			}
		}
		require.False(t, col.Default.Expr.GetLit().GetBval())
	})
	for _, sql := range []string{
		"alter table t1 modify column flag bool default true",
		"alter table t1 change column flag active bool default true",
	} {
		t.Run(sql, func(t *testing.T) {
			stmts, err := mysql.Parse(compiler.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			var def *tree.ColumnTableDef
			switch clause := stmts[0].(*tree.AlterTable).Options[0].(type) {
			case *tree.AlterTableModifyColumnClause:
				def = clause.NewColumn
			case *tree.AlterTableChangeColumnClause:
				def = clause.NewColumn
			}
			require.NotNil(t, def)
			changed, err := buildColumnAndConstraint(compiler, DeepCopyTableDef(table, true), col, def, col.Typ)
			require.NoError(t, err)
			assertFrozen(t, changed.Default, col.Default)
			require.True(t, changed.Default.Expr.GetLit().GetBval())
			require.Equal(t, uint32(7), changed.Seqnum)
		})
	}
	t.Run("COPY re-materializes the layout", func(t *testing.T) {
		copied, err := buildCopyTableDef(t.Context(), table)
		require.NoError(t, err)
		require.Zero(t, copied.FeatureFlag&features.SchemaEvolved)
		require.False(t, copied.Cols[0].Default.HasOriginDefault)
		require.Nil(t, copied.Cols[0].Default.OriginDefault)
		require.True(t, col.Default.HasOriginDefault)
		for _, sql := range []string{"alter table t1 add column x int", "alter table t1 drop column flag"} {
			stmts, err := mysql.Parse(compiler.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			algorithm, err := ResolveAlterTableAlgorithm(compiler.GetContext(), stmts[0].(*tree.AlterTable).Options, table)
			require.NoError(t, err)
			require.Equal(t, pb.AlterTable_COPY, algorithm)
		}
	})
}
