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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedInsertSelectKeepsWriteExpressionPhysicalType(t *testing.T) {
	mock := NewMockOptimizer(false)
	source := mock.ctxt.tables["nation"]
	require.NotNil(t, source)
	id := proto.Clone(source.Cols[0]).(*planpb.ColDef)
	id.Name = "id"
	v := proto.Clone(source.Cols[2]).(*planpb.ColDef)
	v.Name = "v"
	g := proto.Clone(v).(*planpb.ColDef)
	g.Name = "g"
	g.Default = &planpb.Default{NullAbility: true}
	generated, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*planpb.Expr{
		{Typ: v.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "v"}}},
		makePlan2Int32ConstExprWithType(1),
	})
	require.NoError(t, err)
	generated.Typ = g.Typ
	g.GeneratedCol = &planpb.GeneratedCol{Expr: generated, IsStored: true}
	rowID := proto.Clone(source.Cols[len(source.Cols)-1]).(*planpb.ColDef)
	rowID.Name = catalog.Row_ID
	table := &planpb.TableDef{
		Name: "issue_29146_gen", TblId: 29146,
		Cols:          []*planpb.ColDef{id, v, g, rowID},
		Name2ColIndex: map[string]int32{"id": 0, "v": 1, "g": 2, catalog.Row_ID: 3},
		Pkey:          &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
	}
	mock.ctxt.tables[table.Name] = table
	mock.ctxt.objects[table.Name] = &planpb.ObjectRef{Obj: int64(table.TblId), ObjName: table.Name}
	mock.ctxt.id2name[table.TblId] = table.Name

	prepared, err := runOneStmt(mock, t,
		"prepare p from 'insert into issue_29146_gen(id,v) "+
			"select n_nationkey,n_regionkey+? from nation where n_nationkey<=?'")
	require.NoError(t, err)
	original := prepared.GetDcl().GetPrepare().Plan
	foundAssignmentCast := false
	require.NoError(t, planpb.VisitExpressionsInOwner(original, func(root *planpb.Expr) error {
		return planpb.VisitExprTree(root, func(expr *planpb.Expr) error {
			if fn := expr.GetF(); fn != nil && len(fn.Args) == 2 && fn.Args[0].GetCol() != nil {
				switch fn.Func.GetObjName() {
				case "cast", "cast_assign", "cast_ignore", "cast_strict":
					foundAssignmentCast = true
				}
			}
			return nil
		})
	}))
	require.True(t, foundAssignmentCast,
		"a prepared numeric INSERT SELECT source needs a physical assignment boundary even when prepare-time types match")
}
