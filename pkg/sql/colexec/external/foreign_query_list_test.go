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

package external

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// foreignQueryTestNode builds a FOREIGN_TB scan node whose last column is the
// hidden __mo_query column, with physical columns before it.
func foreignQueryTestNode(physicalCols ...string) *plan.Node {
	cols := make([]*plan.ColDef, 0, len(physicalCols)+1)
	positions := make(map[string]int32, len(physicalCols))
	for i, name := range physicalCols {
		cols = append(cols, &plan.ColDef{
			ColId: uint64(i + 1),
			Name:  name,
			Typ:   plan.Type{Id: int32(types.T_varchar)},
		})
		positions[name] = int32(i)
	}
	cols = append(cols, &plan.ColDef{
		ColId: catalog.ExternalQueryColId,
		Name:  catalog.ExternalQuery,
		Typ:   plan.Type{Id: int32(types.T_varchar)},
	})
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{Cols: cols},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_FOREIGN_TB),
			TbColToDataCol: positions,
		},
	}
}

func foreignQueryFn(name string, id int64, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name, Obj: id},
			Args: args,
		}},
	}
}

func foreignQueryExprList(items ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_tuple)},
		Expr: &plan.Expr_List{List: &plan.ExprList{List: items}},
	}
}

func TestDeriveForeignQueryList(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := context.Background()
	node := foreignQueryTestNode("id", "name")
	// __mo_query is the last column: position 2
	queryCol := filePruningColumn(2, catalog.ExternalQuery)
	physCol := filePruningColumn(0, "id")
	lit := filePruningStringLiteral

	eqID := int64(function.EQUAL) << 32
	inID := int64(function.IN) << 32
	orID := int64(function.OR) << 32

	eq := func(l, r *plan.Expr) *plan.Expr { return foreignQueryFn("=", eqID, l, r) }

	// = with the column on either side
	node.FilterList = []*plan.Expr{eq(queryCol, lit("q1"))}
	got, err := DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Equal(t, []string{"q1"}, got)

	node.FilterList = []*plan.Expr{eq(lit("q1"), queryCol)}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Equal(t, []string{"q1"}, got)

	// IN with an expression list; duplicates removed
	node.FilterList = []*plan.Expr{
		foreignQueryFn("in", inID, queryCol, foreignQueryExprList(lit("q1"), lit("q2"), lit("q1"))),
	}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Equal(t, []string{"q1", "q2"}, got)

	// OR of two equalities -> union
	node.FilterList = []*plan.Expr{
		foreignQueryFn("or", orID, eq(queryCol, lit("q1")), eq(queryCol, lit("q2"))),
	}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Equal(t, []string{"q1", "q2"}, got)

	// two separate conjuncts both generate (their intersection is applied
	// later by FilterFileList; derivation itself is a union of candidates)
	node.FilterList = []*plan.Expr{eq(queryCol, lit("q1")), eq(queryCol, lit("q2"))}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Equal(t, []string{"q1", "q2"}, got)

	// a row-level conjunct (physical column) generates nothing
	node.FilterList = []*plan.Expr{eq(physCol, lit("x"))}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Empty(t, got)

	// = against another column is not a generator
	node.FilterList = []*plan.Expr{eq(queryCol, physCol)}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Empty(t, got)

	// OR with one non-generating branch generates nothing at all
	node.FilterList = []*plan.Expr{
		foreignQueryFn("or", orID, eq(queryCol, lit("q1")), eq(physCol, lit("x"))),
	}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Empty(t, got)

	// NULL literal is skipped (NULL never equals anything)
	nullLit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
	}
	node.FilterList = []*plan.Expr{eq(queryCol, nullLit)}
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Empty(t, got)

	// empty filter list -> no candidates (caller falls back to 'query' option)
	node.FilterList = nil
	got, err = DeriveForeignQueryList(ctx, node, proc)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestForeignQueryIsFileLevel proves the generalized classifier accepts
// __mo_query as the file-level column of a FOREIGN_TB scan.
func TestForeignQueryIsFileLevel(t *testing.T) {
	node := foreignQueryTestNode("id")
	queryCol := &plan.ColRef{ColPos: 1, Name: catalog.ExternalQuery}
	require.True(t, isFileLevelColumn(node, queryCol))
	// wrong position
	require.False(t, isFileLevelColumn(node, &plan.ColRef{ColPos: 0, Name: "id"}))
	// wrong ColId for the name
	badNode := foreignQueryTestNode("id")
	badNode.TableDef.Cols[1].ColId = 42
	require.False(t, isFileLevelColumn(badNode, queryCol))
}
