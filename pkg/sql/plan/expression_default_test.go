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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func expressionDefaultIntType() planpb.Type {
	return planpb.Type{Id: int32(types.T_int64), Width: 64}
}

func expressionDefaultCol(pos int32, relPos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: expressionDefaultIntType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: relPos,
			ColPos: pos,
		}},
	}
}

func expressionDefaultAdd(left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: expressionDefaultIntType(),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "+"},
			Args: []*planpb.Expr{left, right},
		}},
	}
}

func expressionDefaultInt(value int64) *planpb.Expr {
	return makePlan2Int64ConstExprWithType(value)
}

func expressionDefaultRand(t *testing.T) *planpb.Expr {
	randFn, err := function.GetFunctionByName(context.Background(), "rand", nil)
	require.NoError(t, err)
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64), Width: 64},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
			Obj: randFn.GetEncodedOverloadID(), ObjName: "rand",
		}}},
	}
}

func expressionDefaultFindLocalCol(expr *planpb.Expr) *planpb.Expr {
	if expr == nil {
		return nil
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Col:
		if impl.Col != nil && impl.Col.RelPos == 0 {
			return expr
		}
	case *planpb.Expr_F:
		for _, arg := range impl.F.Args {
			if found := expressionDefaultFindLocalCol(arg); found != nil {
				return found
			}
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			if found := expressionDefaultFindLocalCol(item); found != nil {
				return found
			}
		}
	}
	return nil
}

func TestExpressionDefaultBindsAgainstCompleteRowSchema(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(),
		"create table t (id int primary key, a int default 5, b int default (a+1), c int default (b+1))", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.Len(t, cols, 4)
	require.Equal(t, []int32{1}, collectRefColPos(cols[2].GetDefault().GetExpr()))
	require.Equal(t, []int32{2}, collectRefColPos(cols[3].GetDefault().GetExpr()))
	ref := expressionDefaultFindLocalCol(cols[2].GetDefault().GetExpr())
	require.NotNil(t, ref)
	require.Equal(t, cols[1].Typ, ref.Typ)
}

func TestExpressionDefaultAllowsForwardReferenceToBaseColumn(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(),
		"create table forward_base (b int default (a+1), a int)", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 2)
	require.Equal(t, "b", cols[0].Name)
	require.Equal(t, "a", cols[1].Name)
	require.Equal(t, []int32{1}, collectRefColPos(cols[0].GetDefault().GetExpr()))
}

func TestExpressionDefaultRejectsInvalidDependencyGraph(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "self",
			sql:  "create table bad_self (id int primary key, a int default (a+1))",
			want: "cannot refer to itself",
		},
		{
			name: "cycle",
			sql:  "create table bad_cycle (id int primary key, a int default (b+1), b int default (a+1))",
			want: "circular dependency",
		},
		{
			name: "generated",
			sql:  "create table bad_generated (a int, b int as (a+1) stored, c int default (b+1))",
			want: "cannot refer to generated column",
		},
		{
			name: "auto increment",
			sql:  "create table bad_auto (a int auto_increment primary key, b int default (a+1))",
			want: "cannot refer to auto-increment column",
		},
		{
			name: "forward expression default",
			sql:  "create table bad_forward_expr (b int default (a+1), a int default (7))",
			want: "defined after it when that column has an expression default",
		},
		{
			name: "forward current timestamp default",
			sql:  "create table bad_forward_timestamp (b int default (a+1), a timestamp default current_timestamp)",
			want: "defined after it when that column has an expression default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), tt.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDefaultExprExpanderResolvesChainsAndPreservesOuterRefs(t *testing.T) {
	columnExprs := map[int32]*planpb.Expr{
		0: expressionDefaultInt(10),
		1: expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1)),
		2: expressionDefaultAdd(expressionDefaultCol(1, 0), expressionDefaultInt(1)),
	}

	expanded, err := expandDefaultExprWithColumnExprs(
		context.Background(), columnExprs[2], columnExprs,
	)
	require.NoError(t, err)
	require.NotNil(t, expanded)
	require.Empty(t, collectRefColPos(expanded))
	require.Equal(t, int64(10), expanded.GetF().GetArgs()[0].GetF().GetArgs()[0].GetLit().GetI64Val())

	outer := expressionDefaultCol(7, 42)
	preserved, err := expandDefaultExprWithColumnExprs(
		context.Background(), outer, columnExprs,
	)
	require.NoError(t, err)
	require.Equal(t, int32(42), preserved.GetCol().GetRelPos())
	require.Equal(t, int32(7), preserved.GetCol().GetColPos())
}

func TestDefaultExprExpanderRejectsCycle(t *testing.T) {
	columnExprs := map[int32]*planpb.Expr{
		0: expressionDefaultAdd(expressionDefaultCol(1, 0), expressionDefaultInt(1)),
		1: expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1)),
	}

	_, err := expandDefaultExprWithColumnExprs(
		context.Background(), columnExprs[0], columnExprs,
	)
	require.ErrorContains(t, err, "circular dependency")
}

func TestMaterializedDefaultProjectionKeepsDependencyBoundedAndStable(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
	nodeCtx := NewBindContext(builder, nil)
	intTyp := expressionDefaultIntType()
	childID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_VALUE_SCAN,
		TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
			{Name: "source", Typ: intTyp},
		}},
	}, nodeCtx)

	const depth = 128
	projection := make([]*planpb.Expr, depth)
	colIdxToProjPos := make(map[int32]int32, depth)
	expressions := make(map[int32]*planpb.Expr, depth)
	materialize := make(map[int32]bool, depth)
	order := make([]int32, depth)
	for i := 0; i < depth; i++ {
		colIdx := int32(i)
		colIdxToProjPos[colIdx] = colIdx
		materialize[colIdx] = true
		order[i] = colIdx
		if i == 0 {
			expressions[colIdx] = expressionDefaultInt(7)
		} else {
			expressions[colIdx] = expressionDefaultAdd(
				expressionDefaultCol(int32(i-1), 0), expressionDefaultInt(1))
		}
		null := makePlan2NullConstExprWithType()
		null.Typ = intTyp
		projection[i] = null
	}

	lastID, finalTag, err := builder.appendMaterializedExprProjections(
		nodeCtx, childID, builder.genNewBindTag(), projection,
		colIdxToProjPos, expressions, materialize, order,
	)
	require.NoError(t, err)
	// One initial image plus one fixed-width stage per dependency. The plan
	// stays linear in schema depth instead of embedding an exponentially copied
	// expression tree.
	require.Equal(t, depth+2, len(builder.qry.Nodes)-int(childID))
	final := builder.qry.Nodes[lastID]
	require.Equal(t, finalTag, final.BindingTags[0])
	require.Equal(t, depth, len(final.ProjectList))
	for i := 1; i < depth; i++ {
		stage := builder.qry.Nodes[childID+2+int32(i)]
		arg := stage.ProjectList[i].GetF().GetArgs()[0].GetCol()
		require.NotNil(t, arg)
		previous := builder.qry.Nodes[stage.Children[0]]
		require.Equal(t, previous.BindingTags[0], arg.RelPos)
		require.Equal(t, int32(i-1), arg.ColPos)
	}
}

func TestMaterializedProjectionStagesVolatileGeneratedDependency(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
	nodeCtx := NewBindContext(builder, nil)
	floatTyp := planpb.Type{Id: int32(types.T_float64), Width: 64}
	childID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_VALUE_SCAN,
		TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
			{Name: "a", Typ: floatTyp},
		}},
	}, nodeCtx)

	randExpr := expressionDefaultRand(t)
	generatedExpr := expressionDefaultAdd(
		&planpb.Expr{Typ: floatTyp, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
		expressionDefaultInt(1),
	)
	generatedExpr.Typ = floatTyp
	projection := []*planpb.Expr{randExpr, makePlan2NullConstExprWithType()}
	projection[1].Typ = floatTyp
	colIdxToProjPos := map[int32]int32{0: 0, 1: 1}
	expressions := map[int32]*planpb.Expr{0: randExpr, 1: generatedExpr}

	lastID, finalTag, err := builder.appendMaterializedExprProjections(
		nodeCtx, childID, builder.genNewBindTag(), projection,
		colIdxToProjPos, expressions, nil, nil,
	)
	require.NoError(t, err)
	require.Equal(t, finalTag, builder.qry.Nodes[lastID].BindingTags[0])
	// The generated column must read a's first-stage value. If it were inlined,
	// the plan would contain only the initial projection and RAND would execute
	// twice for one inserted row.
	require.Equal(t, 3, len(builder.qry.Nodes)-int(childID))
	stage := builder.qry.Nodes[lastID]
	arg := stage.ProjectList[1].GetF().GetArgs()[0].GetCol()
	require.NotNil(t, arg)
	require.Equal(t, builder.qry.Nodes[childID+1].BindingTags[0], arg.RelPos)
	require.Equal(t, int32(0), arg.ColPos)
}

func TestMaterializedProjectionGroupsIndependentDependencyLevels(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, true)
	nodeCtx := NewBindContext(builder, nil)
	intTyp := expressionDefaultIntType()
	childID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_VALUE_SCAN,
		TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
			{Name: "source", Typ: intTyp},
		}},
	}, nodeCtx)

	// Columns 0 and 1 are independent roots; columns 2 and 3 each depend on
	// one root. The two roots can share a projection and the two dependents can
	// share the next projection without reading a sibling from the same stage.
	projection := make([]*planpb.Expr, 4)
	colIdxToProjPos := make(map[int32]int32, len(projection))
	expressions := make(map[int32]*planpb.Expr, len(projection))
	materialize := make(map[int32]bool, len(projection))
	for i := range projection {
		colIdx := int32(i)
		colIdxToProjPos[colIdx] = colIdx
		materialize[colIdx] = true
		null := makePlan2NullConstExprWithType()
		null.Typ = intTyp
		projection[i] = null
	}
	expressions[0] = expressionDefaultInt(7)
	expressions[1] = expressionDefaultInt(11)
	expressions[2] = expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1))
	expressions[3] = expressionDefaultAdd(expressionDefaultCol(1, 0), expressionDefaultInt(1))

	_, finalTag, err := builder.appendMaterializedExprProjections(
		nodeCtx, childID, builder.genNewBindTag(), projection,
		colIdxToProjPos, expressions, materialize, []int32{0, 1, 2, 3},
	)
	require.NoError(t, err)
	// Child + initial image + one stage for each dependency level.
	require.Equal(t, 4, len(builder.qry.Nodes)-int(childID))
	firstStage := builder.qry.Nodes[childID+2]
	secondStage := builder.qry.Nodes[childID+3]
	require.Equal(t, finalTag, secondStage.BindingTags[0])
	require.Equal(t, int64(7), firstStage.ProjectList[0].GetLit().GetI64Val())
	require.Equal(t, int64(11), firstStage.ProjectList[1].GetLit().GetI64Val())
	for _, pos := range []int32{2, 3} {
		arg := secondStage.ProjectList[pos].GetF().GetArgs()[0].GetCol()
		require.NotNil(t, arg)
		require.Equal(t, firstStage.BindingTags[0], arg.RelPos)
	}
}

func TestDefaultExprExpanderHonorsCancellationBeforeExpansion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	expander := newDefaultExprExpander(ctx, func(int32) (*planpb.Expr, bool) {
		return expressionDefaultInt(1), true
	})
	_, err := expander.expandExpr(expressionDefaultAdd(
		expressionDefaultCol(0, 0), expressionDefaultInt(1)))
	require.ErrorIs(t, err, context.Canceled)
}

func TestDefaultExprExpanderHonorsAggregateExpansionBudget(t *testing.T) {
	expander := newDefaultExprExpander(context.Background(), func(colIdx int32) (*planpb.Expr, bool) {
		return nil, false
	})
	expander.maxNodes = 16
	expr := expressionDefaultInt(0)
	for i := int64(1); i <= 32; i++ {
		expr = expressionDefaultAdd(expr, expressionDefaultInt(i))
	}
	_, err := expander.expandExpr(expr)
	require.ErrorContains(t, err, "planner limit")
}

func TestInsertExpressionDefaultReadsMaterializedVolatileDependency(t *testing.T) {
	mock := NewMockOptimizer(true)
	const (
		tableName = "expression_default_dml"
		tableID   = uint64(29001)
	)
	stmt, err := mysql.ParseOne(context.Background(),
		"create table expression_default_dml (id int primary key, a double default (rand()), b double default (a))", 1)
	require.NoError(t, err)
	createPlan, err := BuildPlan(mock.CurrentContext(), stmt, false)
	require.NoError(t, err)
	stmt.Free()
	tableDef := createPlan.GetDdl().GetCreateTable().GetTableDef()
	tableDef.Name = tableName
	tableDef.TblId = tableID
	qualifiedName := mockQualifiedTableName("tpch", tableName)
	objRef := &planpb.ObjectRef{SchemaName: "tpch", ObjName: tableName, Obj: int64(tableID)}
	mock.ctxt.tables[tableName] = tableDef
	mock.ctxt.objects[tableName] = objRef
	mock.ctxt.tablesByQualifiedName[qualifiedName] = tableDef
	mock.ctxt.objectsByQualifiedName[qualifiedName] = objRef
	mock.ctxt.legacyTableOwners[tableName] = qualifiedName
	mock.ctxt.legacyObjectOwners[tableName] = qualifiedName
	mock.ctxt.id2name[tableID] = qualifiedName

	logicPlan, err := runOneStmt(mock, t,
		"insert into expression_default_dml(id) values (1)")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	foundMaterializedDependency := false
	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_PROJECT || len(node.ProjectList) < 3 {
			continue
		}
		bExpr := node.ProjectList[2]
		bCol := bExpr.GetCol()
		if bCol == nil || len(node.Children) != 1 {
			continue
		}
		child := query.Nodes[node.Children[0]]
		if bCol.ColPos != 1 {
			continue
		}
		// createQuery may normalize a single-child relation to RelPos=0 and
		// remove the planner-only binding tag. Before normalization the same
		// reference points at the preceding stage's binding tag.
		if len(child.BindingTags) > 0 && bCol.RelPos != child.BindingTags[0] {
			continue
		}
		foundMaterializedDependency = true
		break
	}
	require.True(t, foundMaterializedDependency,
		"the dependent default must read the preceding materialized a value")

	// When the dependency is omitted from the user column list, VALUE_SCAN must
	// carry it as an internal input column. Otherwise b's DEFAULT(a) would
	// inline rand() and the later table projection would evaluate a second,
	// unrelated rand() for the stored a value.
	logicPlan, err = runOneStmt(mock, t,
		"insert into expression_default_dml(id, b) values (1, default)")
	require.NoError(t, err)
	query = logicPlan.GetQuery()
	var valueScan *planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_VALUE_SCAN {
			valueScan = node
			break
		}
	}
	require.NotNil(t, valueScan)
	require.Len(t, valueScan.TableDef.Cols, 3,
		"the omitted a dependency must be carried through VALUE_SCAN")
	require.Len(t, valueScan.RowsetData.Cols, 3)
	require.Equal(t, []int32{2}, collectRefColPos(valueScan.RowsetData.Cols[1].Data[0].Expr),
		"b's row expression must read the appended a input, not inline rand()")
}

func TestSequentialUpdateDefaultReadsCurrentRowImage(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_UPDATE, NewMockCompilerContext(true), false, true)
	nodeCtx := NewBindContext(builder, nil)
	floatTyp := planpb.Type{Id: int32(types.T_float64), Width: 64}
	rowIDTyp := planpb.Type{Id: int32(types.T_Rowid), Width: 16}
	tableDef := &TableDef{
		Name: "expression_default_update",
		Name2ColIndex: map[string]int32{
			"a": 0, "b": 1, catalog.Row_ID: 2,
		},
		Cols: []*ColDef{
			{Name: "a", Typ: floatTyp, Default: &planpb.Default{Expr: expressionDefaultRand(t)}},
			{Name: "b", Typ: floatTyp, Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)}},
			{Name: catalog.Row_ID, Typ: rowIDTyp, Hidden: true},
		},
		Pkey: &PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
	}
	selectTag := builder.genNewBindTag()
	selectNode := &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{selectTag},
		ProjectList: []*planpb.Expr{
			{Typ: floatTyp, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: selectTag, ColPos: 0}}},
			{Typ: floatTyp, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: selectTag, ColPos: 1}}},
			{Typ: rowIDTyp, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: selectTag, ColPos: 2}}},
		},
	}
	childID := builder.appendNode(selectNode, nodeCtx)
	oldColName2Idx := make(map[string]int32)
	newColName2Idx := make(map[string]int32)
	_, _, _, err := builder.appendSequentialSingleTableUpdateAssignments(
		nodeCtx,
		childID,
		selectNode,
		selectTag,
		tableDef,
		"expression_default_update",
		[]UpdateAssignment{
			{Column: "a", Expr: &tree.DefaultVal{}},
			{Column: "b", Expr: &tree.DefaultVal{}},
		},
		false,
		oldColName2Idx,
		newColName2Idx,
	)
	require.NoError(t, err)

	var countRand func(*planpb.Expr) int
	countRand = func(expr *planpb.Expr) int {
		if expr == nil {
			return 0
		}
		count := 0
		switch impl := expr.Expr.(type) {
		case *planpb.Expr_F:
			if impl.F != nil && impl.F.Func != nil && impl.F.Func.ObjName == "rand" {
				count++
			}
			if impl.F != nil {
				for _, arg := range impl.F.Args {
					count += countRand(arg)
				}
			}
		case *planpb.Expr_List:
			if impl.List != nil {
				for _, item := range impl.List.List {
					count += countRand(item)
				}
			}
		}
		return count
	}
	count := 0
	for _, node := range builder.qry.Nodes {
		for _, expr := range node.ProjectList {
			count += countRand(expr)
		}
	}
	// RAND is the source default for a, and b=DEFAULT must read that
	// materialized value instead of carrying a second RAND root.
	require.Equal(t, 1, count)
}

func TestExpandDefaultExprsInValueScanIsRowLocal(t *testing.T) {
	tableDef := &TableDef{
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
		Cols: []*ColDef{
			{Name: "a", Typ: expressionDefaultIntType()},
			{
				Name: "b",
				Typ:  expressionDefaultIntType(),
				Default: &planpb.Default{Expr: expressionDefaultAdd(
					expressionDefaultCol(0, 0), expressionDefaultInt(1),
				)},
			},
		},
	}
	rowset := &planpb.RowsetData{
		RowCount: 2,
		Cols: []*planpb.ColData{
			{Data: []*planpb.RowsetExpr{
				{Expr: expressionDefaultInt(10)},
				{Expr: expressionDefaultInt(20)},
			}},
			{Data: []*planpb.RowsetExpr{
				{Expr: expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1))},
				{Expr: expressionDefaultInt(99)},
			}},
		},
	}

	require.NoError(t, expandDefaultExprsInValueScan(
		context.Background(), tableDef, []string{"a", "b"}, rowset,
	))
	// Supplied values remain row-local references so VALUE_SCAN reads the
	// materialized source once; the reference is remapped to the input vector
	// position rather than inlined (which would replay volatile expressions).
	require.Equal(t, []int32{0}, collectRefColPos(rowset.Cols[1].Data[0].Expr))
	require.Equal(t, int32(0), rowset.Cols[1].Data[0].Expr.GetF().GetArgs()[0].GetCol().GetColPos())
	require.Equal(t, int64(99), rowset.Cols[1].Data[1].Expr.GetLit().GetI64Val())
}

func TestExpandDefaultExprsInValueScanRemapsExplicitColumnOrder(t *testing.T) {
	intTyp := expressionDefaultIntType()
	tableDef := &TableDef{
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
		Cols: []*ColDef{
			{Name: "a", Typ: intTyp},
			{Name: "b", Typ: intTyp},
		},
	}
	// The synthetic VALUE_SCAN vectors follow the explicit INSERT order b,a;
	// the expression is still written in table-column coordinates initially.
	rowset := &planpb.RowsetData{
		RowCount: 1,
		Cols: []*planpb.ColData{
			{Data: []*planpb.RowsetExpr{{Expr: expressionDefaultAdd(
				expressionDefaultCol(0, 0), expressionDefaultInt(1),
			)}}},
			{Data: []*planpb.RowsetExpr{{Expr: expressionDefaultInt(7)}}},
		},
	}
	require.NoError(t, expandDefaultExprsInValueScan(
		context.Background(), tableDef, []string{"b", "a"}, rowset,
	))
	require.Equal(t, int32(1), rowset.Cols[0].Data[0].Expr.GetF().GetArgs()[0].GetCol().GetColPos())
}

func TestValidateDefaultColumnDependenciesCoversInvalidAndSharedGraphs(t *testing.T) {
	ctx := context.Background()
	intTyp := expressionDefaultIntType()
	autoTyp := intTyp
	autoTyp.AutoIncr = true

	tests := []struct {
		name string
		cols []*planpb.ColDef
		want string
	}{
		{
			name: "negative reference",
			cols: []*planpb.ColDef{{
				Name:    "a",
				Typ:     intTyp,
				Default: &planpb.Default{Expr: expressionDefaultCol(-1, 0)},
			}},
			want: "invalid column position -1",
		},
		{
			name: "out of range reference",
			cols: []*planpb.ColDef{{
				Name:    "a",
				Typ:     intTyp,
				Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)},
			}},
			want: "invalid column position 1",
		},
		{
			name: "nil referenced column",
			cols: []*planpb.ColDef{
				{Name: "a", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)}},
				nil,
			},
			want: "invalid column position 1",
		},
		{
			name: "generated reference",
			cols: []*planpb.ColDef{
				{Name: "a", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)}},
				{Name: "g", Typ: intTyp, GeneratedCol: &planpb.GeneratedCol{}},
			},
			want: "cannot refer to generated column",
		},
		{
			name: "auto increment reference",
			cols: []*planpb.ColDef{
				{Name: "a", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)}},
				{Name: "id", Typ: autoTyp},
			},
			want: "cannot refer to auto-increment column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDefaultColumnDependencies(ctx, tt.cols)
			require.ErrorContains(t, err, tt.want)
		})
	}

	// A shared dependency must be visited once and then accepted from the
	// second branch. All expression dependencies point backwards, so this is a
	// valid graph rather than a forward-reference rejection.
	shared := []*planpb.ColDef{
		{Name: "base", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultInt(1)}},
		{Name: "left", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)}},
		{Name: "right", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)}},
		{Name: "top", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultAdd(
			expressionDefaultCol(1, 0), expressionDefaultCol(2, 0),
		)}},
	}
	require.NoError(t, validateDefaultColumnDependencies(ctx, shared))
	require.NoError(t, validateDefaultColumnDependencies(ctx, nil))
}

func TestDefaultExprExpanderAndProjectionBoundaryCases(t *testing.T) {
	ctx := context.Background()
	literal := expressionDefaultInt(10)
	expander := newDefaultExprExpander(ctx, func(colIdx int32) (*planpb.Expr, bool) {
		if colIdx == 0 {
			return literal, true
		}
		return nil, false
	})

	nilExpr, err := expander.expandExpr(nil)
	require.NoError(t, err)
	require.Nil(t, nilExpr)

	unresolved := expressionDefaultCol(9, 0)
	got, err := expander.expandExpr(unresolved)
	require.NoError(t, err)
	require.Equal(t, int32(9), got.GetCol().GetColPos())

	withNilCol := &planpb.Expr{
		Typ:  expressionDefaultIntType(),
		Expr: &planpb.Expr_Col{Col: nil},
	}
	got, err = expander.expandExpr(withNilCol)
	require.NoError(t, err)
	require.NotNil(t, got.GetCol())

	outer := expressionDefaultCol(4, 2)
	got, err = expander.expandExpr(outer)
	require.NoError(t, err)
	require.Equal(t, int32(2), got.GetCol().GetRelPos())

	list := &planpb.Expr{
		Typ: expressionDefaultIntType(),
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			expressionDefaultCol(0, 0),
			outer,
		}}},
	}
	got, err = expander.expandExpr(list)
	require.NoError(t, err)
	require.Equal(t, int64(10), got.GetList().GetList()[0].GetLit().GetI64Val())
	require.Equal(t, int32(2), got.GetList().GetList()[1].GetCol().GetRelPos())

	// The second expansion uses the memoized copy, while an unknown column
	// has no replacement and expandColumn returns nil.
	first, err := expander.expandColumn(0)
	require.NoError(t, err)
	second, err := expander.expandColumn(0)
	require.NoError(t, err)
	require.Equal(t, first, second)
	missing, err := expander.expandColumn(100)
	require.NoError(t, err)
	require.Nil(t, missing)

	require.False(t, exprHasLocalColumnRef(nil))
	require.False(t, exprHasLocalColumnRef(&planpb.Expr{
		Expr: &planpb.Expr_Col{Col: nil},
	}))
	require.False(t, exprHasLocalColumnRef(outer))
	require.True(t, exprHasLocalColumnRef(list))
	require.False(t, exprHasLocalColumnRef(literal))

	projection := []*planpb.Expr{expressionDefaultCol(0, 0), nil}
	require.NoError(t, expandDefaultExprsInProjection(
		ctx, projection, []int32{-1, 2, 1, 0},
		map[int32]*planpb.Expr{0: literal},
	))
	require.Equal(t, int64(10), projection[0].GetLit().GetI64Val())

	err = expandDefaultExprsInProjection(
		ctx,
		[]*planpb.Expr{expressionDefaultCol(0, 0)},
		[]int32{0},
		map[int32]*planpb.Expr{0: expressionDefaultCol(0, 0)},
	)
	require.ErrorContains(t, err, "circular dependency")
}

func TestExpandDefaultExprsInValueScanRejectsMalformedInputs(t *testing.T) {
	ctx := context.Background()
	intTyp := expressionDefaultIntType()
	tableDef := &TableDef{
		Name2ColIndex: map[string]int32{"a": 0},
		Cols: []*ColDef{
			{Name: "a", Typ: intTyp},
			{Name: "b", Typ: intTyp},
		},
	}

	require.NoError(t, expandDefaultExprsInValueScan(ctx, nil, []string{"a"}, nil))
	require.NoError(t, expandDefaultExprsInValueScan(ctx, tableDef, nil, &planpb.RowsetData{}))
	require.NoError(t, expandDefaultExprsInValueScan(ctx, tableDef, []string{"a"}, &planpb.RowsetData{
		RowCount: 0,
	}))

	// The fallback lookup is case-insensitive, and rows without local refs
	// take the cheap no-expansion path.
	require.NoError(t, expandDefaultExprsInValueScan(
		ctx, tableDef, []string{"A"}, &planpb.RowsetData{
			RowCount: 1,
			Cols: []*planpb.ColData{{Data: []*planpb.RowsetExpr{{
				Expr: expressionDefaultInt(7),
			}}}},
		},
	))

	bad := &planpb.RowsetData{RowCount: 1}
	err := expandDefaultExprsInValueScan(ctx, tableDef, []string{"missing"}, bad)
	require.ErrorContains(t, err, "does not exist")

	badPositionTable := &TableDef{
		Name2ColIndex: map[string]int32{"bad": -1},
		Cols:          []*ColDef{{Name: "a", Typ: intTyp}},
	}
	require.ErrorContains(t,
		expandDefaultExprsInValueScan(ctx, badPositionTable, []string{"bad"}, &planpb.RowsetData{}),
		"does not exist")

	for _, rowset := range []*planpb.RowsetData{
		{RowCount: 1},
		{RowCount: 1, Cols: []*planpb.ColData{{}}},
		{RowCount: 1, Cols: []*planpb.ColData{{Data: []*planpb.RowsetExpr{nil}}}},
	} {
		require.ErrorContains(t,
			expandDefaultExprsInValueScan(ctx, tableDef, []string{"a"}, rowset),
			"invalid VALUES rowset")
	}

	// A local reference to a non-nullable column without a default exercises
	// the error returned by the default resolver rather than a successful row
	// expansion.
	resolveErrorRowset := &planpb.RowsetData{
		RowCount: 1,
		Cols: []*planpb.ColData{{Data: []*planpb.RowsetExpr{{
			Expr: expressionDefaultCol(1, 0),
		}}}},
	}
	tableDef.Cols[1].Typ.NotNullable = true
	require.ErrorContains(t,
		expandDefaultExprsInValueScan(ctx, tableDef, []string{"a"}, resolveErrorRowset),
		"invalid default value")
}

func TestGetDefaultExprHandlesMissingAndNullableMetadata(t *testing.T) {
	ctx := context.Background()
	intTyp := expressionDefaultIntType()

	_, err := getDefaultExpr(ctx, nil)
	require.ErrorContains(t, err, "missing column definition")

	nonNullable := &planpb.ColDef{Name: "required", Typ: planpb.Type{
		Id: int32(types.T_int64), NotNullable: true,
	}}
	_, err = getDefaultExpr(ctx, nonNullable)
	require.ErrorContains(t, err, "invalid default value")

	nullable := &planpb.ColDef{Name: "nullable", Typ: intTyp}
	expr, err := getDefaultExpr(ctx, nullable)
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())
	require.False(t, expr.Typ.NotNullable)

	auto := &planpb.ColDef{Name: "id", Typ: planpb.Type{
		Id: int32(types.T_int64), NotNullable: true, AutoIncr: true,
	}}
	expr, err = getDefaultExpr(ctx, auto)
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())
	require.False(t, expr.Typ.NotNullable)

	invalidMetadata := &planpb.ColDef{
		Name: "bad",
		Typ:  intTyp,
		Default: &planpb.Default{
			NullAbility: false,
		},
	}
	_, err = getDefaultExpr(ctx, invalidMetadata)
	require.ErrorContains(t, err, "invalid default value")

	autoInvalidMetadata := &planpb.ColDef{
		Name: "auto",
		Typ:  planpb.Type{Id: int32(types.T_int64), AutoIncr: true},
		Default: &planpb.Default{
			NullAbility: false,
		},
	}
	expr, err = getDefaultExpr(ctx, autoInvalidMetadata)
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())

	expr, err = getDefaultExpr(ctx, &planpb.ColDef{
		Name:    "explicit_null",
		Typ:     intTyp,
		Default: &planpb.Default{NullAbility: true},
	})
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())

	expr, err = getDefaultExpr(ctx, &planpb.ColDef{
		Name:    "explicit_value",
		Typ:     intTyp,
		Default: &planpb.Default{Expr: expressionDefaultInt(7)},
	})
	require.NoError(t, err)
	require.Equal(t, int64(7), expr.GetLit().GetI64Val())
}

func TestDefaultBinderWithColumnsUsesSourceTypeAndRejectsInvalidNames(t *testing.T) {
	ctx := context.Background()
	sourceTyp := planpb.Type{Id: int32(types.T_int32), Width: 32}
	targetTyp := planpb.Type{Id: int32(types.T_float64)}
	binder := NewDefaultBinderWithColumns(ctx, targetTyp, []*ColDef{
		{Name: "Source", Typ: sourceTyp},
	})

	expr, err := binder.BindColRef(tree.NewUnresolvedColName("source"), 0, true)
	require.NoError(t, err)
	require.Equal(t, sourceTyp, expr.Typ)
	require.Equal(t, int32(0), expr.GetCol().GetColPos())

	qualified := tree.NewUnresolvedName(tree.NewCStr("t", 1), tree.NewCStr("source", 1))
	_, err = binder.BindColRef(qualified, 0, true)
	require.ErrorContains(t, err, "qualified column name")

	_, err = binder.BindColRef(tree.NewUnresolvedColName("missing"), 0, true)
	require.ErrorContains(t, err, "does not exist")
}

func TestRemapExpressionDefaultsAndReferencesAfterColumnChanges(t *testing.T) {
	ctx := context.Background()
	typ := expressionDefaultIntType()

	shiftColPosInExpr(nil, 1, 1)
	shiftColPosInExpr(&planpb.Expr{
		Typ:  typ,
		Expr: &planpb.Expr_Col{Col: nil},
	}, 1, 1)
	outer := expressionDefaultCol(2, 1)
	shiftColPosInExpr(outer, 1, 1)
	require.Equal(t, int32(2), outer.GetCol().GetColPos())

	newTable := func() *TableDef {
		return &TableDef{Cols: []*ColDef{
			{
				Name: "first",
				Default: &planpb.Default{Expr: expressionDefaultAdd(
					expressionDefaultCol(0, 0), expressionDefaultCol(2, 0),
				)},
			},
			{
				Name:         "generated",
				GeneratedCol: &planpb.GeneratedCol{Expr: expressionDefaultCol(2, 0)},
			},
			{
				Name:    "outer",
				Default: &planpb.Default{Expr: expressionDefaultCol(0, 1)},
			},
		}}
	}

	inserted := newTable()
	remapGeneratedColExprsAfterInsert(inserted, 1)
	require.Equal(t, []int32{0, 3}, collectRefColPos(inserted.Cols[0].Default.Expr))
	require.Equal(t, []int32{3}, collectRefColPos(inserted.Cols[1].GeneratedCol.Expr))
	require.Nil(t, collectRefColPos(inserted.Cols[2].Default.Expr))

	dropped := newTable()
	remapGeneratedColExprsAfterDrop(dropped, 1)
	require.Equal(t, []int32{0, 1}, collectRefColPos(dropped.Cols[0].Default.Expr))
	require.Equal(t, []int32{1}, collectRefColPos(dropped.Cols[1].GeneratedCol.Expr))

	cols := []*ColDef{{Name: "First"}, {Name: "Second"}}
	require.False(t, exprReferencesColumn(nil, "first", cols))
	require.False(t, exprReferencesColumn(expressionDefaultInt(1), "first", cols))
	require.False(t, exprReferencesColumn(&planpb.Expr{
		Expr: &planpb.Expr_Col{Col: nil},
	}, "first", cols))
	require.False(t, exprReferencesColumn(expressionDefaultCol(0, 1), "first", cols))
	require.False(t, exprReferencesColumn(expressionDefaultCol(9, 0), "first", cols))
	require.True(t, exprReferencesColumn(expressionDefaultCol(0, 0), "FIRST", cols))
	require.True(t, exprReferencesColumn(expressionDefaultAdd(
		expressionDefaultInt(1), expressionDefaultCol(1, 0),
	), "second", cols))
	require.False(t, exprReferencesColumn(&planpb.Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			expressionDefaultInt(1),
		}}},
	}, "first", cols))

	defaultDependent := &TableDef{
		Cols: []*ColDef{{
			Name:    "value",
			Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)},
		}},
	}
	require.ErrorContains(t,
		checkColumnWithDefaultDependency(ctx, defaultDependent, "value"),
		"depends on it")
	require.NoError(t, checkColumnWithDefaultDependency(ctx, &TableDef{
		Cols: []*ColDef{{Name: "value", Default: &planpb.Default{Expr: expressionDefaultInt(1)}}},
	}, "value"))
}

func TestModifyColPositionRemapsChangedColumnDefaults(t *testing.T) {
	ctx := context.Background()
	typ := expressionDefaultIntType()
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "a", Typ: typ},
		{Name: "b", Typ: typ},
		{Name: "c", Typ: typ},
	}}
	oCol := tableDef.Cols[1]
	nCol := &planpb.ColDef{
		Name:    "b",
		Typ:     typ,
		Default: &planpb.Default{Expr: expressionDefaultCol(2, 0)},
	}
	require.NoError(t, modifyColPosition(
		ctx, tableDef, oCol, nCol,
		&tree.ColumnPosition{Typ: tree.ColumnPositionFirst},
	))
	require.Equal(t, []string{"b", "a", "c"}, []string{
		tableDef.Cols[0].Name, tableDef.Cols[1].Name, tableDef.Cols[2].Name,
	})
	// b's DEFAULT(c) was bound before the move. It must still point at c's
	// final position after the old slot is removed and the new slot inserted.
	require.Equal(t, []int32{2}, collectRefColPos(nCol.Default.Expr))
}

func TestDefaultDependencyDiscoveryHandlesMissingMetadata(t *testing.T) {
	typ := expressionDefaultIntType()
	tableDef := &planpb.TableDef{
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
		Cols: []*planpb.ColDef{
			{Name: "a", Typ: typ}, // implicit NULL default; no catalog metadata
			{Name: "b", Typ: typ, Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)}},
		},
	}
	branches := []*multiInsertBranch{{insertColumns: []string{"b"}}}
	columns, err := multiInsertUnionColumnsWithDefaultDependencies(
		context.Background(), branches, tableDef,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "a"}, columns)
}

func TestExpressionDefaultClassification(t *testing.T) {
	require.False(t, isExpressionDefault(nil))
	require.False(t, isExpressionDefault(&planpb.Default{}))
	require.False(t, isExpressionDefault(&planpb.Default{Expr: expressionDefaultInt(1)}))
	require.True(t, isExpressionDefault(&planpb.Default{
		OriginString: "(1)",
		Expr:         expressionDefaultInt(1),
	}))
	require.True(t, isExpressionDefault(&planpb.Default{Expr: expressionDefaultCol(0, 0)}))
	require.True(t, isExpressionDefault(&planpb.Default{
		Expr: expressionDefaultAdd(expressionDefaultInt(1), expressionDefaultInt(2)),
	}))
}
