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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalIndexLifetimeFromPublicPlanning(t *testing.T) {
	ctx := newVectorJoinMockCtx(t)
	table := newVectorJoinTableDef(false, false)
	table.Name = "temporal_items"
	table.TblId = 901
	table.Cols[1] = &plan.ColDef{Name: "d", Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6}}
	table.Name2ColIndex = map[string]int32{"id": 0, "d": 1}
	table.Indexes = []*plan.IndexDef{{IndexName: "idx_d", IndexTableName: "idx_d_table", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{"d", "id"}}}
	ctx.tables[table.Name] = table
	ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	ctx.tables["idx_d_table"] = &plan.TableDef{Name: "idx_d_table", TableType: catalog.SystemIndexRel,
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "key", Names: []string{"key"}},
		Name2ColIndex: map[string]int32{"key": 0, "id": 1},
		Cols:          []*plan.ColDef{{Name: "key", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}}, table.Cols[0]}}
	ctx.objects["idx_d_table"] = &plan.ObjectRef{SchemaName: "tpch", ObjName: "idx_d_table", Obj: 902}
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"select id from temporal_items where d < now()", true},
		{"prepare p from 'select id from temporal_items where d < ?'", true},
		{"select id from temporal_items where id = 'a'", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			statements, err := mysql.Parse(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			defer statements[0].Free()
			p, err := BuildPlan(ctx, statements[0], false)
			require.NoError(t, err)
			q := p.GetQuery()
			if prepared := p.GetDcl().GetPrepare(); prepared != nil {
				q = prepared.Plan.GetQuery()
			}
			require.NotNil(t, q)
			require.Equal(t, tc.want, q.GetTemporalIndexNeedsReplan())
		})
	}
}

func TestTemporalIndexHintRequiresWholePlanAndIndexProof(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	datetimeType := Type{Id: int32(types.T_datetime), Scale: 6}
	column := GetColExpr(datetimeType, 1, 0)
	column.GetCol().Name = "d"
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	predicate, err := BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{column, now})
	require.NoError(t, err)
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{1}, FilterList: []*Expr{predicate}, TableDef: &plan.TableDef{
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"d": 0, "id": 1},
		Cols:          []*plan.ColDef{{Name: "d", Typ: datetimeType}, {Name: "id", Typ: Type{Id: int32(types.T_int64)}}},
	}}
	project := &plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{0}, ProjectList: []*Expr{column}}
	query := &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{scan, project}, Steps: []int32{1}}
	index := &plan.IndexDef{IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{"d", "id"}}
	scan.TableDef.Indexes = []*plan.IndexDef{index}
	require.True(t, potentialTemporalIndexAccess(query))
	context := withPreparedSourceBindings(proc.Ctx, nil, []any{"unproved"})
	mock := NewMockCompilerContext(true, proc)
	mock.SetContext(context)
	builder := &QueryBuilder{qry: query, compCtx: mock}
	builder.markTemporalAccessLifetime(1)
	require.True(t, query.GetTemporalIndexNeedsReplan())
	require.True(t, preparedBindingState(context).valueDependent)
	query.TemporalIndexNeedsReplan = false
	unresolved := DeepCopyExpr(predicate)
	unresolved.GetF().Args[1] = &plan.Expr{Typ: Type{Id: int32(types.T_any)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	scan.FilterList[0] = unresolved
	require.True(t, potentialTemporalIndexAccess(query), "an unproved parameter must still prevent reuse of a base choice")
	between := DeepCopyExpr(predicate)
	between.GetF().Func.Obj = function.EncodeOverloadID(function.BETWEEN, 0)
	between.GetF().Args = append(between.GetF().Args, DeepCopyExpr(unresolved.GetF().Args[1]))
	scan.FilterList[0] = between
	require.True(t, potentialTemporalIndexAccess(query), "a potential range must be detected before range lowering")
	in := DeepCopyExpr(predicate)
	in.GetF().Func.Obj = function.EncodeOverloadID(function.IN, 0)
	in.GetF().Args[1] = &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{DeepCopyExpr(unresolved.GetF().Args[1])}}}}
	scan.FilterList[0] = in
	require.True(t, potentialTemporalIndexAccess(query), "a potential IN arm must be detected before lowering")
	scan.TableDef.Indexes = nil
	require.False(t, potentialTemporalIndexAccess(query), "unindexed queries do not need temporal replanning")
	scan.TableDef.Indexes = []*plan.IndexDef{index}
	scan.FilterList[0] = predicate
	scan.FilterList = nil
	project.FilterList = []*plan.Expr{predicate}
	require.True(t, potentialTemporalIndexAccess(query), "detection must precede filter pushdown")
	project.FilterList = nil
	scan.FilterList = []*plan.Expr{predicate}
	indexTable := &plan.TableDef{Cols: []*plan.ColDef{{Name: "key", Typ: Type{Id: int32(types.T_varchar)}}}}
	hint, ok, err := temporalIndexHint(proc.Ctx, query, 0, true, index, indexTable, 2, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, hint.TemporalCandidateFilter)
	require.True(t, hint.GetF().Args[0].TemporalCandidateGuard)
	require.False(t, predicate.TemporalCandidateFilter)
	_, ok, err = temporalIndexHint(proc.Ctx, query, 0, false, index, indexTable, 2, 0)
	require.NoError(t, err)
	require.False(t, ok)
	index.Unique = true
	_, ok, err = temporalIndexHint(proc.Ctx, query, 0, true, index, indexTable, 2, 0)
	require.NoError(t, err)
	require.False(t, ok, "nullable suffix prevents proving unique-index completeness")
	index.Unique = false
	suffix := scan.TableDef.Cols[1]
	scan.TableDef.Cols[1] = nil
	_, ok, err = temporalIndexHint(proc.Ctx, query, 0, true, index, indexTable, 2, 0)
	require.NoError(t, err)
	require.False(t, ok, "incomplete index metadata must decline the hint")
	scan.TableDef.Cols[1] = suffix
	project.NodeType = plan.Node_JOIN
	_, ok, err = temporalIndexHint(proc.Ctx, query, 0, true, index, indexTable, 2, 0)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestTemporalIndexHintExactEqualityPrefix(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	integer := Type{Id: int32(types.T_int64)}
	datetime := Type{Id: int32(types.T_datetime), Scale: 6}
	prefixCol := GetColExpr(integer, 1, 0)
	component := GetColExpr(datetime, 1, 1)
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	equality, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*Expr{prefixCol, MakePlan2Int64ConstExprWithType(7)})
	require.NoError(t, err)
	temporal, err := BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{component, now})
	require.NoError(t, err)
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{1}, FilterList: []*Expr{equality, temporal}, TableDef: &plan.TableDef{
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"p": 0, "d": 1, "id": 2},
		Cols:          []*plan.ColDef{{Name: "p", Typ: integer}, {Name: "d", Typ: datetime}, {Name: "id", Typ: integer}},
	}}
	query := &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{scan}, Steps: []int32{0}}
	index := &plan.IndexDef{IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{"p", "d", "id"}}
	indexTable := &plan.TableDef{Cols: []*plan.ColDef{{Name: "key", Typ: Type{Id: int32(types.T_varchar)}}}}
	hint, ok, err := temporalIndexHintWithPrefix(proc.Ctx, query, 0, true, index, indexTable, 2, []int{0}, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, hint.TemporalCandidateFilter)
	require.True(t, hint.GetF().Args[0].TemporalCandidateGuard)
	prefix, pos, found, err := findTemporalIndexMatch(proc.Ctx, query, 0, true, index)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []int{0}, prefix)
	require.Equal(t, 1, pos)
	_, _, found, err = findTemporalIndexMatch(proc.Ctx, query, 0, false, index)
	require.NoError(t, err)
	require.False(t, found, "unproved parameter provenance cannot select a candidate")
	scan.FilterList = []*Expr{temporal, equality}
	prefix, pos, found, err = findTemporalIndexMatch(proc.Ctx, query, 0, true, index)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []int{1}, prefix)
	require.Zero(t, pos, "discovery must not depend on filter order")
	scan.FilterList = []*Expr{equality, temporal}
	_, ok, err = temporalIndexHintWithPrefix(proc.Ctx, query, 0, true, index, indexTable, 2, []int{1}, 1)
	require.NoError(t, err)
	require.False(t, ok, "a range predicate cannot certify an equality prefix")
}

func TestTemporalIndexMatchMultiplePrefixesAndBoundedWork(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	integer := Type{Id: int32(types.T_int64)}
	datetime := Type{Id: int32(types.T_datetime), Scale: 6}
	a := GetColExpr(integer, 1, 0)
	b := GetColExpr(integer, 1, 1)
	d := GetColExpr(datetime, 1, 2)
	eqA, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*Expr{MakePlan2Int64ConstExprWithType(7), a})
	require.NoError(t, err)
	eqB, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*Expr{b, MakePlan2Int64ConstExprWithType(8)})
	require.NoError(t, err)
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	temporal, err := BindFuncExprImplByPlanExpr(proc.Ctx, ">", []*Expr{now, d})
	require.NoError(t, err)
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, BindingTags: []int32{1}, FilterList: []*Expr{temporal, eqB, eqA}, TableDef: &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}, Name2ColIndex: map[string]int32{"a": 0, "b": 1, "d": 2, "id": 3},
		Cols: []*plan.ColDef{{Name: "a", Typ: integer}, {Name: "b", Typ: integer}, {Name: "d", Typ: datetime}, {Name: "id", Typ: integer}},
	}}
	query := &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{scan}, Steps: []int32{0}}
	index := &plan.IndexDef{IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{"a", "b", "d", "id"}}
	prefix, pos, ok, err := findTemporalIndexMatch(proc.Ctx, query, 0, true, index)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []int{2, 1}, prefix)
	require.Zero(t, pos)
	for len(scan.FilterList) <= 64 {
		scan.FilterList = append(scan.FilterList, eqA)
	}
	_, _, ok, err = findTemporalIndexMatch(proc.Ctx, query, 0, true, index)
	require.NoError(t, err)
	require.False(t, ok)
}
