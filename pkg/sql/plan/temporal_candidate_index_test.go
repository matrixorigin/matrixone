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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
		sql               string
		want              bool
		parts             [][]string
		wantNodeCacheable bool
	}{
		{sql: "select id from temporal_items where d < now()", want: true},
		{sql: "prepare p from 'select id from temporal_items where d < ?'"},
		{sql: "prepare p from 'select id from temporal_items where d = cast(? as datetime(6))'"},
		{sql: "prepare p from 'select id from temporal_items where d < ? limit 1'"},
		{sql: "prepare p from 'select count(*) from temporal_items where d < now(6)'"},
		{sql: "prepare p from 'select id from temporal_items where d < now(6)'"},
		{sql: "select id from temporal_items where d < now(6) limit 1"},
		{sql: "select count(*) from temporal_items where d < now(6)"},
		{sql: "select id from temporal_items where d = cast('2026-01-01' as datetime(6))"},
		{sql: "select id from temporal_items where id = 'a'"},
		// The original explicit CAST is not an admitted eager bound, even on
		// the leading key; a later optimizer fold cannot change that proof.
		{sql: "select id from temporal_items where d < cast('2026-01-01' as timestamp(6))", wantNodeCacheable: true},
		{sql: "select id from temporal_items where d < cast('2026-01-01' as timestamp(6))", parts: [][]string{{"id", "d"}}, wantNodeCacheable: true},
		{sql: "select id from temporal_items where d < cast('2026-01-01' as timestamp(6)) and id > 0", wantNodeCacheable: true},
		{sql: "select id from temporal_items where d < cast('2026-01-01' as timestamp(6)) or id > 0", wantNodeCacheable: true},
		{sql: "select id from temporal_items where d between cast('2026-01-01' as timestamp(6)) and cast('2026-02-01' as timestamp(6))", wantNodeCacheable: true},
		{sql: "select id from temporal_items where d < now(6)", parts: [][]string{{"id", "d"}, {"d", "id"}}, want: true},
		{sql: "select id from temporal_items where d < now(6)", parts: [][]string{{catalog.CreateAlias("d"), "id"}}, want: true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			parts := tc.parts
			if parts == nil {
				parts = [][]string{{"d", "id"}}
			}
			table.Indexes = nil
			for _, indexParts := range parts {
				table.Indexes = append(table.Indexes, &plan.IndexDef{IndexName: "idx_d", IndexTableName: "idx_d_table", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: indexParts})
			}
			statements, err := mysql.Parse(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			defer statements[0].Free()
			p, err := BuildPlan(ctx, statements[0], false, nil)
			require.NoError(t, err)
			q := p.GetQuery()
			if prepared := p.GetDcl().GetPrepare(); prepared != nil {
				q = prepared.Plan.GetQuery()
			}
			require.NotNil(t, q)
			require.Equal(t, tc.want, q.GetExecutionDependentAccess())
			if tc.wantNodeCacheable {
				for _, node := range q.Nodes {
					require.False(t, node.GetNotCacheable())
				}
			}
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
	potential := func(ctx context.Context) bool {
		owner := *query
		owner.ExecutionDependentAccess = false
		mock := NewMockCompilerContext(true, proc)
		mock.SetContext(ctx)
		builder := &QueryBuilder{qry: &owner, compCtx: mock}
		builder.collectOriginalIndexOwner(1)
		return owner.ExecutionDependentAccess
	}
	proofFor := func(ctx context.Context) originalIndexProof {
		path, ok := traceIndexCandidatePath(query)
		require.True(t, ok)
		return proveOriginalIndexPath(ctx, query, path)
	}
	require.True(t, potential(proc.Ctx))
	bindingCtx := withPreparedSourceBindings(proc.Ctx, nil, []any{"unproved"})
	mock := NewMockCompilerContext(true, proc)
	mock.SetContext(bindingCtx)
	builder := &QueryBuilder{qry: query, compCtx: mock}
	builder.collectOriginalIndexOwner(1)
	require.False(t, query.GetExecutionDependentAccess())
	require.False(t, preparedBindingState(bindingCtx).valueDependent)
	query.ExecutionDependentAccess = false
	builder.isPrepareStatement = true
	builder.collectOriginalIndexOwner(1)
	require.False(t, query.GetExecutionDependentAccess(), "known prepared provenance cannot enter temporal access")
	builder.isPrepareStatement = false
	unresolved := DeepCopyExpr(predicate)
	unresolved.GetF().Args[1] = &plan.Expr{Typ: Type{Id: int32(types.T_any)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	scan.FilterList[0] = unresolved
	require.True(t, potential(proc.Ctx), "an unproved parameter must still prevent reuse of a base choice")
	between := DeepCopyExpr(predicate)
	between.GetF().Func.Obj = function.EncodeOverloadID(function.BETWEEN, 0)
	between.GetF().Args = append(between.GetF().Args, DeepCopyExpr(unresolved.GetF().Args[1]))
	scan.FilterList[0] = between
	require.True(t, potential(proc.Ctx), "a potential range must be detected before range lowering")
	in := DeepCopyExpr(predicate)
	in.GetF().Func.Obj = function.EncodeOverloadID(function.IN, 0)
	in.GetF().Args[1] = &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{DeepCopyExpr(unresolved.GetF().Args[1])}}}}
	scan.FilterList[0] = in
	require.True(t, potential(proc.Ctx), "a potential IN arm must be detected before lowering")
	scan.TableDef.Indexes = nil
	require.False(t, potential(proc.Ctx), "unindexed queries do not need temporal replanning")
	scan.TableDef.Indexes = []*plan.IndexDef{index}
	scan.FilterList[0] = predicate
	literal := &plan.Expr{Typ: Type{Id: int32(types.T_timestamp), Scale: 6}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 123456}}}}
	literalPredicate := DeepCopyExpr(predicate)
	literalPredicate.GetF().Args[1] = literal
	scan.FilterList[0] = literalPredicate
	proof := proofFor(proc.Ctx)
	require.True(t, proof.fits && proof.single, "the typed literal must pass the original-owner proof")
	require.True(t, potential(proc.Ctx), "a leading key with an eligible typed literal must be replanned")
	typedNull := DeepCopyExpr(literalPredicate)
	typedNull.GetF().Args[1].Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
	scan.FilterList[0] = typedNull
	proof = proofFor(proc.Ctx)
	require.True(t, proof.fits && proof.single)
	require.Equal(t, indexComparisonRaw, proof.atom.domain, "NULL uses the exact comparison domain")
	require.True(t, potential(proc.Ctx), "a typed NULL must not erase the conservative lifetime proof")
	typedNull.GetF().Args[1].Typ.Id = int32(types.T_any)
	proof = proofFor(proc.Ctx)
	require.False(t, proof.single, "an untyped NULL cannot prove an executable comparison")
	require.False(t, potential(proc.Ctx), "a proved untyped NULL cannot become an inverse candidate")
	scan.FilterList[0] = literalPredicate
	builder.compCtx.SetContext(proc.Ctx)
	query.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(1)
	require.True(t, query.GetExecutionDependentAccess())
	builder.compCtx.SetContext(bindingCtx)
	scan.FilterList[0] = predicate
	index.Parts = []string{"id", "d"}
	require.False(t, potential(proc.Ctx), "a suffix-only temporal column cannot select this access")
	require.False(t, potential(bindingCtx), "bound prepared plans retain their existing reuse owner")
	query.ExecutionDependentAccess = false
	preparedBindingState(bindingCtx).valueDependent = false
	builder.collectOriginalIndexOwner(1)
	require.False(t, query.GetExecutionDependentAccess())
	require.False(t, preparedBindingState(bindingCtx).valueDependent)
	scan.TableDef.Indexes = []*plan.IndexDef{{IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true}, index}
	require.True(t, potential(proc.Ctx), "incomplete index metadata cannot prove absence")
	cancelled, cancel := context.WithCancel(proc.Ctx)
	cancel()
	require.True(t, potential(cancelled), "cancellation keeps the conservative lifetime")
	scan.TableDef.Indexes = make([]*plan.IndexDef, engine.MaxIndexPlanningDefinitions+1)
	for i := range scan.TableDef.Indexes {
		scan.TableDef.Indexes[i] = index
	}
	require.True(t, potential(proc.Ctx), "an index count over the proof budget remains conservative")
	longName := strings.Repeat("x", engine.MaxIndexPlanningNameBytes+1)
	scan.TableDef.Indexes = []*plan.IndexDef{{IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{longName, "d"}}}
	require.True(t, potential(proc.Ctx), "an oversized key name remains conservative")
	scan.FilterList = []*plan.Expr{predicate, DeepCopyExpr(predicate)}
	require.False(t, potential(proc.Ctx), "two original filters cannot produce this candidate")
	proof = proofFor(proc.Ctx)
	require.True(t, proof.fits)
	require.Greater(t, proof.remaining, 4096-64, "the proof performs only its initial budget cancellation check")
	cancelBase, cancelAfterCheck := context.WithCancel(proc.Ctx)
	defer cancelAfterCheck()
	lateCancel := &cancelAfterFirstErrCheck{Context: cancelBase, cancel: cancelAfterCheck}
	require.True(t, potential(lateCancel), "cancellation during the proof must retain a potential dependency")
	scan.FilterList = []*plan.Expr{predicate}
	index.Parts = []string{"d", "id"}
	scan.TableDef.Indexes = []*plan.IndexDef{index}
	scan.FilterList = nil
	project.FilterList = []*plan.Expr{predicate}
	require.True(t, potential(proc.Ctx), "detection must precede filter pushdown")
	project.FilterList = nil
	scan.FilterList = []*plan.Expr{predicate}
	indexTable := &plan.TableDef{Cols: []*plan.ColDef{{Name: "key", Typ: Type{Id: int32(types.T_varchar)}}}}
	makeHint := func() (*plan.Expr, bool, error) {
		atom, ok, err := indexCandidateOperands(proc.Ctx, query, 0, index, 0)
		if err != nil || !ok {
			return nil, false, err
		}
		return makeNecessaryIndexHint(proc.Ctx, atom, index, indexTable, 2)
	}
	hint, ok, err := makeHint()
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, hint.CandidateFilter)
	require.False(t, predicate.CandidateFilter)
	index.Unique = true
	_, ok, err = makeHint()
	require.NoError(t, err)
	require.False(t, ok, "nullable suffix prevents proving unique-index completeness")
	index.Unique = false
	suffix := scan.TableDef.Cols[1]
	scan.TableDef.Cols[1] = nil
	_, ok, err = makeHint()
	require.NoError(t, err)
	require.False(t, ok, "incomplete index metadata must decline the hint")
	scan.TableDef.Cols[1] = suffix
	project.NodeType = plan.Node_JOIN
	_, ok, err = makeHint()
	require.NoError(t, err)
	require.False(t, ok)
}

// Cancel just after the budget's first context check, before the proof returns.
type cancelAfterFirstErrCheck struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelAfterFirstErrCheck) Value(any) any { return nil }

func (c *cancelAfterFirstErrCheck) Err() error {
	err := c.Context.Err()
	c.checks++
	if c.checks == 1 {
		c.cancel()
	}
	return err
}
