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
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestFunctionalJSONTypedKeyDDLAndMetadata(t *testing.T) {
	proc := newPlanTestProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
		}
	})
	ctx := NewMockCompilerContext(false, proc)
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false)
	}
	p, err := build("create table docs(id int primary key,doc json,index idx_kind((cast(doc ->> '$.kind' as char(20)))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	require.Len(t, table.Indexes, 1)
	required, err := pb.JSONFunctionalIndexSemantics(table.Indexes[0])
	require.NoError(t, err)
	require.True(t, required)
	col := functionalIndexPartColumn(table, table.Indexes[0], 0)
	require.NotNil(t, col)
	require.Equal(t, int32(20), col.Typ.Width)
	require.NoError(t, validateFunctionalTable(t.Context(), table))
	floor, err := RequiredPersistedExpressionProtocolVersion(table)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion115, floor)
	bad := DeepCopyTableDef(table, true)
	bad.Indexes[0].IndexAlgoParams = ""
	require.Error(t, validateFunctionalTable(t.Context(), bad))
	bad = DeepCopyTableDef(table, true)
	bad.Indexes[0].IndexAlgoParams = `{"functional_json_semantics":"2"}`
	require.Error(t, validateFunctionalTable(t.Context(), bad))
	for _, sql := range []string{
		"create table bad(id int primary key,doc json,index j((doc ->> '$.kind')))",
		"create table bad(id int primary key,doc json,index j((cast(doc ->> '$.*' as char(20)))))",
		"create table bad(id int primary key,ts timestamp,index j((cast(ts as char(20)))))",
	} {
		_, err = build(sql)
		require.Error(t, err, sql)
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion114)
	_, err = build("create table old_json(id int primary key,doc json,index j((cast(doc ->> '$.kind' as char(20)))))")
	require.Error(t, err)
	_, err = build("create table old_ordinary(id int primary key,name varchar(20),index j((lower(name))))")
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	_, err = build("create table child(id int primary key,pid int,index j((cast(json_unquote(json_extract(cast(pid as json),'$')) as char(20)))))")
	require.NoError(t, err)
}

// Exercise the existing SQL replay and physical equality matcher with a JSON
// key. This complements the metadata-only protocol tests; it is not runtime
// index/scan or checkpoint/restart evidence.
func TestFunctionalJSONReplayAndEqualityPlan(t *testing.T) {
	proc := newPlanTestProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
		}
	})
	ctx := NewMockCompilerContext(false, proc)
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(ctx, stmt, false)
	}
	p, err := build("create table docs(id int primary key,doc json,index idx_kind((cast(doc ->> '$.kind' as char(20)))))")
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	key := functionalIndexColumn(table, table.Indexes[0]).GeneratedCol.Expr
	target := key.Typ
	target.Id = int32(types.T_varchar)
	target.Width = 65535
	wrap := &Expr{Typ: target, Expr: &pb.Expr_F{F: &pb.Function{Func: &ObjectRef{Obj: int64(function.CAST)<<32 | 2, ObjName: "cast"}, Args: []*Expr{key, {Typ: target, Expr: &pb.Expr_T{T: &pb.TargetType{}}}}}}}
	require.Same(t, key, functionalJSONComparisonOperand(wrap, table.Cols, 0))
	for _, change := range []func(*Expr){
		func(e *Expr) { e.Typ.Width = 19; e.GetF().Args[1].Typ = e.Typ },
		func(e *Expr) { e.Typ.Charset = uint32(types.CharsetBinary); e.GetF().Args[1].Typ = e.Typ },
		func(e *Expr) { e.Typ.CollationVersion = 1; e.GetF().Args[1].Typ = e.Typ },
		func(e *Expr) { e.Typ.PadSpace = true; e.GetF().Args[1].Typ = e.Typ },
		func(e *Expr) { e.GetF().Func.Obj = int64(function.CAST) << 32 },
	} {
		bad := DeepCopyExpr(wrap)
		change(bad)
		require.Same(t, bad, functionalJSONComparisonOperand(bad, table.Cols, 0))
	}
	ctx.tables["docs"] = table
	ctx.objects["docs"] = &ObjectRef{ObjName: "docs", SchemaName: "tpch"}
	for _, it := range p.GetDdl().GetCreateTable().IndexTables {
		ctx.tables[it.Name] = it
		ctx.objects[it.Name] = &ObjectRef{ObjName: it.Name, SchemaName: "tpch"}
	}
	qp, err := build("select id from docs where cast(doc ->> '$.kind' as char(20))='alpha'")
	require.NoError(t, err)
	physical, residual := false, false
	for _, n := range qp.GetQuery().Nodes {
		if n.NodeType == pb.Node_TABLE_SCAN && n.TableDef != nil {
			if n.TableDef.Name == table.Indexes[0].IndexTableName {
				physical = true
			}
			if n.TableDef.Name == "docs" && len(n.FilterList) > 0 {
				residual = true
			}
		}
	}
	if !physical {
		t.Log("persisted JSON key", functionalIndexColumn(table, table.Indexes[0]).GeneratedCol.Expr.String())
		t.Log("actual query plan", qp.String())
	}
	require.True(t, physical, "identical typed JSON predicate must select its physical index")
	require.True(t, residual, "the base-row predicate must remain as a residual")
	ddl, _, err := constructCreateTableSQL(ctx, table, nil, false, nil, false, nil)
	require.NoError(t, err)
	require.NotContains(t, ddl, functionalColumnPrefix)
	replay, err := build(ddl)
	require.NoError(t, err)
	replayTable := replay.GetDdl().GetCreateTable().TableDef
	require.NoError(t, validateFunctionalTable(t.Context(), replayTable))
	marked, err := pb.JSONFunctionalIndexSemantics(replayTable.Indexes[0])
	require.NoError(t, err)
	require.True(t, marked)
	require.True(t, sameFunctionalValueType(functionalIndexColumn(table, table.Indexes[0]).Typ, functionalIndexColumn(replayTable, replayTable.Indexes[0]).Typ))
	// A dynamically created mock table has no persisted mo_indexes row. Supply
	// its real visibility lookup instead of bypassing the canonical COPY guard.
	previousExecutor := ctx.internalSQLExecutor
	ctx.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.Contains(sql, "SELECT name, is_visible FROM mo_catalog.mo_indexes WHERE table_id = 0") {
			result := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, proc.Mp())
			result.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(result, 0, []string{"idx_kind"}))
			require.NoError(t, executor.AppendFixedRows(result, 1, []int8{1}))
			return result.GetResult(), nil
		}
		return previousExecutor.Exec(t.Context(), sql, executor.Options{})
	})
	for _, sql := range []string{"create index idx_more on docs ((cast(doc ->> '$.other' as char(20))))", "alter table docs add index idx_more ((cast(doc ->> '$.other' as char(20)))), algorithm=copy"} {
		ap, err := build(sql)
		require.NoError(t, err, sql)
		require.Equal(t, pb.AlterTable_COPY, ap.GetDdl().GetAlterTable().AlgorithmType)
		require.NotContains(t, ap.GetDdl().GetAlterTable().CreateTmpTableSql, functionalColumnPrefix)
	}
}

func TestFunctionalJSONForeignKeyFinalRowImage(t *testing.T) {
	proc := newPlanTestProcess(t)
	ctx := NewMockCompilerContext(false, proc)
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
		}
	})
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "create table child(id int primary key,pid int,doc json,index dep((cast(json_unquote(json_extract(cast(pid as json),'$')) as char(20)))),index doc_key((cast(doc ->> '$.kind' as char(20)))))", 1)
	require.NoError(t, err)
	ddl, err := BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.NoError(t, err)
	for _, conditional := range []bool{false, true} {
		table := DeepCopyTableDef(ddl.GetDdl().GetCreateTable().TableDef, true)
		ensureName2ColIndexForReplace(table)
		builder := NewQueryBuilder(pb.Query_UPDATE, ctx, false, false)
		bindCtx := NewBindContext(builder, nil)
		projection := make([]*Expr, 0, len(table.Cols)+1)
		insertPos := make([]int, 0)
		for i, col := range table.Cols {
			projection = append(projection, &Expr{Typ: col.Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: int32(i)}}})
			if !col.Hidden || isFunctionalColumn(col) {
				insertPos = append(insertPos, i)
			}
		}
		pid := table.Name2ColIndex["pid"]
		target := &Expr{Typ: table.Cols[pid].Typ, Expr: &pb.Expr_Lit{Lit: &Const{Isnull: true}}}
		if conditional {
			target, err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "if", []*Expr{makePlan2BoolConstExprWithType(true), target, DeepCopyExpr(projection[pid])})
			require.NoError(t, err)
		}
		replacementPos := len(projection)
		projection = append(projection, target)
		source := builder.appendNode(&Node{NodeType: pb.Node_PROJECT, ProjectList: projection}, bindCtx)
		action := &dmlPlanCtx{tableDef: table, isFkRecursionCall: true, updateColLength: 1, updateColPosMap: map[string]int{"pid": replacementPos}, insertColPos: insertPos}
		last, err := appendForeignKeyFunctionalRowImage(builder, bindCtx, action, source)
		require.NoError(t, err)
		require.NotEqual(t, source, last)
		dependent := functionalIndexColumn(table, table.Indexes[0])
		unrelated := functionalIndexColumn(table, table.Indexes[1])
		pos, changed := action.updateColPosMap[dependent.Name]
		require.True(t, changed)
		require.Equal(t, len(projection), pos)
		require.Equal(t, 2, action.updateColLength)
		_, changed = action.updateColPosMap[unrelated.Name]
		require.False(t, changed)
		for i, e := range getProjectionByLastNode(builder, source) {
			require.True(t, e.Equal(builder.qry.Nodes[last].ProjectList[i]), "old-row/delete prefix preserved")
		}
		refs := collectRefColPos(builder.qry.Nodes[last].ProjectList[pos])
		require.Contains(t, refs, int32(replacementPos))
		require.NotContains(t, refs, pid, "functional image uses final ordinary replacement")
		colMap := make(map[string]*ColDef)
		positions := make(map[string]int)
		for i, col := range table.Cols {
			colMap[col.Name] = col
			positions[col.Name] = i
		}
		rewrite, err := indexNeedsRewriteForUpdate(table, table.Indexes[0], action.updateColPosMap, positions, colMap)
		require.NoError(t, err)
		require.True(t, rewrite)
		rewrite, err = indexNeedsRewriteForUpdate(table, table.Indexes[1], action.updateColPosMap, positions, colMap)
		require.NoError(t, err)
		require.False(t, rewrite)
		visible := 0
		for _, col := range table.Cols {
			if col.Hidden && !isFunctionalColumn(col) {
				continue
			}
			if col.Name == dependent.Name {
				require.Equal(t, pos, action.insertColPos[visible])
			}
			visible++
		}
	}
}
