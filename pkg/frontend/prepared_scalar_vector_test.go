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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// 只有 catalog/engine 元数据是 stub；SQL planner、PREPARE admission 和
// EXECUTE 的缓存取用均走生产入口，不注入假的 Compile 或强改 execType。
type preparedScalarVectorContext struct {
	*plan2.MockCompilerContext
	tables map[string]*plan.TableDef
}

func (c *preparedScalarVectorContext) Resolve(db, name string, _ *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	table := c.tables[name]
	return &plan.ObjectRef{SchemaName: db, ObjName: name, Obj: int64(table.TblId)}, plan2.DeepCopyTableDef(table, true), nil
}
func (c *preparedScalarVectorContext) ResolveIndexTableByRef(ref *plan.ObjectRef, name string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return c.Resolve(ref.SchemaName, name, snapshot)
}
func (c *preparedScalarVectorContext) ResolveVariable(name string, system, global bool) (interface{}, error) {
	switch name {
	case "hnsw_threads_search":
		return int64(1), nil
	case "enable_vector_auto_mode_by_default", "enable_vector_prefilter_by_default":
		return int8(0), nil
	}
	return c.MockCompilerContext.ResolveVariable(name, system, global)
}

func TestPreparedScalarVectorCompileCache(t *testing.T) {
	ses, old, cw, execCtx := newPreparedExecuteEnv(t, 118)
	name := old.Name
	t.Cleanup(func() { ses.RemovePrepareStmt(name) })
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(rel, nil).AnyTimes()
	ses.txnHandler = InitTxnHandler(ses.service, eng, execCtx.reqCtx, nil)
	t.Cleanup(func() { ses.txnHandler.Close() })
	ctx := &preparedScalarVectorContext{MockCompilerContext: plan2.NewMockCompilerContext(false), tables: make(map[string]*plan.TableDef)}
	ctx.SetContext(execCtx.reqCtx)
	ctx.GetProcessFunc = func() *process.Process { return ses.proc }
	items := &plan.TableDef{Name: "items", TblId: 901, Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		{Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 3, NotNullable: true}},
	}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}, Name2ColIndex: map[string]int32{"id": 0, "v": 1}}
	provider := plan2.DeepCopyTableDef(items, true)
	provider.Name, provider.TblId, provider.Cols[1].Typ.NotNullable = "provider", 902, false
	ctx.tables[items.Name], ctx.tables[provider.Name] = items, provider
	for i, kind := range []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage} {
		items.Indexes = append(items.Indexes, &plan.IndexDef{IndexName: "idx", IndexAlgo: "hnsw", IndexAlgoTableType: kind, IndexTableName: kind, Parts: []string{"v"}, TableExist: true, IndexAlgoParams: `{"op_type":"vector_l2_ops"}`})
		ctx.tables[kind] = &plan.TableDef{Name: kind, TblId: uint64(910 + i)}
	}
	sql := "select id from items order by l2_distance(v,(select v from provider where id=1)) limit 2"
	stmts, err := mysql.Parse(execCtx.reqCtx, sql, 1)
	require.NoError(t, err)
	// createPrepareStmt 成功后由 PrepareStmt.Close 释放 AST；失败时本层释放。
	transferred := false
	t.Cleanup(func() {
		if !transferred {
			stmts[0].Free()
		}
	})
	originalBuild := buildPlanWithAuthorization
	t.Cleanup(func() { buildPlanWithAuthorization = originalBuild })
	buildPlanWithAuthorization = func(_ context.Context, _ FeSession, _ plan2.CompilerContext, stmt tree.Statement) (*plan.Plan, error) {
		return plan2.BuildPlan(ctx, stmt, false)
	}
	prepare := tree.NewPrepareString(tree.Identifier(name), sql)
	defer prepare.Free()
	prepared, err := createPrepareStmt(execCtx, ses, sql, prepare, stmts[0])
	require.NoError(t, err)
	require.NotNil(t, prepared)
	transferred = true
	registered := false
	t.Cleanup(func() {
		if !registered {
			prepared.Close()
		}
	})
	query := prepared.PreparePlan.GetDcl().GetPrepare().Plan.GetQuery()
	require.Equal(t, plan2.ExecTypeTP, plan2.GetExecType(query, false, true))
	selectors := 0
	for _, n := range query.Nodes {
		if n.NodeType == plan.Node_VECTOR_QUERY_TOP {
			selectors++
		}
	}
	require.Equal(t, 1, selectors)
	require.NotNil(t, prepared.compile, "real PREPARE must retain a compiled scalar selector")
	require.True(t, prepared.compile.IsTpQuery())
	require.NoError(t, ses.SetPrepareStmt(execCtx.reqCtx, name, prepared))
	registered = true
	cached := prepared.compile
	for generation := 0; generation < 3; generation++ {
		got, p, _, _, owned, err := initExecuteStmtParam(execCtx, ses, cw, nil, prepared.Name)
		require.NoError(t, err)
		require.False(t, owned)
		require.Same(t, cached, got)
		require.Same(t, prepared.PreparePlan.GetDcl().GetPrepare().Plan, p)
		got.Reset(cw.proc, time.Now(), nil, sql)
	}
}
