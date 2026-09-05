// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	offsetop "github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	limitop "github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	orderop "github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	partitionop "github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	windowop "github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestHasOrderedGroupConcat(t *testing.T) {
	ordered := &plan.Node{
		AggList: []*plan.Expr{{
			Expr: &plan.Expr_F{F: &plan.Function{
				Func:          &plan.ObjectRef{ObjName: "group_concat"},
				AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
			}},
		}},
	}
	require.True(t, hasOrderedGroupConcat(ordered))

	ordered.GroupBy = []*plan.Expr{{}}
	require.True(t, hasOrderedGroupConcat(ordered))
	ordered.GroupBy = nil
	ordered.AggList[0].GetF().AggConfigType = plan.AggregateConfigType_AGG_CONFIG_NONE
	require.False(t, hasOrderedGroupConcat(ordered))
}

func TestCompileMongoDBQueryDiagnosticsAreRedacted(t *testing.T) {
	for _, sql := range []string{
		`select * from mongo_events where __mo_query = '{"filter":{"password":"super-secret-value"}}'`,
		`select * from mongo_events where __MO_QUERY = '{"pipeline":[{"$match":{"api_key":"super-secret-value"}}]}'`,
	} {
		t.Run(sql[:20], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctrl := gomock.NewController(t)
			_, txnOp := newTestTxnClientAndOp(ctrl)
			proc.Base.TxnOperator = txnOp
			compile := NewCompile("test", "test", sql, "", "", nil, proc, nil, false, nil, time.Now())
			t.Cleanup(compile.Release)

			compile.SetOriginSQL(sql)
			for _, diagnostic := range []string{compile.sql, compile.originSQL} {
				require.Equal(t, "<redacted MongoDB __mo_query statement>", diagnostic)
				require.NotContains(t, diagnostic, "password")
				require.NotContains(t, diagnostic, "api_key")
				require.NotContains(t, diagnostic, "super-secret-value")
			}

			info, err := proc.BuildProcessInfo(compile.sql)
			require.NoError(t, err)
			diagnostic := info.String()
			require.Contains(t, diagnostic, "redacted MongoDB")
			require.NotContains(t, diagnostic, "password")
			require.NotContains(t, diagnostic, "api_key")
			require.NotContains(t, diagnostic, "super-secret-value")

			require.NoError(t, compile.Reset(proc, time.Now(), nil, sql))
			require.Equal(t, "<redacted MongoDB __mo_query statement>", compile.sql)
		})
	}
}

func TestFilterScanStorageExprsExcludesVolatilePredicates(t *testing.T) {
	randFn, err := function.GetFunctionByName(context.Background(), "rand", nil)
	require.NoError(t, err)
	volatile := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{
		Obj: randFn.GetEncodedOverloadID(), ObjName: "rand",
	}}}}
	stable := plan2.MakePlan2Int64ConstExprWithType(1)

	require.Equal(t, []*plan.Expr{stable}, filterScanStorageExprs([]*plan.Expr{stable, volatile}))
}

func TestCompileRunPreservesBinaryPrepareParamAcrossRetries(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return ^uint64(0), nil
		}
		return "STRICT_TRANS_TABLES", nil
	})
	compilerCtx := plan2.NewEmptyCompilerContext()
	compilerCtx.SetContext(ctx)
	stmts, err := mysql.Parse(ctx, "select ?", 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], true)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}, IsPrepare: true}
	_, _, err = plan2.ResetPreparePlan(compilerCtx, pn)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	txnCli, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	want := []byte{'A', 'B', 0, 0}
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, want, false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithIsBin(params, []bool{true})

	evaluations := 0
	fill := func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		if bat == nil {
			return nil
		}
		require.Len(t, bat.Vecs, 1)
		require.True(t, bat.Vecs[0].GetIsBin(), "binary semantics were lost on evaluation %d", evaluations+1)
		require.Equal(t, want, bat.Vecs[0].GetBytesAt(0))
		evaluations++
		if evaluations <= 2 {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		return nil
	}

	c := NewCompile("test", "test", "select ?", "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
	require.NoError(t, c.Compile(ctx, pn, fill))
	_, err = c.Run(0)
	require.NoError(t, err)
	require.Equal(t, 3, evaluations)
	require.Equal(t, 2, c.retryTimes)
	require.Zero(t, params.Length())
	require.Nil(t, params.GetData())
	require.Nil(t, params.GetArea())
	c.Release()
	proc.Free()
	proc.GetSessionInfo().Buf.Free()
}

func TestSQLSelectLimitIsResolvedForEachExecution(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.GetSessionInfo().Buf = buffer.New()
	t.Cleanup(func() {
		proc.Free()
		proc.GetSessionInfo().Buf.Free()
	})
	limitValue := uint64(1)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return limitValue, nil
		}
		return "STRICT_TRANS_TABLES", nil
	})

	compilerCtx := plan2.NewEmptyCompilerContext()
	compilerCtx.SetContext(ctx)
	const sql = "select 1 union all select 2"
	stmts, err := mysql.Parse(ctx, sql, 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}

	ctrl := gomock.NewController(t)
	txnCli, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	rows := 0
	fill := func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		if bat != nil {
			rows += bat.RowCount()
		}
		return nil
	}
	c := NewCompile("test", "test", sql, "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
	t.Cleanup(c.Release)
	c.SetIsPrepare(true)
	require.NoError(t, c.Compile(ctx, pn, fill))
	_, err = c.Run(0)
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	limitValue = 0
	rows = 0
	require.NoError(t, c.Reset(proc, time.Now(), fill, sql))
	_, err = c.Run(0)
	require.NoError(t, err)
	require.Zero(t, rows)
}

func TestSQLSelectLimitOperatorSelection(t *testing.T) {
	resolverErr := errors.New("sql_select_limit resolver failed")
	tests := []struct {
		name       string
		limit      uint64
		resolveErr error
		isPrepare  bool
		wantLimit  bool
	}{
		{name: "ordinary default is a no-op", limit: ^uint64(0), wantLimit: false},
		{name: "ordinary finite value is enforced", limit: 1, wantLimit: true},
		{name: "prepared default remains dynamic", limit: ^uint64(0), isPrepare: true, wantLimit: true},
		{name: "resolver error fails compilation", resolveErr: resolverErr},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.ApplySQLSelectLimit = true
			proc.GetSessionInfo().Buf = buffer.New()
			t.Cleanup(func() {
				proc.Free()
				proc.GetSessionInfo().Buf.Free()
			})
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == plan2.SQLSelectLimitVariable {
					return tc.limit, tc.resolveErr
				}
				return "STRICT_TRANS_TABLES", nil
			})

			compilerCtx := plan2.NewEmptyCompilerContext()
			compilerCtx.SetContext(ctx)
			const sql = "select 1 union all select 2"
			stmts, err := mysql.Parse(ctx, sql, 1)
			require.NoError(t, err)
			query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
			require.NoError(t, err)
			pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}

			ctrl := gomock.NewController(t)
			txnCli, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)

			c := NewCompile("test", "test", sql, "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
			t.Cleanup(c.Release)
			c.SetIsPrepare(tc.isPrepare)
			err = c.Compile(ctx, pn, func(*batch.Batch, *perfcounter.CounterSet) error {
				return nil
			})
			if tc.resolveErr != nil {
				require.ErrorIs(t, err, tc.resolveErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantLimit, compiledScopesContainOperator(c.scopes, vm.Limit))
		})
	}
}

func TestSQLSelectLimitIsResolvedForEachCachedPlanReuse(t *testing.T) {
	tests := []struct {
		name   string
		limits []uint64
	}{
		{name: "finite to finite", limits: []uint64{2, 4}},
		{name: "unlimited to finite", limits: []uint64{^uint64(0), 3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.ApplySQLSelectLimit = true
			limitIndex := 0
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == plan2.SQLSelectLimitVariable {
					return tc.limits[limitIndex], nil
				}
				return "STRICT_TRANS_TABLES", nil
			})
			t.Cleanup(proc.Free)

			query := &plan.Query{
				StmtType:            plan.Query_SELECT,
				ApplySqlSelectLimit: true,
				Steps:               []int32{0},
				Nodes:               []*plan.Node{{NodeId: 0, NodeType: plan.Node_PROJECT}},
			}
			queryPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
			compiler := &Compile{proc: proc}

			for i, want := range tc.limits {
				limitIndex = i
				materialization, err := compiler.materializeSQLSelectLimit(queryPlan)
				require.NoError(t, err)
				require.False(t, query.ApplySqlSelectLimit)
				if want == ^uint64(0) {
					require.Nil(t, query.Nodes[0].Limit)
				} else {
					require.Equal(t, want, query.Nodes[0].Limit.GetLit().GetU64Val())
				}
				materialization.restore()
				require.True(t, query.ApplySqlSelectLimit)
				require.Nil(t, query.Nodes[0].Limit)
			}
		})
	}
}

type sqlSelectLimitReleaseOperator struct {
	*colexec.MockOperator
	released bool
}

func (op *sqlSelectLimitReleaseOperator) Release() {
	op.released = true
}

func TestSQLSelectLimitResolverFailureReleasesCompileStepsTree(t *testing.T) {
	resolverErr := errors.New("sql_select_limit resolver failed")
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return nil, resolverErr
		}
		return "STRICT_TRANS_TABLES", nil
	})
	t.Cleanup(proc.Free)

	c := &Compile{
		proc: proc, anal: &AnalyzeModule{}, ncpu: 1,
		execType: plan2.ExecTypeAP_ONECN,
	}
	owners := []*sqlSelectLimitReleaseOperator{
		{MockOperator: colexec.NewMockOperator()},
		{MockOperator: colexec.NewMockOperator()},
	}
	scopes := make([]*Scope, len(owners))
	for i, owner := range owners {
		scope := newScope(Normal)
		scope.NodeInfo.Mcpu = 1
		scope.Proc = proc.NewNoContextChildProc(0)
		scope.RootOp = owner
		scopes[i] = scope
	}

	qry := &plan.Query{
		StmtType:            plan.Query_SELECT,
		ApplySqlSelectLimit: true,
		Steps:               []int32{0},
		Nodes:               []*plan.Node{{NodeId: 0, NodeType: plan.Node_PROJECT}},
	}
	compiled, err := c.compileSteps(qry, scopes, 0)
	require.ErrorIs(t, err, resolverErr)
	require.Nil(t, compiled)
	for _, owner := range owners {
		require.True(t, owner.released)
	}
}

func TestCompileStepsKeepsOutputOnCurrentCNForSingleRemoteScope(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local-cn:6001"
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{}

	remote := newScope(Remote)
	remote.NodeInfo = engine.Node{Addr: "remote-cn:6001", Mcpu: 1}
	remote.Proc = c.proc.NewNoContextChildProc(0)
	remote.setRootOperator(projection.NewArgument())

	qry := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   0,
			NodeType: plan.Node_PROJECT,
		}},
	}
	compiled, err := c.compileSteps(qry, []*Scope{remote}, 0)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	t.Cleanup(func() { ReleaseScopes(compiled) })

	resultScope := compiled[0]
	require.Equal(t, Merge, resultScope.Magic)
	require.Equal(t, c.addr, resultScope.NodeInfo.Addr)
	require.Equal(t, vm.Output, resultScope.RootOp.OpType())
	require.Equal(t, vm.Merge, resultScope.RootOp.GetOperatorBase().GetChildren(0).OpType())
	require.Len(t, resultScope.PreScopes, 1)

	remote = resultScope.PreScopes[0]
	require.Equal(t, Remote, remote.Magic)
	require.Equal(t, vm.Connector, remote.RootOp.OpType())
	require.Equal(t, vm.Projection, remote.RootOp.GetOperatorBase().GetChildren(0).OpType())

	encodedScope, withoutOutput := getScopeForRemoteRunEncoding(remote)
	require.False(t, withoutOutput)
	require.Equal(t, vm.Projection, encodedScope.RootOp.OpType())
	_, err = encodeScope(encodedScope)
	require.NoError(t, err)
}

func TestCompileStepsReusesSingleScopeExecutingOnCurrentCNForOutput(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local-cn:6001"
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{}

	local := newScope(Remote)
	local.NodeInfo = engine.Node{Addr: c.addr, Mcpu: 1}
	local.Proc = c.proc.NewNoContextChildProc(0)
	local.setRootOperator(projection.NewArgument())

	qry := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*plan.Node{{
			NodeId:   0,
			NodeType: plan.Node_PROJECT,
		}},
	}
	compiled, err := c.compileSteps(qry, []*Scope{local}, 0)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	t.Cleanup(func() { ReleaseScopes(compiled) })

	require.Same(t, local, compiled[0])
	require.Empty(t, compiled[0].PreScopes)
	require.Equal(t, vm.Output, compiled[0].RootOp.OpType())
	require.Equal(t, vm.Projection, compiled[0].RootOp.GetOperatorBase().GetChildren(0).OpType())
}

func TestSQLCalcFoundRowsOwnsPreparedSQLSelectLimit(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	proc := c.proc
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return uint64(1), nil
		}
		return nil, nil
	})
	c.stmt = sqlCalcFoundRowsTestStatement()
	c.isPrepare = true
	input := newLazyUnionAllLeaf(c, nil)
	nestedLimit := &plan.Node{
		NodeId:   1,
		NodeType: plan.Node_PROJECT,
		Limit:    plan2.MakePlan2Uint64ConstExprWithType(5),
	}
	query := &plan.Query{
		StmtType:            plan.Query_SELECT,
		ApplySqlSelectLimit: true,
		Steps:               []int32{0},
		Nodes: []*plan.Node{
			{NodeId: 0, NodeType: plan.Node_PROJECT, Children: []int32{1}},
			nestedLimit,
		},
	}
	// The nested LIMIT must not block the dynamic top-level sql_select_limit
	// added below for each prepared execution.
	c.foundRowsOwnerNode = c.selectFoundRowsOwnerNode(query)
	require.Nil(t, c.foundRowsOwnerNode)

	result, err := c.compileSteps(query, []*Scope{input}, 0)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.NotNil(t, c.foundRowsOwnerNode)

	foundOwner := false
	require.NoError(t, vm.HandleAllOp(result[0].RootOp, func(_ vm.Operator, op vm.Operator) error {
		if limitArg, ok := op.(*limitop.Limit); ok && limitArg.IsFoundRowsOwner() {
			foundOwner = true
		}
		return nil
	}))
	require.True(t, foundOwner)
	require.NotSame(t, nestedLimit, c.foundRowsOwnerNode)
	freeLazyUnionAllTestScope(c, result[0])
}

func TestPreparedSQLSelectLimitDrainsAboveFoundRowsOffsetOwner(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	proc := c.proc
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return uint64(1), nil
		}
		return nil, nil
	})

	stmts, err := mysql.Parse(proc.Ctx,
		"select sql_calc_found_rows id from t order by id offset 2", 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	c.stmt = stmts[0]
	c.isPrepare = true

	offsetNode := &plan.Node{
		NodeId:   0,
		NodeType: plan.Node_SORT,
		Offset:   plan2.MakePlan2Uint64ConstExprWithType(2),
	}
	query := &plan.Query{
		StmtType:            plan.Query_SELECT,
		ApplySqlSelectLimit: true,
		Steps:               []int32{0},
		Nodes:               []*plan.Node{offsetNode},
	}
	c.foundRowsOwnerNode = c.selectFoundRowsOwnerNode(query)
	require.Same(t, offsetNode, c.foundRowsOwnerNode)

	input := newLazyUnionAllLeaf(c, nil)
	withOffset := c.compileOffset(offsetNode, []*Scope{input})
	result, err := c.compileSteps(query, withOffset, 0)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Same(t, offsetNode, c.foundRowsOwnerNode)

	var dynamicLimit *limitop.Limit
	var countingOffset *offsetop.Offset
	var inspectScopes func([]*Scope)
	inspectScopes = func(scopes []*Scope) {
		for _, scope := range scopes {
			_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
				switch arg := op.(type) {
				case *limitop.Limit:
					dynamicLimit = arg
				case *offsetop.Offset:
					countingOffset = arg
				}
				return nil
			})
			inspectScopes(scope.PreScopes)
		}
	}
	inspectScopes(result)

	require.NotNil(t, dynamicLimit)
	require.False(t, dynamicLimit.IsFoundRowsOwner())
	require.True(t, dynamicLimit.DrainsForFoundRows())
	require.NotNil(t, countingOffset)
	require.True(t, countingOffset.IsFoundRowsOwner())

	freeLazyUnionAllTestScope(c, result[0])
}

func compiledScopesContainOperator(scopes []*Scope, opType vm.OpType) bool {
	for _, scope := range scopes {
		found := false
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			found = found || op.OpType() == opType
			return nil
		})
		if found || compiledScopesContainOperator(scope.PreScopes, opType) {
			return true
		}
	}
	return false
}

type retryRecordingResultSink struct {
	events []string
	rows   map[uint64]int
}

type generationCheckingResultSink struct {
	activeGeneration uint64
	events           []string
}

func (s *generationCheckingResultSink) BeginAttempt(_ context.Context, generation uint64, _ *process.Process) error {
	s.activeGeneration = generation
	s.events = append(s.events, fmt.Sprintf("begin:%d", generation))
	return nil
}

func (s *generationCheckingResultSink) Write(generation uint64, bat *batch.Batch, _ *perfcounter.CounterSet) error {
	if bat == nil {
		return nil
	}
	s.events = append(s.events, fmt.Sprintf("write:%d", generation))
	if generation != s.activeGeneration {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("result sink generation mismatch: active=%d write=%d", s.activeGeneration, generation))
	}
	return nil
}

func (s *generationCheckingResultSink) SealAttempt(generation uint64) error {
	s.events = append(s.events, fmt.Sprintf("seal:%d", generation))
	return nil
}

func (s *generationCheckingResultSink) AbortAttempt(generation uint64, _ error) error {
	s.events = append(s.events, fmt.Sprintf("abort:%d", generation))
	return nil
}

func (s *retryRecordingResultSink) BeginAttempt(_ context.Context, generation uint64, _ *process.Process) error {
	s.events = append(s.events, fmt.Sprintf("begin:%d", generation))
	if s.rows == nil {
		s.rows = make(map[uint64]int)
	}
	return nil
}

func (s *retryRecordingResultSink) Write(generation uint64, bat *batch.Batch, _ *perfcounter.CounterSet) error {
	if bat == nil {
		return nil
	}
	s.events = append(s.events, fmt.Sprintf("write:%d", generation))
	s.rows[generation] += bat.RowCount()
	if generation < 2 {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	return nil
}

func (s *retryRecordingResultSink) SealAttempt(generation uint64) error {
	s.events = append(s.events, fmt.Sprintf("seal:%d", generation))
	return nil
}

func (s *retryRecordingResultSink) AbortAttempt(generation uint64, _ error) error {
	s.events = append(s.events, fmt.Sprintf("abort:%d", generation))
	delete(s.rows, generation)
	return nil
}

func TestResultWriterCapturesExecutionGeneration(t *testing.T) {
	sink := &retryRecordingResultSink{rows: make(map[uint64]int)}
	c := &Compile{resultSink: sink, executionGeneration: 3}
	writer := c.resultWriter()
	c.executionGeneration = 4
	require.NoError(t, writer(batch.EmptyBatch, nil))
	require.Equal(t, []string{"write:3"}, sink.events)
}

func TestCompileResultSinkDiscardsRetriedGenerations(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().Buf = buffer.New()
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return ^uint64(0), nil
		}
		return "STRICT_TRANS_TABLES", nil
	})
	compilerCtx := plan2.NewEmptyCompilerContext()
	compilerCtx.SetContext(ctx)
	stmts, err := mysql.Parse(ctx, "select 1", 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}

	ctrl := gomock.NewController(t)
	txnCli, txnOp := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	c := NewCompile("test", "test", "select 1", "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
	require.NoError(t, c.Compile(ctx, pn, func(*batch.Batch, *perfcounter.CounterSet) error {
		return errors.New("streaming callback must not be used when ResultSink is installed")
	}))
	sink := &retryRecordingResultSink{}
	c.SetResultSink(sink)
	_, err = c.Run(0)
	require.NoError(t, err)
	require.Equal(t, []string{
		"begin:0", "write:0", "abort:0",
		"begin:1", "write:1", "abort:1",
		"begin:2", "write:2", "seal:2",
	}, sink.events)
	require.Equal(t, map[uint64]int{2: 1}, sink.rows)
	require.Equal(t, uint64(2), c.executionGeneration)

	// Compile.Reset is the prepared-statement reuse boundary. The next execution
	// must rebuild its output callback for generation zero even when the previous
	// execution succeeded after retries on a later generation.
	nextSink := &generationCheckingResultSink{}
	c.SetResultSink(nextSink)
	require.NoError(t, c.Reset(proc, time.Now(), func(*batch.Batch, *perfcounter.CounterSet) error {
		return errors.New("streaming callback must not be used when ResultSink is installed")
	}, "select 1"))
	_, err = c.Run(0)
	require.NoError(t, err)
	require.Equal(t, []string{"begin:0", "write:0", "seal:0"}, nextSink.events)

	c.Release()
	proc.Free()
	proc.GetSessionInfo().Buf.Free()
}

func TestApplyExecutorLockWaitTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZero(),
		nil,
		txnOp,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)

	applyExecutorLockWaitTimeout(proc, executor.Options{}.WithLockWaitTimeout(1500*time.Millisecond))
	require.Equal(t, int64(2), proc.Base.SessionInfo.LockWaitTimeout)
	require.True(t, proc.Base.SessionInfo.LockWaitTimeoutSet)

	clearOpts := executor.Options{}.WithTxn(txnOp).WithLockWaitTimeout(0)
	require.True(t, clearOpts.HasExistsTxn())
	applyExecutorLockWaitTimeout(proc, clearOpts)
	require.Zero(t, proc.Base.SessionInfo.LockWaitTimeout)
	require.True(t, proc.Base.SessionInfo.LockWaitTimeoutSet,
		"an explicit zero must be distinguishable from an absent override")
}

type Ws struct {
	advanceSnapshot func(context.Context, timestamp.Timestamp) error
}

func (w *Ws) SetCloneTxn(snapshot int64) {}

func (w *Ws) SetCCPRTxn() {}

func (w *Ws) IsCCPRTxn() bool { return false }

func (w *Ws) SetCCPRTaskID(taskID string) {}

func (w *Ws) GetCCPRTaskID() string { return "" }

func (w *Ws) SetSyncProtectionJobID(jobID string) {}

func (w *Ws) GetSyncProtectionJobID() string { return "" }

func (w *Ws) Readonly() bool {
	return false
}

func (w *Ws) Snapshot() bool {
	return false
}

func (w *Ws) IncrStatementID(ctx context.Context, commit bool) error {
	return nil
}

func (w *Ws) AdvanceSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	if w.advanceSnapshot != nil {
		return w.advanceSnapshot(ctx, ts)
	}
	return nil
}

func (w *Ws) RollbackLastStatement(ctx context.Context) error {
	return nil
}

func (w *Ws) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	return nil, nil
}

func (w *Ws) FinalizeCommit(ctx context.Context) {
}

func (w *Ws) FinalizeCommitWithUnknownResult(ctx context.Context) {
}

func (w *Ws) Rollback(ctx context.Context) error {
	return nil
}

func (w *Ws) UpdateSnapshotWriteOffset() {
}

func (w *Ws) GetSnapshotWriteOffset() int {
	return 0
}

func (w *Ws) WriteOffset() uint64 {
	return 0
}

func (w *Ws) Adjust(_ uint64) error {
	return nil
}

func (w *Ws) StartStatement()     {}
func (w *Ws) EndStatement()       {}
func (w *Ws) IncrSQLCount()       {}
func (w *Ws) GetSQLCount() uint64 { return 0 }

func (w *Ws) CloneSnapshotWS() client.Workspace {
	return nil
}

func (w *Ws) BindTxnOp(op client.TxnOperator) {
}

func (w *Ws) SetHaveDDL(flag bool) {
}

func (w *Ws) GetHaveDDL() bool {
	return false
}

func (w *Ws) PPString() string {
	return ""
}

func NewMockCompile(t *testing.T) *Compile {
	return &Compile{
		proc: testutil.NewProcess(t),
		ncpu: system.GoMaxProcs(),
	}
}

func TestShouldPrePipelineLockTable(t *testing.T) {
	c := NewMockCompile(t)
	target := &plan.LockTarget{LockTable: true}

	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_INSERT},
		},
	}
	require.False(t, c.shouldPrePipelineLockTable(target))
	require.True(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: true}
	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_INSERT, LoadTag: true},
		},
	}
	require.True(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: true}
	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_UPDATE},
		},
	}
	require.True(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: true}
	c.pn = &plan.Plan{}
	require.True(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)

	target = &plan.LockTarget{LockTable: false, LockTableAtTheEnd: true}
	require.False(t, c.shouldPrePipelineLockTable(target))
	require.False(t, target.LockTableAtTheEnd)
}

func TestCompileLockCandidateLoadKeepsCanonicalTableTarget(t *testing.T) {
	c := NewMockCompile(t)
	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{StmtType: plan.Query_INSERT, LoadTag: true},
		},
	}
	c.lockTables = make(map[uint64]*plan.LockTarget)
	c.loadUniqueIndexPromotion = &loadUniqueIndexPromotionState{
		phase: loadUniqueIndexPromotionEligible,
	}
	target := &plan.LockTarget{
		TableId:   42,
		LockTable: true,
	}
	node := &plan.Node{LockTargets: []*plan.LockTarget{target}}
	scopes := []*Scope{{}}

	got, err := c.compileLock(node, scopes)
	require.NoError(t, err)
	require.Equal(t, scopes, got)
	require.Equal(t, []*plan.LockTarget{target}, node.LockTargets,
		"physical compilation must not mutate the canonical plan")
	require.NotSame(t, target, c.lockTables[target.TableId])
	require.False(t, c.lockTables[target.TableId].LockTableAtTheEnd)
	require.False(t, target.LockTableAtTheEnd,
		"physical annotations must stay on the compiler-local copy")
}

func TestCompileLockNonCandidatePreservesExactMainMutation(t *testing.T) {
	c := NewMockCompile(t)
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_INSERT,
		LoadTag:  true,
	}}}
	c.lockTables = make(map[uint64]*plan.LockTarget)
	target := &plan.LockTarget{TableId: 42, LockTable: true}
	node := &plan.Node{LockTargets: []*plan.LockTarget{target}}

	got, err := c.compileLock(node, []*Scope{{}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Empty(t, node.LockTargets)
	require.Same(t, target, c.lockTables[target.TableId])
}

func TestConstructLockOpPreservesSharedTableMode(t *testing.T) {
	for _, lockTable := range []bool{false, true} {
		t.Run(fmt.Sprintf("table=%t", lockTable), func(t *testing.T) {
			node := &plan.Node{LockTargets: []*plan.LockTarget{{
				TableId: 42, PrimaryColTyp: plan.Type{Id: int32(types.T_int64)},
				Mode: lockpb.LockMode_Shared, LockTable: lockTable,
			}}}

			op, err := constructLockOp(node, nil)
			require.NoError(t, err)
			targets := op.CopyToPipelineTarget()
			require.Len(t, targets, 1)
			assert.Equal(t, lockTable, targets[0].LockTable)
			assert.Equal(t, lockpb.LockMode_Shared, targets[0].Mode)
		})
	}
}

func TestValidateForeignKeyParentTxnMode(t *testing.T) {
	ctx := context.Background()
	query := &plan.Query{DetectSqls: []string{"REPLACE_PARENT_LOCK:select 1 for update"}}

	require.NoError(t, validateForeignKeyParentTxnMode(ctx, query, true))
	require.ErrorContains(t, validateForeignKeyParentTxnMode(ctx, query, false),
		"optimistic transaction mode")
	query.DetectSqls = []string{"REPLACE_PARENT_PLAN:"}
	require.NoError(t, validateForeignKeyParentTxnMode(ctx, query, true))
	require.ErrorContains(t, validateForeignKeyParentTxnMode(ctx, query, false),
		"optimistic transaction mode")
	query.DetectSqls = []string{"UPDATE_PARENT_PLAN:"}
	require.NoError(t, validateForeignKeyParentTxnMode(ctx, query, true))
	require.ErrorContains(t, validateForeignKeyParentTxnMode(ctx, query, false),
		"UPDATE on a referenced parent table")
	require.NoError(t, validateForeignKeyParentTxnMode(ctx,
		&plan.Query{DetectSqls: []string{"select true"}}, false))
	require.NoError(t, validateForeignKeyParentTxnMode(ctx, nil, false))
}

func TestLockTableLocksAllPrePipelineTargets(t *testing.T) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			runtime.SetupServiceBasedRuntime("s1", rt)
			lockservice.RunLockServicesForTest(
				zap.DebugLevel,
				[]string{"s1"},
				time.Second,
				func(_ lockservice.LockTableAllocator, services []lockservice.LockService) {
					rt.SetGlobalVariables(runtime.LockService, services[0])

					ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
					defer cancel()

					sender, err := rpc.NewSender(rpc.Config{}, rt)
					require.NoError(t, err)

					txnClient := client.NewTxnClient("", sender, client.WithLockService(services[0]))
					txnClient.Resume()
					defer func() {
						require.NoError(t, txnClient.Close())
					}()

					txnOp, err := txnClient.New(ctx, timestamp.Timestamp{})
					require.NoError(t, err)
					defer func() {
						require.NoError(t, txnOp.Rollback(ctx))
					}()

					proc := process.NewTopProcess(
						ctx,
						mpool.MustNewZero(),
						txnClient,
						txnOp,
						nil,
						services[0],
						nil,
						nil,
						nil,
						nil,
						nil)
					c := &Compile{
						proc: proc,
						lockTables: map[uint64]*plan.LockTarget{
							10: {TableId: 10, PrimaryColTyp: plan.Type{Id: int32(types.T_int32)},
								Mode: lockpb.LockMode_Shared},
							11: {TableId: 11, PrimaryColTyp: plan.Type{Id: int32(types.T_int32)}},
						},
					}

					require.NoError(t, c.lockTable())
					require.True(t, txnOp.HasLockTable(10))
					require.True(t, txnOp.HasLockTable(11))

					sharedTxn, err := txnClient.New(ctx, timestamp.Timestamp{})
					require.NoError(t, err)
					defer func() { require.NoError(t, sharedTxn.Rollback(ctx)) }()
					sharedProc := process.NewTopProcess(ctx, mpool.MustNewZero(), txnClient, sharedTxn,
						nil, services[0], nil, nil, nil, nil, nil)
					require.NoError(t, lockop.LockTableWithMode(nil, sharedProc, 10,
						types.T_int32.ToType(), lockpb.LockMode_Shared, false))
				},
				nil,
			)
		},
	)
}
func newTestTxnClientAndOp(
	ctrl *gomock.Controller,
	workspaces ...client.Workspace,
) (client.TxnClient, client.TxnOperator) {
	return newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_SI, workspaces...)
}

func newTestTxnClientAndOpWithIsolation(
	ctrl *gomock.Controller,
	isolation txn.TxnIsolation,
	workspaces ...client.Workspace,
) (client.TxnClient, client.TxnOperator) {
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := client.Workspace(&Ws{})
	if len(workspaces) > 0 {
		workspace = workspaces[0]
	}
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{Isolation: isolation}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().CheckLockTableBinds(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

func TestPlanSnapshotGenerationCaptureAndScopeReuse(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	currentSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	txnOperator.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
		return txn.TxnMeta{SnapshotTS: currentSnapshot}
	}).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()

	proc := testutil.NewProcess(t)
	serviceRuntime := runtime.ServiceRuntime(proc.GetService())
	originalProtocolVersion, hadProtocolVersion := serviceRuntime.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadProtocolVersion {
			serviceRuntime.SetGlobalVariables(runtime.MOProtocolVersion, originalProtocolVersion)
		} else {
			serviceRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	serviceRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion33)
	c := NewCompile("", "", "", "", "", nil, proc, nil, false, nil, time.Now())
	defer func() {
		c.SetIsPrepare(false)
		c.Release()
	}()
	proc.Base.TxnOperator = txnOperator
	c.capturePlanSnapshot()
	c.captureStringShuffleHashAlgorithm()
	require.Equal(t, process.StringShuffleHashComplete, proc.StringShuffleHashAlgorithm())

	child := proc.NewNoContextChildProc(0)
	got, ok := child.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, currentSnapshot, got)
	require.Equal(t, process.StringShuffleHashComplete, child.StringShuffleHashAlgorithm())

	// Prepared execution reuses the same compiled-plan generation. Applying the
	// binding to a newer transaction process must not recapture its snapshot.
	c.SetPlanGenerationReused(true)
	currentSnapshot = timestamp.Timestamp{PhysicalTime: 20}
	serviceRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion32)
	require.NoError(t, c.Reset(proc, time.Now(), nil, "execute prepared_stmt"))
	got, ok = proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, got)
	require.True(t, proc.PlanGenerationReused())
	require.Equal(t, process.StringShuffleHashLegacy, proc.StringShuffleHashAlgorithm())
	// The prior execution's already-created child remains immutable until the
	// prepared scope is explicitly reset for the next execution.
	require.Equal(t, process.StringShuffleHashComplete, child.StringShuffleHashAlgorithm())
	scope := &Scope{Proc: child}
	require.NoError(t, scope.resetForReuse(c))
	got, ok = child.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, got)
	require.True(t, child.PlanGenerationReused())
	require.Equal(t, process.StringShuffleHashLegacy, child.StringShuffleHashAlgorithm())

	// A data-only retry recompiles pipelines from the same logical plan. It
	// retains the original binding even after the transaction snapshot moves.
	retry := &Compile{proc: proc}
	c.bindRetryPlanGeneration(retry, false)
	retry.bindPlanSnapshotForCompile()
	retry.bindStringShuffleHashAlgorithmForCompile()
	got, ok = proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 10}, got)
	require.True(t, proc.PlanGenerationReused())
	require.Equal(t, process.StringShuffleHashLegacy, proc.StringShuffleHashAlgorithm())

	// A definition-change retry rebuilds the logical plan and starts a new plan
	// generation at the transaction's refreshed snapshot. The old prepared
	// physical topology becomes ineligible for another execution.
	c.SetIsPrepare(true)
	rebuilt := &Compile{proc: proc}
	c.bindRetryPlanGeneration(rebuilt, true)
	require.True(t, c.PlanGenerationRebuilt())
	rebuilt.bindPlanSnapshotForCompile()
	got, ok = proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, currentSnapshot, got)
	require.False(t, proc.PlanGenerationReused())
	c.inheritPlanSnapshot(rebuilt)

	// A later data-only retry of that rebuilt plan inherits the new generation,
	// not the stale generation that originally encountered the DDL fence.
	currentSnapshot = timestamp.Timestamp{PhysicalTime: 30}
	postRebuildRetry := &Compile{proc: proc}
	c.bindRetryPlanGeneration(postRebuildRetry, false)
	postRebuildRetry.bindPlanSnapshotForCompile()
	got, ok = proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 20}, got)
	require.False(t, proc.PlanGenerationReused())

	// The same signal is required when EXECUTE compiles an old prepared logical
	// plan without a cached physical topology.
	uncachedPrepared := &Compile{proc: proc}
	uncachedPrepared.bindRetryPlanGeneration(&Compile{proc: proc}, true)
	require.True(t, uncachedPrepared.PlanGenerationRebuilt())

	// A prepared logical plan without a cached physical pipeline must also keep
	// its original generation when it is compiled inside a newer transaction.
	preparedWithoutCache := &Compile{proc: proc}
	preparedWithoutCache.SetPlanSnapshotTS(timestamp.Timestamp{PhysicalTime: 5})
	preparedWithoutCache.bindPlanSnapshotForCompile()
	got, ok = proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 5}, got)
}

func TestStringShuffleHashCaptureIgnoresParticipantRuntimeAfterAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	const (
		coordinatorID = "string-shuffle-v33-coordinator"
		participantID = "string-shuffle-v32-participant"
	)
	coordinatorRuntime := runtime.NewRuntime(
		metadata.ServiceType_CN, coordinatorID, zap.NewNop())
	participantRuntime := runtime.NewRuntime(
		metadata.ServiceType_CN, participantID, zap.NewNop())
	runtime.SetupServiceBasedRuntime(coordinatorID, coordinatorRuntime)
	runtime.SetupServiceBasedRuntime(participantID, participantRuntime)
	coordinatorRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion33)
	participantRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion32)

	coordinatorProc := testutil.NewProcess(t)
	participantProc := testutil.NewProcess(t)
	defer coordinatorProc.Free()
	defer participantProc.Free()
	coordinatorLock := mock_lock.NewMockLockService(ctrl)
	participantLock := mock_lock.NewMockLockService(ctrl)
	coordinatorLock.EXPECT().GetConfig().Return(
		lockservice.Config{ServiceID: coordinatorID}).AnyTimes()
	participantLock.EXPECT().GetConfig().Return(
		lockservice.Config{ServiceID: participantID}).AnyTimes()
	coordinatorProc.Base.LockService = coordinatorLock
	participantProc.Base.LockService = participantLock

	compile := &Compile{proc: coordinatorProc}
	compile.captureStringShuffleHashAlgorithm()
	require.Equal(t, process.StringShuffleHashComplete,
		coordinatorProc.StringShuffleHashAlgorithm())
	require.False(t, supportsStableStringShuffleHash(participantProc.GetService()))

	// Model the ProcessInfo decoder's copy of the coordinator's exact selection
	// (its codec round trip is covered separately). A new participant must
	// consume that value instead of consulting its v32 local gate during Prepare
	// or remote-pipeline reconstruction.
	participantProc.CopyStringShuffleHashAlgorithmFrom(coordinatorProc)
	require.Equal(t, process.StringShuffleHashComplete,
		participantProc.StringShuffleHashAlgorithm())
	arg := shuffle.NewArgument()
	defer arg.Release()
	arg.ShuffleType = int32(plan.ShuffleType_Hash)
	arg.StringHashKey = true
	_, instruction, err := convertToPipelineInstruction(
		arg, participantProc, &scopeContext{}, 1)
	require.NoError(t, err)
	require.Equal(t, int32(vm.ShuffleStable), instruction.Op)

	// A rollout gate change applies only to the next execution. It cannot alter
	// either process already admitted into this one.
	coordinatorRuntime.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion32)
	require.Equal(t, process.StringShuffleHashComplete,
		coordinatorProc.StringShuffleHashAlgorithm())
	require.Equal(t, process.StringShuffleHashComplete,
		participantProc.StringShuffleHashAlgorithm())
	next := &Compile{proc: coordinatorProc}
	next.captureStringShuffleHashAlgorithm()
	require.Equal(t, process.StringShuffleHashLegacy,
		coordinatorProc.StringShuffleHashAlgorithm())
	require.Equal(t, process.StringShuffleHashComplete,
		participantProc.StringShuffleHashAlgorithm())
}

func TestShuffleConstructionMarksOnlyStringHashKeys(t *testing.T) {
	for _, test := range []struct {
		name       string
		typ        types.T
		stringHash bool
	}{
		{name: "varchar", typ: types.T_varchar, stringHash: true},
		{name: "text", typ: types.T_text, stringHash: true},
		{name: "char", typ: types.T_char, stringHash: true},
		{name: "int64", typ: types.T_int64},
		{name: "binary", typ: types.T_binary},
	} {
		t.Run(test.name, func(t *testing.T) {
			left := &plan.Expr{Typ: plan.Type{Id: int32(test.typ)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}}
			right := &plan.Expr{Typ: plan.Type{Id: int32(test.typ)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 2, ColPos: 0}}}
			stats := &plan.Stats{HashmapStats: &plan.HashMapStats{
				ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Hash,
			}}

			groupArg := constructShuffleArgForGroup(8, &plan.Node{
				Stats: stats, GroupBy: []*plan.Expr{left},
			})
			require.Equal(t, test.stringHash, groupArg.StringHashKey)
			groupArg.Release()

			joinArg := constructShuffleOperatorForJoin(8, &plan.Node{
				Stats: stats,
				OnList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{
					Args: []*plan.Expr{left, right},
				}}}},
			}, true)
			require.Equal(t, test.stringHash, joinArg.StringHashKey)
			joinArg.Release()
		})
	}
}

func TestFrozenResultMetadataRejectsIncompatibleDefinitionRetry(t *testing.T) {
	makeResultPlan := func(name string, typ types.T) *plan.Plan {
		return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			StmtType: plan.Query_SELECT,
			Steps:    []int32{0},
			Headings: []string{name},
			Nodes: []*plan.Node{{
				ProjectList: []*plan.Expr{{Typ: plan.Type{Id: int32(typ)}}},
			}},
		}}}
	}

	original := makeResultPlan("v", types.T_int64)
	c := &Compile{pn: original}
	// Before a consumer materializes metadata, a definition retry may change
	// the output schema and the caller can derive metadata from the new plan.
	require.NoError(t, c.validateRetryResultMetadata(
		context.Background(), makeResultPlan("renamed", types.T_varchar)))

	c.FreezeResultMetadata()
	require.NoError(t, c.validateRetryResultMetadata(
		context.Background(), makeResultPlan("v", types.T_int64)))
	for _, rebuilt := range []*plan.Plan{
		makeResultPlan("renamed", types.T_int64),
		makeResultPlan("v", types.T_varchar),
	} {
		err := c.validateRetryResultMetadata(context.Background(), rebuilt)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)
	}
}

func TestSelectIntoRetryRevalidatesResultArity(t *testing.T) {
	makeResultPlan := func(columnTypes ...types.T) *plan.Plan {
		projectList := make([]*plan.Expr, len(columnTypes))
		headings := make([]string, len(columnTypes))
		for i, typ := range columnTypes {
			projectList[i] = &plan.Expr{Typ: plan.Type{Id: int32(typ)}}
			headings[i] = fmt.Sprintf("c%d", i)
		}
		return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			StmtType: plan.Query_SELECT,
			Steps:    []int32{0},
			Headings: headings,
			Nodes: []*plan.Node{{
				ProjectList: projectList,
			}},
		}}}
	}

	c := &Compile{
		pn: makeResultPlan(types.T_int64, types.T_int64),
		stmt: &tree.Select{IntoVars: []*tree.VarExpr{
			{Name: "a"},
			{Name: "b"},
		}},
	}

	// SELECT INTO consumes values rather than client result metadata, so a
	// same-arity retry may adopt compatible type changes.
	require.NoError(t, c.validateRetryResultMetadata(
		context.Background(), makeResultPlan(types.T_varchar, types.T_int64)))

	// Arity is checked before the first attempt. A definition retry must repeat
	// that check because an empty rebuilt result never invokes the row callback.
	err := c.validateRetryResultMetadata(
		context.Background(), makeResultPlan(types.T_int64, types.T_int64, types.T_int64))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongNumberOfColumnsInSelect), err)
}

func TestCompileReleaseClearsPlanSnapshotTransport(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := NewCompile("", "", "", "", "", nil, proc, nil, false, nil, time.Now())
	c.SetIsPrepare(true)
	planSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	c.SetPlanSnapshotTS(planSnapshot)

	c.Release()
	_, ok := proc.GetPlanSnapshotTS()
	require.False(t, ok)

	// A cached Compile retains ownership and can bind the same generation to a
	// later execution even though the Process transport was cleared.
	c.applyPlanSnapshot()
	got, ok := proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, planSnapshot, got)

	c.SetIsPrepare(false)
	c.Release()
}

var (
	_ func(*Compile, client.TxnOperator)       = MarkQueryRunning
	_ func(*Compile, client.TxnOperator) error = TryMarkQueryRunning
)

func TestMarkQueryRunningPreservesLegacyContract(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), "select 1").Return(uint64(0))
	txnOperator.EXPECT().ExitRunSqlWithToken(uint64(0))

	c := &Compile{
		proc:      testutil.NewProcess(t),
		originSQL: "select 1",
	}
	MarkQueryRunning(c, txnOperator)
	require.True(t, c.proc.GetBaseProcessRunningStatus())
	require.Zero(t, c.runSqlToken)

	MarkQueryDone(c, txnOperator)
	require.False(t, c.proc.GetBaseProcessRunningStatus())
}

func TestTryMarkQueryRunningRejectsSealedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	expectedErr := moerr.NewTxnClosedNoCtx([]byte("sealed"))
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), "select 1").
		Return(uint64(0), expectedErr)

	c := &Compile{
		proc:      testutil.NewProcess(t),
		originSQL: "select 1",
	}
	err := TryMarkQueryRunning(c, txnOperator)
	require.ErrorIs(t, err, expectedErr)
	require.Zero(t, c.runSqlToken)
	require.False(t, c.proc.GetBaseProcessRunningStatus())
}

func newTestTxnClientAndOpWithPessimistic(ctrl *gomock.Controller) (client.TxnClient, client.TxnOperator) {
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
		Mode: txn.TxnMode_Pessimistic,
	}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().CheckLockTableBinds(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	return txnClient, txnOperator
}

func TestDebugLogFor19288(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		bsql      string
		originSQL string
	}{
		{
			name:      "Retry Error",
			err:       moerr.NewTxnNeedRetryNoCtx(),
			bsql:      "SELECT * FROM test_table",
			originSQL: "INSERT INTO test_table VALUES (1, 'test')",
		},
		{
			name:      "Non-Retry Error",
			err:       moerr.NewInternalErrorNoCtx("internal error"),
			bsql:      "SELECT * FROM test_table",
			originSQL: "INSERT INTO test_table VALUES (1, 'test')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			c := NewMockCompile(t)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
				Isolation: txn.TxnIsolation_RC,
			}).AnyTimes()
			c.proc.Base.TxnOperator = txnOperator
			c.originSQL = tt.originSQL
			c.debugLogFor19288(tt.err, tt.bsql)
		})
	}
}

func TestPreferPrimaryScopeResult(t *testing.T) {
	cleanupErr := process.ErrPipelineEndSignalDeliveryFailed
	executionErr := moerr.NewDuplicateEntryNoCtx("1000000", "")
	joinedExecutionErr := errors.Join(executionErr, context.Canceled)
	joinedDeadlineErr := errors.Join(context.DeadlineExceeded, context.Canceled)
	queryInterrupted := moerr.NewQueryInterrupted(context.Background())
	joinedCancellationErr := errors.Join(context.Canceled, queryInterrupted)
	joinMapCancellationErr := message.NewJoinMapBuildError(context.Canceled).AsError()
	joinMapDeadlineErr := message.NewJoinMapBuildError(context.DeadlineExceeded).AsError()
	internalCancelCtx, cancelInternal := context.WithCancelCause(context.Background())
	cancelInternal(executionErr)
	internalNormalCancelCtx, cancelInternalNormal := context.WithCancelCause(context.Background())
	cancelInternalNormal(nil)
	activeQueryCtx := context.Background()
	externalCancelCtx, cancelExternal := context.WithCancel(context.Background())
	cancelExternal()
	externalDeadlineCtx, cancelExternalDeadline := context.WithTimeout(context.Background(), 0)
	defer cancelExternalDeadline()
	queryDeadlineCauseCtx, cancelQueryDeadlineCause := context.WithTimeoutCause(
		context.Background(), 0, moerr.CauseInternalExecutorExec)
	defer cancelQueryDeadlineCause()
	pipelineDeadlineCauseCtx, cancelPipelineDeadlineCause := context.WithCancelCause(queryDeadlineCauseCtx)
	defer cancelPipelineDeadlineCause(nil)
	externalCause := moerr.NewInternalErrorNoCtx("client canceled query")
	externalCauseCtx, cancelExternalCause := context.WithCancelCause(context.Background())
	cancelExternalCause(externalCause)
	remoteQueryCtx, cancelRemoteQuery := context.WithCancel(
		context.WithValue(context.Background(), defines.RemoteRunContext{}, true))
	remotePipelineCtx, cancelRemotePipeline := context.WithCancelCause(remoteQueryCtx)
	cancelRemoteQuery()
	defer cancelRemotePipeline(nil)

	tests := []struct {
		name      string
		current   scopeRunResult
		candidate scopeRunResult
		want      error
	}{
		{name: "first error", candidate: scopeRunResult{err: cleanupErr}, want: cleanupErr},
		{name: "execution error replaces cleanup fallback", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: executionErr}, want: executionErr},
		{name: "joined execution error replaces cleanup fallback", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: joinedExecutionErr}, want: joinedExecutionErr},
		{name: "joined independent deadline replaces cleanup fallback", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: joinedDeadlineErr}, want: context.DeadlineExceeded},
		{name: "causal cancellation replaces cleanup fallback with execution error", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: context.Canceled, ctx: internalCancelCtx}, want: executionErr},
		{name: "external cancellation replaces cleanup fallback with external cause", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: context.Canceled, ctx: externalCauseCtx}, want: externalCause},
		{name: "cleanup fallback does not replace execution error", current: scopeRunResult{err: executionErr}, candidate: scopeRunResult{err: cleanupErr}, want: executionErr},
		{name: "unresolved canceled sibling is secondary", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: context.Canceled}, want: cleanupErr},
		{name: "unresolved interrupted sibling is secondary", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: queryInterrupted}, want: cleanupErr},
		{name: "unresolved joined cancellation is secondary", current: scopeRunResult{err: cleanupErr}, candidate: scopeRunResult{err: joinedCancellationErr}, want: cleanupErr},
		{name: "internally canceled sibling resolves to execution error", current: scopeRunResult{err: context.Canceled, ctx: internalCancelCtx}, candidate: scopeRunResult{err: executionErr}, want: executionErr},
		{name: "join map cancellation resolves to execution error", current: scopeRunResult{err: joinMapCancellationErr, ctx: internalCancelCtx}, candidate: scopeRunResult{err: executionErr}, want: executionErr},
		{name: "normal internal cancellation is secondary", current: scopeRunResult{err: context.Canceled, ctx: internalNormalCancelCtx, queryCtx: activeQueryCtx}, candidate: scopeRunResult{err: executionErr}, want: executionErr},
		{name: "internally interrupted sibling resolves to execution error", current: scopeRunResult{err: queryInterrupted, ctx: internalCancelCtx}, candidate: scopeRunResult{err: executionErr}, want: executionErr},
		{name: "remote query cancellation remains primary", current: scopeRunResult{err: queryInterrupted, ctx: remotePipelineCtx, queryCtx: remoteQueryCtx}, want: context.Canceled},
		{name: "plain external cancellation remains primary", current: scopeRunResult{err: context.Canceled, ctx: externalCancelCtx, queryCtx: externalCancelCtx}, candidate: scopeRunResult{err: executionErr}, want: context.Canceled},
		{name: "external deadline remains primary", current: scopeRunResult{err: context.DeadlineExceeded, ctx: externalDeadlineCtx, queryCtx: externalDeadlineCtx}, candidate: scopeRunResult{err: executionErr}, want: context.DeadlineExceeded},
		{name: "join map deadline remains primary", current: scopeRunResult{err: joinMapDeadlineErr, ctx: externalDeadlineCtx, queryCtx: externalDeadlineCtx}, candidate: scopeRunResult{err: executionErr}, want: context.DeadlineExceeded},
		{name: "query deadline classification survives custom timeout cause", current: scopeRunResult{err: context.DeadlineExceeded, ctx: pipelineDeadlineCauseCtx, queryCtx: queryDeadlineCauseCtx}, candidate: scopeRunResult{err: executionErr}, want: context.DeadlineExceeded},
		{name: "external cancellation cause remains primary", current: scopeRunResult{err: context.Canceled, ctx: externalCauseCtx, queryCtx: externalCauseCtx}, candidate: scopeRunResult{err: executionErr}, want: externalCause},
		{name: "join map external cancellation cause remains primary", current: scopeRunResult{err: joinMapCancellationErr, ctx: externalCauseCtx, queryCtx: externalCauseCtx}, candidate: scopeRunResult{err: executionErr}, want: externalCause},
		{name: "first substantive error remains", current: scopeRunResult{err: executionErr}, candidate: scopeRunResult{err: moerr.NewInternalErrorNoCtx("later")}, want: executionErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := preferPrimaryScopeResult(tt.current, tt.candidate)
			got, _ = got.resolveCancelCause()
			if errors.Is(tt.want, context.Canceled) || errors.Is(tt.want, context.DeadlineExceeded) {
				require.ErrorIs(t, got.err, tt.want)
			} else {
				require.Same(t, tt.want, got.err)
			}
		})
	}
}

type scopeRunCancelErrorOperator struct {
	*colexec.MockOperator
	cancelCause error
	runErr      error
}

func (op *scopeRunCancelErrorOperator) Call(proc *process.Process) (vm.CallResult, error) {
	proc.Cancel(op.cancelCause)
	return vm.NewCallResult(), op.runErr
}

func TestScopeRunPreservesPrimaryErrorAcrossCancellation(t *testing.T) {
	primaryErr := moerr.NewInternalErrorNoCtx("hash build memory budget exceeded")
	tests := []struct {
		name        string
		cancelCause error
		runErr      error
		want        error
	}{
		{
			name:        "substantive execution error survives normal sibling cancellation",
			cancelCause: nil,
			runErr:      primaryErr,
			want:        primaryErr,
		},
		{
			name:        "joined execution error survives normal sibling cancellation",
			cancelCause: nil,
			runErr:      errors.Join(primaryErr, context.Canceled),
			want:        primaryErr,
		},
		{
			name:        "cancellation resolves to substantive pipeline cause",
			cancelCause: primaryErr,
			runErr:      context.Canceled,
			want:        primaryErr,
		},
		{
			name:        "normal internal cancellation remains secondary",
			cancelCause: nil,
			runErr:      context.Canceled,
			want:        nil,
		},
		{
			name:        "independent operator deadline survives normal cancellation",
			cancelCause: nil,
			runErr:      context.DeadlineExceeded,
			want:        context.DeadlineExceeded,
		},
		{
			name:        "joined independent deadline survives normal cancellation",
			cancelCause: nil,
			runErr:      errors.Join(context.DeadlineExceeded, context.Canceled),
			want:        context.DeadlineExceeded,
		},
		{
			name:        "joined cancellation fallout remains secondary",
			cancelCause: nil,
			runErr: errors.Join(
				context.Canceled,
				moerr.NewQueryInterrupted(context.Background())),
			want: nil,
		},
		{
			name:        "joined cancellation fallout resolves to substantive cause",
			cancelCause: primaryErr,
			runErr: errors.Join(
				context.Canceled,
				moerr.NewQueryInterrupted(context.Background())),
			want: primaryErr,
		},
		{
			name:        "joined independent deadline survives substantive cancellation cause",
			cancelCause: primaryErr,
			runErr:      errors.Join(context.DeadlineExceeded, context.Canceled),
			want:        context.DeadlineExceeded,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.BuildPipelineContext(context.Background())
			op := &scopeRunCancelErrorOperator{
				MockOperator: colexec.NewMockOperator(),
				cancelCause:  test.cancelCause,
				runErr:       test.runErr,
			}
			scope := &Scope{RootOp: op, Proc: proc}
			compile := &Compile{proc: proc}

			got := scope.Run(compile)
			if test.want == nil {
				require.NoError(t, got)
			} else {
				require.ErrorIs(t, got, test.want)
			}
		})
	}
}

func TestScopeRunPreservesQueryDeadlineClassification(t *testing.T) {
	tests := []struct {
		name   string
		runErr error
	}{
		{name: "deadline", runErr: context.DeadlineExceeded},
		{name: "canceled child", runErr: context.Canceled},
		{name: "query interrupted", runErr: moerr.NewQueryInterrupted(context.Background())},
		{name: "joined deadline and cancellation", runErr: errors.Join(context.DeadlineExceeded, context.Canceled)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			deadlineCtx, cancelDeadline := context.WithTimeoutCause(
				context.Background(), 0, moerr.CauseInternalExecutorExec)
			defer cancelDeadline()
			queryCtx := proc.Base.GetContextBase().BuildQueryCtx(deadlineCtx)
			proc.BuildPipelineContext(queryCtx)

			op := &scopeRunCancelErrorOperator{
				MockOperator: colexec.NewMockOperator(),
				runErr:       test.runErr,
			}
			got := (&Scope{RootOp: op, Proc: proc}).Run(&Compile{proc: proc})
			require.ErrorIs(t, got, context.DeadlineExceeded)
			require.NotErrorIs(t, got, moerr.CauseInternalExecutorExec)

			attached := moerr.AttachCause(deadlineCtx, got)
			require.ErrorIs(t, attached, context.DeadlineExceeded)
			require.ErrorIs(t, attached, moerr.CauseInternalExecutorExec)
		})
	}
}

func TestLockMeta_doLock(t *testing.T) {
	lm := &LockMeta{
		database_table_id: 11230,
		table_table_id:    123123,
		metaTables: map[string]struct{}{
			"test1": {},
		},
		lockDbExe:    nil,
		lockTableExe: nil,
		lockMetaVecs: nil,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Ctx = context.Background()
	eng := mock_frontend.NewMockEngine(ctrl)

	assert.Error(t, lm.doLock(eng, proc))
}

func TestLockMetaInitRetriesAfterPartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(context.Background(), catalog.System_Account)
	eng := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	databaseRel := mock_frontend.NewMockRelation(ctrl)
	tableRel := mock_frontend.NewMockRelation(ctrl)
	lookupErr := moerr.NewInternalErrorNoCtx("lookup mo_tables")

	eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, gomock.Any()).Return(database, nil).Times(2)
	database.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(databaseRel, nil).Times(2)
	database.EXPECT().Relation(gomock.Any(), catalog.MO_TABLES, gomock.Any()).Return(nil, lookupErr).Times(1)
	database.EXPECT().Relation(gomock.Any(), catalog.MO_TABLES, gomock.Any()).Return(tableRel, nil).Times(1)
	databaseRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).Times(1)
	tableRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(2)).Times(1)
	tableRel.EXPECT().Reset(gomock.Any()).Return(nil).Times(1)
	databaseRel.EXPECT().Reset(gomock.Any()).Return(nil).Times(1)

	lm := NewLockMeta()
	require.ErrorIs(t, lm.initLockExe(eng, proc), lookupErr)
	require.Nil(t, lm.lockDbExe)
	require.Nil(t, lm.lockTableExe)
	require.Nil(t, lm.database_rel)
	require.Nil(t, lm.table_rel)

	require.NoError(t, lm.initLockExe(eng, proc))
	require.NotNil(t, lm.lockDbExe)
	require.NotNil(t, lm.lockTableExe)
	require.Same(t, databaseRel, lm.database_rel)
	require.Same(t, tableRel, lm.table_rel)
	require.NoError(t, lm.initLockExe(eng, proc))

	lm.clear(proc)
	proc.Free()
}

func TestCompileClearReleasesLockMetaBeforeProcess(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := allocateNewCompile(proc)
	c.lockMeta = NewLockMeta()
	c.lockMeta.lockMetaVecs = []*vector.Vector{vector.NewVec(types.T_uint32.ToType())}

	require.NotPanics(t, c.clear)
	require.Nil(t, c.proc)
	require.Nil(t, c.lockMeta)
}

func TestCompileShuffleGroupUsesDistributedPathWhenScopeMcpuDiffersFromDop(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope := newShuffleGroupInputScope(t, 1)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 16)
	for _, resultScope := range result {
		require.IsType(t, &group.Group{}, resultScope.RootOp)
	}
	require.Len(t, result[0].PreScopes, 1)
	require.IsType(t, &shuffle.Shuffle{}, result[0].PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))
}

func TestCompileShuffleGroupSkipsNormalShuffleWithSingleOwner(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(1)
	scope := newShuffleGroupInputScope(t, 1)
	input := scope.RootOp

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 1)
	require.Same(t, scope, result[0])
	groupOp, ok := result[0].RootOp.(*group.Group)
	require.True(t, ok)
	require.Same(t, input, groupOp.GetOperatorBase().GetChildren(0),
		"one physical owner must not pay for a one-bucket shuffle and dispatch")
}

func TestCompileShuffleGroupSkipsSingleOwnerWithoutDroppingInputs(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(1)
	inputs := []*Scope{
		newShuffleGroupInputScope(t, 1),
		newShuffleGroupInputScope(t, 1),
	}

	result := c.compileShuffleGroup(aggNode, inputs, nodes)

	require.Len(t, result, 1)
	require.IsType(t, &group.MergeGroup{}, result[0].RootOp)
	require.Len(t, result[0].PreScopes, len(inputs))
	for _, input := range result[0].PreScopes {
		require.IsType(t, &group.Group{},
			input.RootOp.GetOperatorBase().GetChildren(0))
	}
}

func TestCompileShuffleGroupKeepsOrderedSingleOwnerSingleStage(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	c.proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "group_concat_max_len", name)
		require.True(t, system)
		require.False(t, global)
		return int64(1024), nil
	})
	aggNode, nodes := newShuffleGroupTestNodes(1)
	aggNode.AggList = []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				ObjName: plan2.NameGroupConcat,
			},
			AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		}},
	}}
	scope := newShuffleGroupInputScope(t, 1)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 1)
	groupOp, ok := result[0].RootOp.(*group.Group)
	require.True(t, ok)
	require.True(t, groupOp.NeedEval)
	require.Len(t, result[0].PreScopes, 1)
	require.Same(t, scope, result[0].PreScopes[0])
}

func TestCompileShuffleGroupKeepsNormalShuffleAcrossCNsAtDopOne(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	c.addr = "cn-1:6001"
	c.cnList = engine.Nodes{
		{Id: "cn-1", Addr: "cn-1:6001", Mcpu: 1},
		{Id: "cn-2", Addr: "cn-2:6001", Mcpu: 1},
	}
	aggNode, nodes := newShuffleGroupTestNodes(1)
	scope := newShuffleGroupInputScope(t, 1)
	scope.NodeInfo = c.cnList[0]

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 2,
		"DOP one on each CN still exposes two physical aggregate owners")
	for _, resultScope := range result {
		require.IsType(t, &group.Group{}, resultScope.RootOp)
	}
	require.Len(t, result[0].PreScopes, 1)
	require.IsType(t, &shuffle.Shuffle{},
		result[0].PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))
}

func TestCompileShuffleGroupSupportsOrderedGroupConcat(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	c.proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "group_concat_max_len", name)
		require.True(t, system)
		require.False(t, global)
		return int64(1024), nil
	})
	aggNode, nodes := newShuffleGroupTestNodes(16)
	aggNode.AggList = []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				ObjName: plan2.NameGroupConcat,
			},
			AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		}},
	}}
	scope := newShuffleGroupInputScope(t, 1)

	require.True(t, hasOrderedGroupConcat(aggNode))
	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 16)
	for _, resultScope := range result {
		groupOp, ok := resultScope.RootOp.(*group.Group)
		require.True(t, ok)
		require.True(t, groupOp.NeedEval)
	}
	require.IsType(t, &shuffle.Shuffle{}, result[0].PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))
}

func TestCompileShuffleGroupGatesOrderedAggregateByProtocolVersion(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, _ := newShuffleGroupTestNodes(16)
	aggNode.AggList = []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				ObjName: plan2.NameGroupConcat,
			},
			AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		}},
	}}
	rt := runtime.ServiceRuntime(c.proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion5)
	require.False(t, c.supportsRemoteOrderedAggregates())
	require.False(t, c.canCompileShuffleGroup(aggNode),
		"mixed-version clusters must keep the final ordered aggregate local")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion6)
	require.True(t, c.supportsRemoteOrderedAggregates())
	require.True(t, c.canCompileShuffleGroup(aggNode))

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion5)
	require.False(t, c.canCompileShuffleGroup(aggNode),
		"rollback must disable the v6 pipeline field before contacting old CNs")

	aggNode.AggList = nil
	require.True(t, c.canCompileShuffleGroup(aggNode),
		"legacy shuffle aggregates remain safe on protocol v5")
}

func TestCompileShuffleGroupGatesVarianceByProtocolVersion(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, _ := newShuffleGroupTestNodes(16)
	aggNode.AggList = []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.VAR_POP) << 32},
		}},
	}}
	rt := runtime.ServiceRuntime(c.proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion34)
	require.False(t, c.supportsRemoteVarianceAggregates())
	require.False(t, c.canCompileShuffleGroup(aggNode),
		"mixed-version clusters must keep exponent-scaled variance state local")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion35)
	require.True(t, c.supportsRemoteVarianceAggregates())
	require.True(t, c.canCompileShuffleGroup(aggNode))
}

func TestCompileShuffleGroupGatesOrderedSetPercentileByProtocolVersion(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, _ := newShuffleGroupTestNodes(16)
	aggNode.AggList = []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				ObjName: plan2.NamePercentileCont,
			},
			Args: []*plan.Expr{
				aggNode.GroupBy[0],
				plan2.MakePlan2Float64ConstExprWithType(0.5),
			},
		}},
	}}
	rt := runtime.ServiceRuntime(c.proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion16)
	require.False(t, c.supportsRemoteOrderedSetAggregates())
	require.False(t, c.canCompileShuffleGroup(aggNode),
		"mixed-version clusters must keep ordered-set percentile aggregates local")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion17)
	require.True(t, c.supportsRemoteOrderedSetAggregates())
	require.True(t, c.canCompileShuffleGroup(aggNode))
}

func TestCompilePartitionTopNGatedByProtocolVersion(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	rt := runtime.ServiceRuntime(c.proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion17)
	require.False(t, c.supportsRemotePartitionTopN())

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion19)
	require.True(t, c.supportsRemotePartitionTopN())

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion17)
	require.False(t, c.supportsRemotePartitionTopN(), "rollback must select the legacy partition path")
}

func TestCompilePartitionTopNPhysicalTopology(t *testing.T) {
	newNode := func() *plan.Node {
		return &plan.Node{
			NodeType: plan.Node_PARTITION,
			OrderBy: []*plan.OrderBySpec{
				{Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}},
				{Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}},
			},
			Limit:            plan2.MakePlan2Uint64ConstExprWithType(1),
			PartitionByCount: 1,
		}
	}

	t.Run("v18 coalesces candidates for window", func(t *testing.T) {
		c := newCompileForShuffleGroupTest(t)
		rt := runtime.ServiceRuntime(c.proc.GetService())
		defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion19)

		partitionScopes := c.compilePartition(newNode(), []*Scope{newShuffleGroupInputScope(t, 1)})
		require.Len(t, partitionScopes, 1)
		physicalPartition := partitionScopes[0].RootOp.(*partitionop.Partition)
		require.NotNil(t, physicalPartition.Limit)
		require.True(t, physicalPartition.PreReduce)
		partitionScopes[0].setRootOperator(projection.NewArgument())

		windowScopes := c.compileWin(&plan.Node{}, partitionScopes)
		physicalWindow := windowScopes[0].RootOp.(*windowop.Window)
		require.True(t, physicalWindow.PartitionTopN)
	})

	t.Run("v17 keeps legacy window contract", func(t *testing.T) {
		c := newCompileForShuffleGroupTest(t)
		rt := runtime.ServiceRuntime(c.proc.GetService())
		defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion17)

		partitionScopes := c.compilePartition(newNode(), []*Scope{newShuffleGroupInputScope(t, 1)})
		physicalPartition := partitionScopes[0].RootOp.(*partitionop.Partition)
		require.Nil(t, physicalPartition.Limit)
		windowScopes := c.compileWin(&plan.Node{}, partitionScopes)
		physicalWindow := windowScopes[0].RootOp.(*windowop.Window)
		require.False(t, physicalWindow.PartitionTopN)
	})
}

func TestCompileHashPartitionPhysicalTopology(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	node := &plan.Node{
		NodeType:           plan.Node_PARTITION,
		PartitionAlgorithm: plan.Node_PARTITION_ALGORITHM_HASH,
		SpillMem:           4096,
		OrderBy: []*plan.OrderBySpec{{
			Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		}},
	}
	left := newShuffleGroupInputScope(t, 1)
	right := newShuffleGroupInputScope(t, 1)

	result := c.compilePartition(node, []*Scope{left, right})

	require.Len(t, result, 1)
	physical, ok := result[0].RootOp.(*partitionop.Partition)
	require.True(t, ok)
	require.Equal(t, plan.Node_PARTITION_ALGORITHM_HASH, physical.Algorithm)
	require.Equal(t, int64(4096), physical.SpillMem)
	require.Len(t, result[0].PreScopes, 2)
	for _, input := range result[0].PreScopes {
		// newMergeScope adds a Connector over the previous input root. HASH must
		// leave that previous root untouched instead of adding local Order.
		require.IsType(t, &colexec.MockOperator{}, input.RootOp.GetOperatorBase().GetChildren(0))
	}
}

func TestCompileHashPartitionGatedByProtocolVersion(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	rt := runtime.ServiceRuntime(c.proc.GetService())
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion46)
	require.False(t, c.supportsRemoteHashPartition())
	node := &plan.Node{
		NodeType:           plan.Node_PARTITION,
		PartitionAlgorithm: plan.Node_PARTITION_ALGORITHM_HASH,
		OrderBy: []*plan.OrderBySpec{{
			Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
		}},
	}
	legacy := c.compilePartition(node, []*Scope{newShuffleGroupInputScope(t, 1)})
	require.Len(t, legacy, 1)
	require.Equal(t, plan.Node_PARTITION_ALGORITHM_SORT, legacy[0].RootOp.(*partitionop.Partition).Algorithm)
	require.IsType(t, &orderop.Order{}, legacy[0].PreScopes[0].RootOp.GetOperatorBase().GetChildren(0))

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion47)
	require.True(t, c.supportsRemoteHashPartition())
}

func TestCompileOrderedSetPercentileUsesSingleStageForNonShuffleMerge(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	aggNode.Stats.HashmapStats = nil
	aggNode.AggList = []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				ObjName: plan2.NamePercentileCont,
			},
			Args: []*plan.Expr{
				aggNode.GroupBy[0],
				plan2.MakePlan2Float64ConstExprWithType(0.5),
			},
		}},
	}}
	scope1 := newShuffleGroupInputScope(t, 1)
	scope2 := newShuffleGroupInputScope(t, 1)

	require.True(t, hasOrderedSetPercentile(aggNode))
	result := c.compileOrderedAggregateSingleStage(aggNode, []*Scope{scope1, scope2}, nodes)

	require.Len(t, result, 1)
	groupOp, ok := result[0].RootOp.(*group.Group)
	require.True(t, ok)
	require.True(t, groupOp.NeedEval)
	require.Len(t, result[0].PreScopes, 2)
	require.Contains(t, result[0].PreScopes, scope1)
	require.Contains(t, result[0].PreScopes, scope2)
}

func TestCompileShuffleGroupUsesDistributedPathWhenInputScopesNotSingle(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope1 := newShuffleGroupInputScope(t, 1)
	scope2 := newShuffleGroupInputScope(t, 1)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope1, scope2}, nodes)

	require.Len(t, result, 16)
	for _, resultScope := range result {
		require.IsType(t, &group.Group{}, resultScope.RootOp)
	}
	require.Len(t, result[0].PreScopes, 1)
	for _, input := range result[0].PreScopes {
		require.IsType(t, &shuffle.Shuffle{}, input.RootOp.GetOperatorBase().GetChildren(0))
	}
}

func TestCompileShuffleGroupUsesLocalPathWhenScopeMcpuMatchesDop(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope := newShuffleGroupInputScope(t, 16)

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 1)
	require.Same(t, scope, result[0])
	require.IsType(t, &group.Group{}, result[0].RootOp)
	shuffleOp, ok := result[0].RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
	require.True(t, ok)
	require.Equal(t, int32(16), shuffleOp.BucketNum)
	require.Equal(t, int32(0), shuffleOp.CurrentShuffleIdx)
}

func TestCompileShuffleGroupKeepsNestedShuffleLocal(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	aggNode, nodes := newShuffleGroupTestNodes(16)
	scope := newShuffleGroupInputScope(t, 16)
	inner := shuffle.NewArgument()
	inner.BucketNum = 16
	scope.setRootOperator(inner)
	scope.setRootOperator(colexec.NewMockOperator())

	result := c.compileShuffleGroup(aggNode, []*Scope{scope}, nodes)

	require.Len(t, result, 1)
	require.Same(t, scope, result[0])
	require.IsType(t, &group.Group{}, result[0].RootOp)
	outer, ok := result[0].RootOp.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
	require.True(t, ok)
	require.False(t, outer.DrainAllBuckets)
	middle := outer.GetOperatorBase().GetChildren(0)
	nestedInner, ok := middle.GetOperatorBase().GetChildren(0).(*shuffle.Shuffle)
	require.True(t, ok)
	require.False(t, nestedInner.DrainAllBuckets)
}

func TestCompileShuffleJoinKeepsNestedShuffleLocal(t *testing.T) {
	const dop = int32(16)
	for _, nestedSide := range []string{"probe", "build"} {
		t.Run(nestedSide, func(t *testing.T) {
			c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: int(dop)}})
			node := newShuffleJoinTestNode(dop)
			node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			left := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			right := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			probe := newShuffleJoinTestScope(t, c.cnList[0], int(dop))
			build := newShuffleJoinTestScope(t, c.cnList[0], int(dop))

			inner := shuffle.NewArgument()
			inner.BucketNum = dop
			if nestedSide == "probe" {
				probe.setRootOperator(inner)
				probe.setRootOperator(colexec.NewMockOperator())
			} else {
				build.setRootOperator(inner)
				build.setRootOperator(colexec.NewMockOperator())
			}

			result := c.compileShuffleJoin(node, left, right, []*Scope{probe}, []*Scope{build})

			require.Len(t, result, 1,
				"the asynchronous local exchange permits nested fixed-bucket shuffles")
			require.Same(t, probe, result[0])
			_, probeIsDispatch := probe.RootOp.(*dispatch.Dispatch)
			_, buildIsDispatch := build.RootOp.(*dispatch.Dispatch)
			require.False(t, probeIsDispatch)
			require.False(t, buildIsDispatch)
		})
	}
}

func TestCompileShuffleJoinKeepsReusableLocalShuffle(t *testing.T) {
	const dop = int32(4)
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: int(dop)}})
	node := newShuffleJoinTestNode(dop)
	left := &plan.Node{Stats: &plan.Stats{Dop: dop}}
	right := &plan.Node{Stats: &plan.Stats{Dop: dop}}
	probe := newShuffleJoinTestScope(t, c.cnList[0], int(dop))
	build := newShuffleJoinTestScope(t, c.cnList[0], int(dop))
	inner := shuffle.NewArgument()
	inner.BucketNum = dop
	probe.setRootOperator(inner)

	result := c.compileShuffleJoin(node, left, right, []*Scope{probe}, []*Scope{build})

	require.Len(t, result, 1,
		"reusing an existing probe partition must keep the single local fast path")
}

func TestCompileLocalShuffleJoinOnlySkipsProvenProbeShuffle(t *testing.T) {
	const dop = int32(4)
	tests := []struct {
		name             string
		method           plan.ShuffleMethod
		shuffleType      plan.ShuffleType
		wantProbeShuffle bool
	}{
		{
			name:             "normal strategy repartitions probe",
			method:           plan.ShuffleMethod_Normal,
			wantProbeShuffle: true,
		},
		{
			name:             "normal range strategy repartitions probe",
			method:           plan.ShuffleMethod_Normal,
			shuffleType:      plan.ShuffleType_Range,
			wantProbeShuffle: true,
		},
		{
			name:   "proved reuse keeps probe partition",
			method: plan.ShuffleMethod_Reuse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cn := engine.Node{Addr: "cn1:6001", Mcpu: int(dop)}
			c := newCompileForShuffleJoinTest(t, engine.Nodes{cn})
			node := newShuffleJoinTestNode(dop)
			node.Stats.HashmapStats.ShuffleMethod = tt.method
			node.Stats.HashmapStats.ShuffleType = tt.shuffleType
			left := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			right := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			probe := newShuffleJoinTestScope(t, cn, int(dop))
			build := newShuffleJoinTestScope(t, cn, int(dop))
			originalProbeRoot := probe.RootOp

			result := c.compileLocalShuffleJoin(
				node, left, right, []*Scope{probe}, []*Scope{build},
			)

			require.Len(t, result, 1)
			probeInput := result[0].RootOp.GetOperatorBase().GetChildren(0)
			if tt.wantProbeShuffle {
				require.IsType(t, &shuffle.Shuffle{}, probeInput)
				probeShuffle := probeInput.(*shuffle.Shuffle)
				require.Equal(t, int32(tt.shuffleType), probeShuffle.ShuffleType)
				require.Same(t, originalProbeRoot, probeInput.GetOperatorBase().GetChildren(0))
			} else {
				require.Same(t, originalProbeRoot, probeInput)
			}
		})
	}
}

func TestCompileShuffleJoinDistributesSinkScanHashbuild(t *testing.T) {
	const dop = int32(2)
	tests := []struct {
		name        string
		joinType    plan.Node_JoinType
		isRightJoin bool
		sinkOnBuild bool
	}{
		{
			name:     "probe-side sink inner join",
			joinType: plan.Node_INNER,
		},
		{
			name:        "build-side sink right dedup join",
			joinType:    plan.Node_DEDUP,
			isRightJoin: true,
			sinkOnBuild: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodes := engine.Nodes{
				{Id: "cn-local", Addr: "cn-local:6001", Mcpu: int(dop)},
				{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: int(dop)},
			}
			c := newCompileForShuffleJoinTest(t, nodes)
			c.execType = plan2.ExecTypeAP_MULTICN
			node := newShuffleJoinTestNode(dop)
			node.JoinType = test.joinType
			node.IsRightJoin = test.isRightJoin
			if test.joinType == plan.Node_DEDUP {
				node.DedupJoinCtx = &plan.DedupJoinCtx{}
			}
			node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			left := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			right := &plan.Node{Stats: &plan.Stats{Dop: dop}}

			sinkMerge := merge.NewArgument().WithSinkScan(true)
			sinkRoot := projection.NewArgument()
			sinkRoot.AppendChild(sinkMerge)
			probe := newShuffleJoinTestScope(t, nodes[1], 1)
			build := newShuffleJoinTestScope(t, nodes[1], 1)
			var sinkScope *Scope
			if test.sinkOnBuild {
				build = newShuffleJoinTestScope(t, nodes[0], 1)
				sinkScope = build
			} else {
				probe = newShuffleJoinTestScope(t, nodes[0], 1)
				sinkScope = probe
			}
			sinkScope.RootOp = sinkRoot

			result := c.compileShuffleJoin(node, left, right, []*Scope{probe}, []*Scope{build})

			require.Len(t, result, len(nodes)*int(dop))
			hashbuildByCN := make(map[string]int)
			for _, scope := range result {
				require.NotNil(t, scope)
				require.NotEmpty(t, scope.PreScopes)
				require.IsType(t, &hashbuild.HashBuild{}, scope.PreScopes[0].RootOp)
				hashbuildByCN[scope.NodeInfo.Addr]++
				if scope.NodeInfo.Addr == nodes[1].Addr {
					_, hasSinkScan := sinkScanDependencyNode([]*Scope{scope})
					require.False(t, hasSinkScan,
						"the in-process SINK_SCAN dependency must never enter a remote scope tree")
				}
			}
			require.Equal(t, int(dop), hashbuildByCN[nodes[0].Addr])
			require.Equal(t, int(dop), hashbuildByCN[nodes[1].Addr])

			sinkDispatch, ok := sinkScope.RootOp.(*dispatch.Dispatch)
			require.True(t, ok)
			require.Len(t, sinkDispatch.LocalRegs, int(dop))
			require.Len(t, sinkDispatch.RemoteRegs, int(dop))

			localSinkOwners := 0
			for _, scope := range result {
				_, hasSinkScan := sinkScanDependencyNode([]*Scope{scope})
				if hasSinkScan {
					localSinkOwners++
					require.Equal(t, nodes[0].Addr, scope.NodeInfo.Addr)
				}
			}
			require.Equal(t, 1, localSinkOwners,
				"the local SINK_SCAN producer must be started by exactly one receiver tree")

			grouped := c.groupShuffleBucketsByCNIfNeeded(result)
			require.Len(t, grouped, len(nodes))
			for _, scope := range grouped {
				if scope.NodeInfo.Addr == nodes[1].Addr {
					require.True(t, checkPipelineStandaloneExecutableAtRemote(scope),
						"the remote CN bucket group must own every local receiver targeted by its dispatch")
				}
			}
		})
	}
}

func TestCompileJoinGroupsExternalSinkScanOwner(t *testing.T) {
	const dop = int32(2)
	for _, workerCount := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d scheduled workers", workerCount), func(t *testing.T) {
			workers := make(engine.Nodes, workerCount)
			for i := range workers {
				workers[i] = engine.Node{
					Id:   fmt.Sprintf("cn-remote-%d", i),
					Addr: fmt.Sprintf("cn-remote-%d:6001", i),
					Mcpu: int(dop),
				}
			}
			owner := engine.Node{
				Id:   "cn-sink-owner",
				Addr: "cn-sink-owner:6001",
				Mcpu: int(dop),
			}
			c := newCompileForShuffleJoinTest(t, workers)
			c.addr = owner.Addr
			c.execType = plan2.ExecTypeAP_MULTICN

			node := newShuffleJoinTestNode(dop)
			node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			left := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			right := &plan.Node{Stats: &plan.Stats{Dop: dop}}
			sinkMerge := merge.NewArgument().WithSinkScan(true)
			sinkRoot := projection.NewArgument()
			sinkRoot.AppendChild(sinkMerge)
			probe := newShuffleJoinTestScope(t, owner, 1)
			probe.RootOp = sinkRoot
			build := newShuffleJoinTestScope(t, workers[0], 1)

			buckets := c.compileJoin(node, left, right, []*Scope{probe}, []*Scope{build})
			require.Len(t, buckets, (workerCount+1)*int(dop))

			grouped := c.groupShuffleBucketsByCNIfNeeded(buckets)
			require.Len(t, grouped, workerCount+1)

			groupedByCN := make(map[string]*Scope, len(grouped))
			sinkOwnerGroups := 0
			for _, scope := range grouped {
				groupedByCN[scope.NodeInfo.Addr] = scope
				_, hasSinkScan := sinkScanDependencyNode([]*Scope{scope})
				if hasSinkScan {
					sinkOwnerGroups++
					require.Equal(t, owner.Addr, scope.NodeInfo.Addr)
				}
			}
			require.Equal(t, 1, sinkOwnerGroups)
			require.Contains(t, groupedByCN, owner.Addr)
			for _, worker := range workers {
				remoteGroup, ok := groupedByCN[worker.Addr]
				require.True(t, ok)
				require.True(t, checkPipelineStandaloneExecutableAtRemote(remoteGroup),
					"each remote CN group must own every receiver targeted by its local dispatches")
			}
		})
	}
}

func TestCompileMergeGroupDistinctTopology(t *testing.T) {
	var containsGroup func(vm.Operator) bool
	containsGroup = func(op vm.Operator) bool {
		if _, ok := op.(*group.Group); ok {
			return true
		}
		for _, child := range op.GetOperatorBase().Children {
			if containsGroup(child) {
				return true
			}
		}
		return false
	}
	makeAgg := func(name string, id int32, distinct bool, arg *plan.Expr) *plan.Expr {
		encoded := function.EncodeOverloadID(id, 0)
		if distinct {
			encoded = int64(uint64(encoded) | uint64(function.Distinct))
		}
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: encoded, ObjName: name},
				Args: []*plan.Expr{arg},
			}},
		}
	}

	t.Run("mixed count distinct uses local parallel groups", func(t *testing.T) {
		c := newCompileForShuffleGroupTest(t)
		node, nodes := newShuffleGroupTestNodes(2)
		arg := nodes[0].ProjectList[0]
		node.AggList = []*plan.Expr{
			makeAgg("count", function.COUNT, false, arg),
			makeAgg("count", function.COUNT, true, arg),
		}
		scopes := []*Scope{
			newShuffleGroupInputScope(t, 1),
			newShuffleGroupInputScope(t, 1),
		}

		requiresSingleStage := plan2.RequiresSingleStageDistinctAgg(node)
		require.False(t, requiresSingleStage)
		result := c.compileMergeGroup(node, scopes, nodes, requiresSingleStage)

		require.Len(t, result, 1)
		for _, scope := range scopes {
			require.True(t, containsGroup(scope.RootOp),
				"each input worker should aggregate before MergeGroup")
		}
	})

	t.Run("unsupported distinct keeps one group", func(t *testing.T) {
		c := newCompileForShuffleGroupTest(t)
		node, nodes := newShuffleGroupTestNodes(2)
		arg := nodes[0].ProjectList[0]
		node.AggList = []*plan.Expr{makeAgg("avg", function.AVG, true, arg)}
		scopes := []*Scope{
			newShuffleGroupInputScope(t, 1),
			newShuffleGroupInputScope(t, 1),
		}

		requiresSingleStage := plan2.RequiresSingleStageDistinctAgg(node)
		require.True(t, requiresSingleStage)
		result := c.compileMergeGroup(node, scopes, nodes, requiresSingleStage)

		require.Len(t, result, 1)
		for _, scope := range scopes {
			require.False(t, containsGroup(scope.RootOp))
		}
		require.Len(t, result[0].PreScopes, 1)
		require.True(t, containsGroup(result[0].PreScopes[0].RootOp),
			"non-mergeable DISTINCT states must share one Group operator")
	})
}

func TestCompileLocalPreAggregationKeepsEveryInputScope(t *testing.T) {
	c := newCompileForShuffleGroupTest(t)
	local, nodes := newShuffleGroupTestNodes(4)
	local.Stats.HashmapStats.Shuffle = false
	local.ProjectList = []*plan.Expr{{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: -1, ColPos: 0}},
	}}
	final := &plan.Node{
		NodeType: plan.Node_AGG,
		Stats: &plan.Stats{Dop: 4, HashmapStats: &plan.HashMapStats{
			Shuffle:       true,
			ShuffleColIdx: 0,
			ShuffleType:   plan.ShuffleType_Hash,
		}},
		Children: []int32{1},
		GroupBy: []*plan.Expr{{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
		}},
	}
	require.True(t, isLocalPreAggregationGroup(final, local))
	final.Stats.HashmapStats.Shuffle = false
	require.False(t, isLocalPreAggregationGroup(final, local),
		"a downstream complete Group is required before localizing its child")
	final.Stats.HashmapStats.Shuffle = true
	final.GroupBy[0].GetCol().ColPos = 1
	require.False(t, isLocalPreAggregationGroup(final, local),
		"the downstream Group must reproduce every child key position exactly")
	final.GroupBy[0].GetCol().ColPos = 0
	inputs := []*Scope{
		newShuffleGroupInputScope(t, 1),
		newShuffleGroupInputScope(t, 1),
		newShuffleGroupInputScope(t, 1),
		newShuffleGroupInputScope(t, 1),
	}
	inputRoots := make([]vm.Operator, len(inputs))
	for i := range inputs {
		inputRoots[i] = inputs[i].RootOp
	}

	result := c.compileLocalGroupBy(local, inputs, nodes)

	require.Len(t, result, 4,
		"local pre-dedup output must remain parallel for the parent exchange")
	for i := range result {
		require.Same(t, inputs[i], result[i])
		groupOp, ok := result[i].RootOp.(*group.Group)
		require.True(t, ok)
		require.False(t, groupOp.NeedEval)
		require.Same(t, inputRoots[i], groupOp.GetOperatorBase().GetChildren(0))
		require.Empty(t, result[i].PreScopes,
			"a local-only Group must not introduce a MergeGroup scope")
	}

	nodes = append(nodes, local)
	owners := c.compileShuffleGroup(final, result, nodes)
	require.Len(t, owners, 4)
	for _, owner := range owners {
		require.IsType(t, &group.Group{}, owner.RootOp,
			"the parent exchange must retain four final pair owners")
	}
}

type distinctPreAggregationCompilerContext struct {
	*plan2.MockCompilerContext
	stats *statspb.StatsInfo
	cache *plan2.StatsCache
}

func (c *distinctPreAggregationCompilerContext) Stats(
	_ *plan.ObjectRef,
	_ *plan.Snapshot,
) (*statspb.StatsInfo, error) {
	return c.stats, nil
}

func (c *distinctPreAggregationCompilerContext) GetStatsCache() *plan2.StatsCache {
	return c.cache
}

func TestLocalPreAggregationCompileShapeIsReachableFromSQL(t *testing.T) {
	base := plan2.NewMockCompilerContext(false)
	base.SetContext(context.Background())
	_, tableDef, err := base.Resolve("tpch", "lineitem", nil)
	require.NoError(t, err)
	require.NotNil(t, tableDef)

	stats := plan2.NewStatsInfo()
	stats.TableCnt = 6_000_000
	stats.BlockNumber = 1_000
	stats.NdvMap["l_returnflag"] = 3
	stats.NdvMap["l_orderkey"] = 1_500_000
	cache := plan2.NewStatsCache()
	cache.Set(tableDef.TblId, stats)
	compilerCtx := &distinctPreAggregationCompilerContext{
		MockCompilerContext: base,
		stats:               stats,
		cache:               cache,
	}

	const sql = "select l_returnflag, count(distinct l_orderkey) " +
		"from lineitem group by l_returnflag"
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(statements[0], false)
	require.NoError(t, err)

	found := false
	for _, parent := range query.Nodes {
		if parent == nil || len(parent.Children) != 1 {
			continue
		}
		childID := parent.Children[0]
		if childID < 0 || int(childID) >= len(query.Nodes) {
			continue
		}
		if isLocalPreAggregationGroup(parent, query.Nodes[childID]) {
			found = true
			break
		}
	}
	require.True(t, found,
		"the finalized and column-remapped SQL plan must retain the local-pair compile contract")
}

func newCompileForShuffleGroupTest(t *testing.T) *Compile {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{}
	return c
}

func newShuffleGroupInputScope(t *testing.T, mcpu int) *Scope {
	scope := newScope(Merge)
	scope.NodeInfo = engine.Node{Addr: "127.0.0.1:18000", Mcpu: mcpu}
	scope.Proc = testutil.NewProcess(t)
	scope.setRootOperator(colexec.NewMockOperator())
	return scope
}

func TestCompilePreInsertUkMergesParallelMultiKeyIgnoreInput(t *testing.T) {
	c := NewMockCompile(t)
	c.anal = &AnalyzeModule{}
	input := newScope(Merge)
	input.NodeInfo = engine.Node{Addr: "127.0.0.1:18000", Mcpu: 4}
	input.Proc = c.proc.NewNoContextChildProc(0)
	input.setRootOperator(colexec.NewMockOperator())

	node := &plan.Node{PreInsertUkCtx: &plan.PreInsertUkCtx{InsertIgnoreMultiDedup: true}}
	result := c.compilePreInsertUk(node, []*Scope{input})

	require.Len(t, result, 1)
	require.NotSame(t, input, result[0])
	require.Equal(t, 1, result[0].NodeInfo.Mcpu)
	require.Contains(t, result[0].PreScopes, input)
}

func newShuffleGroupTestNodes(dop int32) (*plan.Node, []*plan.Node) {
	col := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 0},
		},
	}
	child := &plan.Node{
		NodeId:   3,
		NodeType: plan.Node_SORT,
		Stats:    &plan.Stats{Dop: dop},
		ProjectList: []*plan.Expr{
			col,
		},
	}
	agg := &plan.Node{
		NodeId:   4,
		NodeType: plan.Node_AGG,
		Stats: &plan.Stats{
			Dop: dop,
			HashmapStats: &plan.HashMapStats{
				Shuffle:       true,
				ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Range,
				ShuffleMethod: plan.ShuffleMethod_Normal,
			},
		},
		Children: []int32{0},
		GroupBy:  []*plan.Expr{col},
	}
	return agg, []*plan.Node{child}
}

func TestDistributedShuffleJoinFallsBackFromPackedReuse(t *testing.T) {
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	node := newShuffleJoinTestNode(4)
	probe := newShuffleJoinTestScope(t, c.cnList[0], 4)
	build := newShuffleJoinTestScope(t, c.cnList[0], 4)

	result := c.newShuffleJoinScopeList([]*Scope{probe}, []*Scope{build}, node)

	require.Len(t, result, 4)
	require.Equal(t, plan.ShuffleMethod_Reuse, node.Stats.HashmapStats.ShuffleMethod,
		"the physical fallback must not mutate reusable plan metadata")
	probeDispatch, ok := probe.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, probeDispatch.LocalRegs, 4)
	require.Equal(t, []int{0, 1, 2, 3}, probeDispatch.ShuffleRegIdxLocal)
	require.IsType(t, &shuffle.Shuffle{}, probeDispatch.GetOperatorBase().GetChildren(0))
	buildDispatch, ok := build.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, buildDispatch.LocalRegs, 4)
	require.Equal(t, []int{0, 1, 2, 3}, buildDispatch.ShuffleRegIdxLocal)
}

func TestDistributedShuffleJoinKeepsMaterializedReuse(t *testing.T) {
	c := newCompileForShuffleJoinTest(t, engine.Nodes{{Addr: "cn1:6001", Mcpu: 4}})
	node := newShuffleJoinTestNode(4)
	probes := make([]*Scope, 4)
	probeRoots := make([]vm.Operator, 4)
	for i := range probes {
		probes[i] = newShuffleJoinTestScope(t, c.cnList[0], 1)
		probeRoots[i] = probes[i].RootOp
	}
	build := newShuffleJoinTestScope(t, c.cnList[0], 4)

	result := c.newShuffleJoinScopeList(probes, []*Scope{build}, node)

	require.Len(t, result, 4)
	for i := range probes {
		require.Same(t, probes[i], result[i])
		require.Same(t, probeRoots[i], probes[i].RootOp,
			"valid reuse must not add a probe shuffle")
	}
	buildDispatch, ok := build.RootOp.(*dispatch.Dispatch)
	require.True(t, ok)
	require.Len(t, buildDispatch.LocalRegs, 4)
}

func TestDistributedShuffleJoinRejectsMisorderedReuse(t *testing.T) {
	nodes := engine.Nodes{
		{Addr: "cn1:6001", Mcpu: 2},
		{Addr: "cn2:6001", Mcpu: 2},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	c.execType = plan2.ExecTypeAP_MULTICN
	node := newShuffleJoinTestNode(2)
	probes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	builds := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}

	result := c.newShuffleJoinScopeList(probes, builds, node)

	require.Len(t, result, 4)
	require.NotSame(t, probes[0], result[0],
		"noncanonical bucket order must materialize a new distributed shuffle layout")
}

func TestDistributedShuffleJoinRejectsMultiCNDedupReuse(t *testing.T) {
	nodes := engine.Nodes{
		{Addr: "cn1:6001", Mcpu: 2},
		{Addr: "cn2:6001", Mcpu: 2},
	}
	c := newCompileForShuffleJoinTest(t, nodes)
	c.execType = plan2.ExecTypeAP_MULTICN
	node := newShuffleJoinTestNode(2)
	node.JoinType = plan.Node_DEDUP
	probes := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}
	builds := []*Scope{
		newShuffleJoinTestScope(t, nodes[0], 1),
		newShuffleJoinTestScope(t, nodes[1], 1),
	}

	result := c.newShuffleJoinScopeList(probes, builds, node)

	require.Len(t, result, 4,
		"multi-CN DEDUP normalization must force a physical reshuffle")
}

func newCompileForShuffleJoinTest(t *testing.T, nodes engine.Nodes) *Compile {
	c := NewMockCompile(t)
	c.addr = nodes[0].Addr
	c.cnList = nodes
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{}
	return c
}

func newShuffleJoinTestScope(t *testing.T, node engine.Node, mcpu int) *Scope {
	scope := newScope(Remote)
	scope.NodeInfo = scopeNodeWithMcpu(node, mcpu)
	scope.Proc = testutil.NewProcess(t)
	scope.setRootOperator(colexec.NewMockOperator())
	return scope
}

func newShuffleJoinTestNode(dop int32) *plan.Node {
	leftCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 1,
			ColPos: 0,
		}},
	}
	rightCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: 2,
			ColPos: 0,
		}},
	}
	return &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Stats: &plan.Stats{
			Dop:      dop,
			TableCnt: 1000,
			HashmapStats: &plan.HashMapStats{
				Shuffle:       true,
				ShuffleColIdx: 0,
				ShuffleType:   plan.ShuffleType_Hash,
				ShuffleMethod: plan.ShuffleMethod_Reuse,
			},
		},
		OnList: []*plan.Expr{{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Args: []*plan.Expr{leftCol, rightCol},
			}},
		}},
		SendMsgList: []plan.MsgHeader{{
			MsgType: int32(message.MsgJoinMap),
			MsgTag:  1,
		}},
	}
}

// TestNewCompileTxnOffsetForInternalSql verifies the statement-boundary
// contract of NewCompile and Compile.Reset (issue #25557): a compile of a
// user statement advances the workspace snapshot write offset, while an
// internal sub-sql compile (DisableIncrStatement, marked on the process)
// must not touch the shared boundary — it captures the current end of the
// workspace as its own TxnOffset instead.
func TestNewCompileTxnOffsetForInternalSql(t *testing.T) {
	t.Run("user statement advances the boundary", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ws := mock_frontend.NewMockWorkspace(ctrl)
		ws.EXPECT().UpdateSnapshotWriteOffset().Times(1)
		ws.EXPECT().GetSnapshotWriteOffset().Return(3).Times(1)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()

		proc := testutil.NewProcess(t)
		proc.Base.TxnOperator = txnOp

		c := NewCompile("test", "test", "select 1", "", "", nil, proc, nil, false, nil, time.Now())
		require.Equal(t, 3, c.TxnOffset)
	})

	t.Run("internal sub-sql must not advance the boundary", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ws := mock_frontend.NewMockWorkspace(ctrl)
		// no UpdateSnapshotWriteOffset expectation: the mock controller
		// fails the test if the internal compile advances the boundary
		ws.EXPECT().WriteOffset().Return(uint64(7)).Times(1)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()

		proc := testutil.NewProcess(t)
		proc.Base.TxnOperator = txnOp
		proc.SetIncrStatementDisabled(true)

		c := NewCompile("test", "test", "select 1", "", "", nil, proc, nil, false, nil, time.Now())
		require.Equal(t, 7, c.TxnOffset)
	})
}
