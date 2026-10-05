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

package frontend

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestViewSchemaFrozenVariablesPreserveAttributesAndOwnBytes(t *testing.T) {
	bytes := []byte{0, 0xff, 'A'}
	wantType := pb.Type{Id: int32(types.T_varbinary), Width: 3}
	ses := &Session{
		feSessionImpl: feSessionImpl{
			sesSysVars: &SystemVariables{mp: map[string]interface{}{"sql_mode": "ANSI_QUOTES", "lower_case_table_names": int64(0)}},
			gSysVars:   &SystemVariables{mp: map[string]interface{}{"max_connections": int64(111)}},
		},
		userDefinedVars: map[string]*UserDefinedVar{
			"payload": {Value: bytes, Sql: "set @payload = 0x00ff41", IsBin: true, Type: wantType, RuntimeStringDomain: types.RuntimeStringBinary},
			"amount":  {Value: "12.34", Type: pb.Type{Id: int32(types.T_decimal64), Width: 8, Scale: 2}, PrepareParamKind: vector.PrepareParamDecimal},
		},
	}
	source := &ExecCtx{reqCtx: t.Context(), ses: ses}
	variables, err := freezeViewSchemaVariables(ses, source)
	require.NoError(t, err)
	c := &viewSchemaCompilerContext{variables: variables}

	// 修改原有值和整个变量槽位，不能改变已冻结的值或附带类型。
	bytes[1] = 0
	ses.userDefinedVars["payload"] = &UserDefinedVar{Value: "replacement", Type: pb.Type{Id: int32(types.T_varchar)}}
	ses.userDefinedVars["amount"] = &UserDefinedVar{Value: int64(9), PrepareParamKind: vector.PrepareParamInteger}
	ses.sesSysVars.Set("lower_case_table_names", int64(1))
	ses.sesSysVars.Set("sql_mode", "")
	value, err := c.ResolveVariable("PAYLOAD", false, false)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0xff, 'A'}, value)
	gotType, err := c.ResolveVariableType("payload", false, false)
	require.NoError(t, err)
	require.Equal(t, wantType, gotType)
	isBin, err := c.ResolveVariableIsBin("payload", false, false)
	require.NoError(t, err)
	require.True(t, isBin)
	domain, err := c.ResolveVariableStringDomain("payload", false, false)
	require.NoError(t, err)
	require.Equal(t, types.RuntimeStringBinary, domain)
	amountType, err := c.ResolveVariableType("amount", false, false)
	require.NoError(t, err)
	require.Equal(t, pb.Type{Id: int32(types.T_decimal64), Width: 8, Scale: 2}, amountType)
	kind, err := c.ResolveVariablePrepareParamKind("amount", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamDecimal, kind)
	require.Zero(t, c.GetLowerCaseTableNames())
	value, err = c.ResolveVariable("SQL_MODE", true, false)
	require.NoError(t, err)
	require.Equal(t, "ANSI_QUOTES", value)
	require.Same(t, ses, source.ses)
}

func TestViewSchemaFrozenGlobalVariablesSurviveCatalogRefresh(t *testing.T) {
	ses := &Session{feSessionImpl: feSessionImpl{
		gSysVars: &SystemVariables{mp: map[string]interface{}{"max_connections": int64(111)}},
	}}
	variables, err := freezeViewSchemaVariables(ses, &ExecCtx{reqCtx: t.Context(), ses: ses})
	require.NoError(t, err)
	c := &viewSchemaCompilerContext{variables: variables}
	generation := ses.gSysVars.getMutationGeneration()
	ses.gSysVars.replaceIfMutationGeneration(generation, map[string]interface{}{"max_connections": int64(222)})
	require.Equal(t, generation, ses.gSysVars.getMutationGeneration(), "刷新不提升本地 SET 代号")
	live, err := ses.GetGlobalSysVar("max_connections")
	require.NoError(t, err)
	require.Equal(t, int64(222), live)
	value, err := c.ResolveVariable("MAX_CONNECTIONS", true, true)
	require.NoError(t, err)
	require.Equal(t, int64(111), value)
}

func TestViewSchemaFrozenVariablesEnforceEnvironmentBoundary(t *testing.T) {
	const name = "payload"
	for _, extra := range []int{0, 1} {
		t.Run(map[int]string{0: "N", 1: "N plus one"}[extra], func(t *testing.T) {
			value := strings.Repeat("x", viewSchemaEnvironmentLimit-len(name)-128+extra)
			ses := &Session{userDefinedVars: map[string]*UserDefinedVar{name: {Value: value}}}
			frozen, err := freezeViewSchemaVariables(ses, &ExecCtx{reqCtx: t.Context(), ses: ses})
			if extra != 0 {
				require.ErrorIs(t, err, plan.ErrViewSchemaLimit)
				require.Nil(t, frozen)
				return
			}
			require.NoError(t, err)
			got, err := frozen.ResolveVariable(name, false, false)
			require.NoError(t, err)
			require.Equal(t, value, got)
		})
	}
}

func TestViewSchemaFrozenVariablesKeepMissingAndDefaultSemantics(t *testing.T) {
	ses := &Session{}
	variables, err := freezeViewSchemaVariables(ses, &ExecCtx{reqCtx: t.Context(), ses: ses})
	require.NoError(t, err)
	c := &viewSchemaCompilerContext{variables: variables}
	value, err := c.ResolveVariable("unassigned", false, false)
	require.NoError(t, err)
	require.Nil(t, value)
	typ, err := c.ResolveVariableType("unassigned", false, false)
	require.NoError(t, err)
	require.Equal(t, inferUserDefinedVarType(nil), typ)
	isBin, err := c.ResolveVariableIsBin("unassigned", false, false)
	require.NoError(t, err)
	require.False(t, isBin)
	domain, err := c.ResolveVariableStringDomain("unassigned", false, false)
	require.NoError(t, err)
	require.Equal(t, types.RuntimeStringInherit, domain)
	kind, err := c.ResolveVariablePrepareParamKind("unassigned", false, false)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, kind)
	value, err = c.ResolveVariable("lower_case_table_names", true, false)
	require.NoError(t, err)
	require.Equal(t, gSysVarsDefs["lower_case_table_names"].Default, value)
	_, err = c.ResolveVariable("not_a_registered_system_variable", true, false)
	require.Error(t, err)
}

func TestViewSchemaFrozenDiagnosticsUseStatementSnapshot(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "live counts captured once", true: "existing statement snapshot"}[snapshot], func(t *testing.T) {
			ses := &Session{errInfo: &errInfo{maxCnt: MoDefaultErrorCount}}
			ses.appendWarningDiagnostic(1265, "warning")
			ses.appendErrorDiagnostic(1366, "error")
			warningCount, errorCount := ses.diagnosticsCounts()
			source := &ExecCtx{reqCtx: t.Context(), ses: ses}
			if snapshot {
				source.diagnosticCountsSnapshotSet = true
				source.diagnosticWarningCountSnapshot = 17
				source.diagnosticErrorCountSnapshot = 3
				warningCount, errorCount = 17, 3
			}
			variables, err := freezeViewSchemaVariables(ses, source)
			require.NoError(t, err)
			c := &viewSchemaCompilerContext{variables: variables}
			ses.appendWarningDiagnostic(1265, "later warning")
			for name, want := range map[string]uint64{warningCountSystemVariable: warningCount, errorCountSystemVariable: errorCount} {
				for _, spelling := range []string{name, strings.ToUpper(name)} {
					got, err := c.ResolveVariable(spelling, true, false)
					require.NoError(t, err)
					require.Equal(t, want, got, spelling)
				}
			}
		})
	}
}

func TestViewSchemaCompilerPrivateStatsAndSQLHelper(t *testing.T) {
	// 不提供引擎/session：任何回退到真正统计读取都会失败，而不是悄悄通过mock。
	c := &viewSchemaCompilerContext{
		TxnCompilerContext: &TxnCompilerContext{execCtx: &ExecCtx{reqCtx: t.Context()}},
		stats:              plan.NewStatsCache(),
	}
	require.Same(t, c.stats, c.GetStatsCache())
	stats, err := c.Stats(&plan.ObjectRef{Obj: 42}, nil)
	require.NoError(t, err)
	require.Nil(t, stats)
	stats, err = c.StatsWithTableDef(&plan.ObjectRef{Obj: 42}, &plan.TableDef{Version: 3}, nil)
	require.NoError(t, err)
	require.Nil(t, stats)
	helper := &viewSchemaSQLHelper{compiler: c}
	require.Same(t, c, helper.GetCompilerContext())
	for _, sql := range []string{"insert into t values (1)", "select nextval('seq')", "begin", "select 1"} {
		rows, err := helper.ExecSql(sql)
		require.Error(t, err, sql)
		require.Nil(t, rows)
		rows, err = helper.ExecSqlWithCtx(t.Context(), sql)
		require.Error(t, err, sql)
		require.Nil(t, rows)
	}
}

type viewSchemaOwnedFixture struct {
	ctx       context.Context
	session   *Session
	parent    *TxnCompilerContext
	proc      *process.Process
	op        client.TxnOperator
	workspace *disttae.Transaction
}

func newViewSchemaOwnedFixture(t *testing.T) *viewSchemaOwnedFixture {
	t.Helper()
	ctx := defines.AttachAccount(t.Context(), 7, 8, 9)
	previousRuntime := moruntime.ServiceRuntime("")
	op, closeClient := client.NewTestTxnOperator(ctx)
	mp := mpool.MustNewZeroNoFixed()
	proc := process.NewTopProcess(ctx, mp, nil, op, nil, nil, nil, nil, nil, nil, nil)
	proc.Base.Lim.Size = 1 << 20
	proc.SetStmtProfile(process.NewStmtProfile(uuid.New(), uuid.New()))
	// 真实事务operator和真实workspace；该fixture只测试生命周期与修订号，
	// 不声称包含native catalog表或持久DDL。
	workspace := (&disttae.Transaction{}).CloneSnapshotWS().(*disttae.Transaction)
	workspace.BindTxnOp(op)
	op.AddWorkspace(workspace)
	ses := &Session{
		feSessionImpl: feSessionImpl{
			tenant:     &TenantInfo{Tenant: "tenant", User: "user", DefaultRole: "role", TenantID: 7, UserID: 8, DefaultRoleID: 9},
			accountId:  7,
			txnHandler: InitTxnHandler("", nil, ctx, op),
		},
		cache:           &privilegeCache{},
		userDefinedVars: make(map[string]*UserDefinedVar),
	}
	parent := InitTxnCompilerContext("binding_db")
	parent.SetExecCtx(&ExecCtx{reqCtx: ctx, ses: ses, proc: proc})
	t.Cleanup(func() {
		parent.Close()
		proc.SetStmtProfile(nil)
		proc.Free()
		// 此fixture没有引擎状态；只结束真实operator，不伪造workspace回滚。
		op.AddWorkspace(nil)
		var rollbackErr error
		if _, err := op.(catalogStampReader).CatalogReadStamp(); err == nil {
			rollbackErr = op.Rollback(context.Background())
		}
		ses.txnHandler.Close()
		closeClient()
		mpool.DeleteMPool(mp)
		moruntime.SetupServiceBasedRuntime("", previousRuntime)
		require.NoError(t, rollbackErr)
	})
	return &viewSchemaOwnedFixture{ctx: ctx, session: ses, parent: parent, proc: proc, op: op, workspace: workspace}
}

func (f *viewSchemaOwnedFixture) open(t *testing.T) *plan.ViewSchemaBinding {
	t.Helper()
	provider := &viewSchemaProvider{parent: f.parent, authorize: func(context.Context, string, string, *plan.Snapshot) error { return nil }}
	binding, err := provider.OpenViewSchemaBinding(f.ctx)
	require.NoError(t, err)
	t.Cleanup(binding.Close)
	return binding
}

func TestViewSchemaProviderBorrowsBudgetAndPreservesParentOnClose(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	f.parent.SetViews([]string{"parent_view"})
	f.parent.SetSnapshot(&pb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 11}})
	f.parent.SetQueryingSubscription(&pb.SubscriptionMeta{SubName: "sub", DbName: "pub", AccountId: 17})
	binding := f.open(t)
	child := binding.Compiler.(*viewSchemaCompilerContext)
	require.NoError(t, binding.Check())
	require.Equal(t, "binding_db", child.DefaultDatabase())
	require.NotSame(t, f.proc.Base, child.GetProcess().Base)
	childBudget, err := child.GetProcess().GetExecutionResourceBudget()
	require.NoError(t, err)
	parentBudget, err := f.proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.Same(t, parentBudget, childBudget)
	require.Same(t, parentBudget, binding.Generation)
	require.Equal(t, f.proc.Base.Lim, child.GetProcess().Base.Lim)
	child.SetViews([]string{"child_view"})
	child.GetSnapshot().TS.PhysicalTime = 12
	child.GetQueryingSubscription().AccountId = 18
	require.Equal(t, []string{"parent_view"}, f.parent.GetViews())
	require.Equal(t, int64(11), f.parent.GetSnapshot().TS.PhysicalTime)
	require.Equal(t, int32(17), f.parent.GetQueryingSubscription().AccountId)
	baseline := parentBudget.Used()
	lease, err := childBudget.ReserveTransientMemory(1024)
	require.NoError(t, err)
	require.Equal(t, baseline+1024, parentBudget.Used())
	require.True(t, lease.Release())
	binding.Close()
	require.False(t, parentBudget.Closed())
	require.Zero(t, parentBudget.Used())
	require.Same(t, f.op, f.session.GetTxnHandler().GetTxn())
	_, err = f.op.(catalogStampReader).CatalogReadStamp()
	require.NoError(t, err, "关闭描述child不得关闭借用的事务")
	lease, err = parentBudget.ReserveTransientMemory(32)
	require.NoError(t, err, "parent预算在描述关闭后仍可使用")
	require.True(t, lease.Release())
}

func TestViewSchemaProviderRejectsOwnerTransitions(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*viewSchemaOwnedFixture)
	}{
		{"workspace same offset new revision", func(f *viewSchemaOwnedFixture) { f.workspace.UpdateSnapshotWriteOffset() }},
		{"transaction snapshot", func(f *viewSchemaOwnedFixture) { f.op.SetSnapshotTS(f.op.SnapshotTS().Next()) }},
		{"statement profile", func(f *viewSchemaOwnedFixture) { f.proc.SetStmtProfile(process.NewStmtProfile(uuid.New(), uuid.New())) }},
		{"active role", func(f *viewSchemaOwnedFixture) { f.session.GetTenantInfo().SetDefaultRoleID(10) }},
		{"role grant invalidation", func(f *viewSchemaOwnedFixture) { f.session.cache.invalidate() }},
		{"session DDL revision", func(f *viewSchemaOwnedFixture) { f.session.advanceDDLVersion() }},
		{"temporary alias revision", func(f *viewSchemaOwnedFixture) {
			f.session.mu.Lock()
			f.session.tempTableVersion++
			f.session.mu.Unlock()
		}},
		{"compiler replaced", func(f *viewSchemaOwnedFixture) {
			f.parent.SetExecCtx(&ExecCtx{reqCtx: f.ctx, ses: f.session, proc: f.proc})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newViewSchemaOwnedFixture(t)
			binding := f.open(t)
			require.NoError(t, binding.Check())
			test.change(f)
			require.ErrorIs(t, binding.Check(), plan.ErrViewSchemaChanged)
		})
	}
}

func TestViewSchemaProviderRejectsAccountDomainDrift(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	binding := f.open(t)
	f.session.SetAccountId(77)
	require.ErrorIs(t, binding.Check(), plan.ErrViewSchemaChanged, "accountId与TenantInfo是两个独立存储，均影响binding身份")
}

func TestViewSchemaProviderChildBudgetResetDoesNotCloseParentGeneration(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	binding := f.open(t)
	child := binding.Compiler.GetProcess()
	child.SetStmtProfile(nil)
	require.False(t, binding.Generation.Closed(), "child丢弃借用关系不能关闭parent资源代")
	require.True(t, f.proc.UsesExecutionResourceGeneration(binding.Generation))
	require.ErrorIs(t, binding.Check(), plan.ErrViewSchemaChanged)
}

func TestViewSchemaProviderChildUsesParentAdmissionBoundary(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	binding := f.open(t)
	budget, err := binding.Compiler.GetProcess().GetExecutionResourceBudget()
	require.NoError(t, err)
	require.Same(t, binding.Generation, budget)
	baseline := budget.Used()
	require.Greater(t, budget.Cap(), baseline)
	lease, err := budget.ReserveTransientMemory(budget.Cap() - baseline)
	require.NoError(t, err)
	defer lease.Release()
	overflow, err := binding.Generation.ReserveTransientMemory(1)
	require.Error(t, err)
	require.Nil(t, overflow)
	require.Equal(t, budget.Cap(), binding.Generation.Used())
	require.True(t, lease.Release())
	require.Equal(t, baseline, budget.Used())
}

func TestViewSchemaProviderRejectsClosedTransaction(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	binding := f.open(t)
	// 这里结束实际txn operator；fixture没有可回滚的引擎数据。
	f.op.AddWorkspace(nil)
	require.NoError(t, f.op.Rollback(context.Background()))
	_, err := f.op.(catalogStampReader).CatalogReadStamp()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), "%v", err)
	err = binding.Check()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), "%v", err)
}

func TestViewSchemaProviderAuthorizationPrecedesCatalogRead(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	denied := errors.New("metadata root is not visible")
	calls := 0
	request := f.parent.NewViewSchemaRequest(f.ctx, func(ctx context.Context, database, name string, snapshot *plan.Snapshot) error {
		calls++
		require.Equal(t, "private_db", database)
		require.Equal(t, "broken_view", name)
		return denied
	})
	defer request.Close()
	// fixture未安装engine。若授权前触发catalog读取，这里会失败或panic。
	for range 2 {
		result, err := request.Describe("private_db", "broken_view", nil)
		require.ErrorIs(t, err, denied)
		require.Nil(t, result)
	}
	require.Equal(t, 2, calls)
}

func TestViewSchemaProviderCancellationAndMissingAuthorization(t *testing.T) {
	f := newViewSchemaOwnedFixture(t)
	provider := &viewSchemaProvider{parent: f.parent}
	binding, err := provider.OpenViewSchemaBinding(f.ctx)
	require.Error(t, err)
	require.Nil(t, binding)
	ctx, cancel := context.WithCancelCause(f.ctx)
	cause := errors.New("cancel metadata request")
	provider.authorize = func(context.Context, string, string, *plan.Snapshot) error { return nil }
	binding, err = provider.OpenViewSchemaBinding(ctx)
	require.NoError(t, err)
	defer binding.Close()
	cancel(cause)
	require.ErrorIs(t, binding.Check(), cause)
}
