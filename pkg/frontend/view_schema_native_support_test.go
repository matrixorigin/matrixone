//go:build integration

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
	"fmt"
	"math"
	goruntime "runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// 此构造器只进入 integration 测试产物。集群、事务、workspace 和 process
// 均由调用者真实持有；这里仅补齐普通 Session 的私有 ExecCtx 接线。
func WithNativeViewSchemaCompilerForTest(
	ctx context.Context, service string, storage *disttae.Engine,
	op client.TxnOperator, proc *process.Process, database string,
	tenant *TenantInfo, run func(*TxnCompilerContext) error,
) error {
	unit := getPuIfPresent(service)
	if unit == nil || unit.StorageEngine != storage || proc.GetTxnOperator() != op || proc.GetService() != service {
		return fmt.Errorf("native view schema fixture does not own the CN transaction")
	}
	if tenant == nil {
		tenant = &TenantInfo{Tenant: sysAccountName, User: rootName, DefaultRole: moAdminRoleName,
			TenantID: sysAccountID, UserID: rootID, DefaultRoleID: moAdminRoleID}
	}
	tenant = tenant.Copy()
	ctx = defines.AttachAccount(ctx, tenant.TenantID, tenant.UserID, tenant.DefaultRoleID)
	handler := InitTxnHandler(service, storage, ctx, op)
	defer handler.Close()
	compiler := InitTxnCompilerContext(database)
	defer compiler.Close()
	vars := &SystemVariables{mp: make(map[string]interface{}, len(gSysVarsDefs))}
	for name, definition := range gSysVarsDefs {
		vars.mp[name] = definition.Default
	}
	ses := &Session{
		feSessionImpl: feSessionImpl{
			pool: proc.Mp(), service: service, accountId: tenant.TenantID,
			timeZone: proc.GetSessionInfo().TimeZone, tenant: tenant,
			respr:      &NullResp{username: tenant.User, database: database},
			txnHandler: handler, txnCompileCtx: compiler, sesSysVars: vars, gSysVars: vars.Clone(),
		},
		proc: proc, errInfo: &errInfo{}, cache: &privilegeCache{},
		userDefinedVars: make(map[string]*UserDefinedVar), tempTables: make(map[string]string),
	}
	previousFS := proc.GetFileService()
	proc.SetFileService(unit.FileService)
	defer proc.SetFileService(previousFS)
	compiler.SetExecCtx(&ExecCtx{reqCtx: ctx, ses: ses, proc: proc})
	return run(compiler)
}

// EnableNativeViewSchemaProtocolForTest is scoped to one owned CN and database.
// Close client requests before unregistering; plans never retain the S3 request.
func EnableNativeViewSchemaProtocolForTest(service, database string, afterPrepared ...func(context.Context) error) (roots *atomic.Int64, cleanup func()) {
	roots = new(atomic.Int64)
	route := &nativeViewSchemaRoute{database: database}
	route.build = func(ctx context.Context, compiler *TxnCompilerContext, stmt tree.Statement, prepare bool) (*plan.Plan, error) {
		authorize := NativeViewSchemaRoleAuthorizerForTest(compiler)
		return compiler.BuildPlanWithViewSchema(ctx, stmt, prepare, func(ctx context.Context, db, name string, snapshot *plan.Snapshot) error {
			roots.Add(1)
			return authorize(ctx, db, name, snapshot)
		})
	}
	route.prepared = func(ctx context.Context, compiler *TxnCompilerContext, stmt tree.Statement, bindings []plan.PreparedSourceBinding, values []any) (*plan.PreparedExecutionPlan, error) {
		bound, err := func() (*plan.PreparedExecutionPlan, error) {
			authorize := NativeViewSchemaRoleAuthorizerForTest(compiler)
			request := compiler.NewViewSchemaRequest(ctx, func(ctx context.Context, db, name string, snapshot *plan.Snapshot) error {
				roots.Add(1)
				return authorize(ctx, db, name, snapshot)
			})
			defer request.Close()
			return request.BuildPreparedForIntegrationTest(stmt, bindings, values, compiler.GetProcess())
		}()
		if err != nil {
			return nil, err
		}
		for _, checkpoint := range afterPrepared {
			if err := checkpoint(ctx); err != nil {
				return nil, err
			}
		}
		return bound, nil
	}
	if _, loaded := nativeViewSchemaRoutes.LoadOrStore(service, route); loaded {
		panic("duplicate native View protocol route")
	}
	return roots, func() { nativeViewSchemaRoutes.CompareAndDelete(service, route) }
}

// 使用 production 的 derived shared-transaction catalog SELECT，验证其不会
// 推进外层 statement/catalog 代号，且 Close 不回滚调用者事务。
func ReadNativeViewSchemaCatalogForTest(ctx context.Context, compiler *TxnCompilerContext) error {
	ctx = withViewSchemaCatalogRead(ctx)
	bh := compiler.getOrCreateBackExec(ctx)
	bh.ClearExecResultSet()
	return bh.Exec(ctx, "select relname from mo_catalog.mo_tables where relname = 'mo_tables'")
}

// 通过真实 frontend back-exec 执行 SHOW/DESC。普通 SQLExecutor 没有前端
// subscription resolver，不能用它替代订阅 SHOW 的消费者验证。此只读子语句
// 复用 fixture 的事务边界，返回数据前关闭自己的结果/session，不关闭父事务。
func ReadNativeViewMetadataSQLForTest(ctx context.Context, compiler *TxnCompilerContext, sql string) ([][]string, error) {
	ctx = withViewSchemaCatalogRead(ctx)
	bh := NewShareTxnBackgroundExec(ctx, compiler.GetSession(), false)
	defer bh.Close()
	if err := bh.Exec(ctx, sql); err != nil {
		return nil, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	var rows [][]string
	for _, result := range results {
		for row := uint64(0); row < result.GetRowCount(); row++ {
			values := make([]string, result.GetColumnCount())
			for col := range values {
				null, err := result.ColumnIsNull(ctx, row, uint64(col))
				if err != nil {
					return nil, err
				}
				if null {
					values[col] = "<NULL>"
					continue
				}
				values[col], err = result.GetString(ctx, row, uint64(col))
				if err != nil {
					return nil, err
				}
			}
			rows = append(rows, values)
		}
	}
	return rows, nil
}

// 对显式派生计划执行真正的 frontend 执行授权。schema 根访问资格不代替
// DEFINER/INVOKER 的完整执行权限链；此入口只进入 integration 测试产物。
func AuthorizeNativeViewSchemaPlanForTest(ctx context.Context, compiler *TxnCompilerContext, statement tree.Statement, p *plan.Plan) error {
	_, err := authenticateCanExecuteStatementAndPlan(ctx, compiler.GetSession().(*Session), statement, p)
	return err
}

// Invoke the actual compile-retry callback with the fixture-owned session.
// The caller controls the catalog transition between initial admission and retry.
func RebuildNativeSelectLockCTASForTest(ctx context.Context, compiler *TxnCompilerContext, statement tree.Statement, prepared bool, bindingDatabase string) (*plan.Plan, error) {
	ses := compiler.GetSession().(*Session)
	var retry *preparedExecutionRetry
	if prepared {
		ses.SetSql("execute select_lock_ctas")
		retry = &preparedExecutionRetry{}
	}
	return preparedExecutionBuildPlanFunc(ses, statement, false, retry, bindingDatabase, prepared, nil)(ctx)
}

// 测试角色只具有一个明确的 active role，直接复用既有 view privilege 查询。
// 此 helper 不把它推广为生产 SHOW/查询权限策略。
func NativeViewSchemaRoleAuthorizerForTest(compiler *TxnCompilerContext) ViewSchemaAuthorizer {
	return func(ctx context.Context, database, name string, _ *plan.Snapshot) error {
		ses := compiler.GetSession().(*Session)
		allowed, err := verifyViewPrivilegeForRole(ctx, compiler.getOrCreateBackExec(ctx), ses,
			nil, int64(ses.GetTenantInfo().GetDefaultRoleID()), PrivilegeTypeSelect, database, name, false, false)
		if err != nil {
			return err
		}
		if !allowed {
			return moerr.NewInternalError(ctx, "view schema access denied")
		}
		return nil
	}
}

// Exercise the real batch admission and transaction owner using the fixture's
// engine. Warm membership in this epoch, then count targets consumed together.
func AuthorizeNativeSelectLockBatchForTest(ctx context.Context, compiler *TxnCompilerContext, q *plan.Query) (int, bool, error) {
	ses := compiler.GetSession().(*Session)
	ctx, epoch := beginAuthorizationEpoch(ctx, ses, &ExecCtx{})
	defer epoch.end()
	tips, err := selectLockPrivilegeTips(q)
	if err != nil {
		return 0, false, err
	}
	batch := &selectLockAuthorizationBatch{}
	for _, tip := range tips {
		batch.items = append(batch.items, privilegeItem{privilegeTyp: PrivilegeTypeSelect, selectLock: true,
			objType: objectTypeTable, dbName: tip.databaseName, tableName: tip.tableName,
			objectRef: tip.objectRef, viewPath: tip.viewPath,
			isClusterTable: tip.isClusterTable, clusterTableOperation: clusterTableSelect})
	}
	priv := &privilege{objType: objectTypeTable, entries: []privilegeEntry{{privilegeEntryTyp: privilegeEntryTypeCompound,
		compound: &compoundEntry{items: batch.items[:1]}}}}
	if ok, _, err := determineUserHasPrivilegeSet(ctx, ses, priv); err != nil || !ok {
		return 0, ok, err
	}
	ok, _, err := determineUserHasPrivilegeSetWithSelectLockBatch(ctx, ses, priv, batch)
	return batch.checked, ok, err
}

// Pause the real second-target boundary, then change one reuse precondition.
// The existing frontend fault point supplies the barrier; production has no
// test hook. The count distinguishes legacy resumption from skipping a target.
func AuthorizeNativeSelectLockBoundaryForTest(ctx context.Context, compiler *TxnCompilerContext, q *plan.Query, change string) (int64, bool, error) {
	ses := compiler.GetSession().(*Session)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runCtx, epoch := beginAuthorizationEpoch(runCtx, ses, &ExecCtx{})
	defer epoch.end()
	if ok, _, err := authenticateSelectLockReferences(runCtx, ses, q); err != nil || !ok {
		return 0, ok, err
	}
	if fault.EnableDomain(fault.DomainFrontend) {
		defer fault.DisableDomain(fault.DomainFrontend)
	}
	const target = "authorization_before_grant_read"
	for _, point := range []struct{ name, frequency, action string }{
		{target, "2:2::", "wait"},
		{"select_lock_batch_waiters", ":::", "getwaiters"},
		{"select_lock_batch_notify", ":::", "notifyall"},
		{"select_lock_batch_count", ":::", "getcount"},
	} {
		if err := fault.AddFaultPointInDomain(ctx, fault.DomainFrontend, point.name, point.frequency, point.action, 0, target, false); err != nil {
			return 0, false, err
		}
		defer fault.RemoveFaultPointFromDomain(ctx, fault.DomainFrontend, point.name)
	}
	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		ok, _, err := authenticateSelectLockReferences(runCtx, ses, q)
		done <- result{ok, err}
	}()
	// Removing the barrier and joining the worker also runs on a failed setup
	// or cancelled parent, before the session/epoch fixture can be released.
	joined := false
	defer func() {
		cancel()
		fault.RemoveFaultPointFromDomain(ctx, fault.DomainFrontend, target)
		if !joined {
			<-done
		}
	}()
	for {
		n, _, _ := fault.TriggerFaultInDomain(fault.DomainFrontend, "select_lock_batch_waiters")
		if n == 1 {
			break
		}
		select {
		case r := <-done:
			joined = true
			return 0, r.ok, moerr.NewInternalErrorf(ctx, "batch did not reach boundary: %v", r.err)
		case <-ctx.Done():
			return 0, false, ctx.Err()
		default:
			goruntime.Gosched()
		}
	}
	oldRole := ses.GetTenantInfo().GetDefaultRoleID()
	defer ses.GetTenantInfo().SetDefaultRoleID(oldRole)
	switch change {
	case "generation":
		ses.GetPrivilegeCache().invalidate()
	case "principal":
		ses.GetTenantInfo().SetDefaultRoleID(oldRole + 1)
	case "minimum":
		ses.updateLastCommitTS(timestamp.Timestamp{PhysicalTime: math.MaxInt64})
	case "cancel":
		cancel()
	}
	fault.TriggerFaultInDomain(fault.DomainFrontend, "select_lock_batch_notify")
	r := <-done
	joined = true
	count, _, _ := fault.TriggerFaultInDomain(fault.DomainFrontend, "select_lock_batch_count")
	return count, r.ok, r.err
}

type nativeSelectLockFailureKey struct{}

// Install before starting the owned cluster; restore only after cluster cleanup.
// Requests without the private test context always use the original factory.
func InstallNativeSelectLockFailureExecForTest() func() {
	original := NewBackgroundExec
	NewBackgroundExec = func(ctx context.Context, upstream FeSession, options ...*BackgroundExecOption) BackgroundExec {
		bh := original(ctx, upstream, options...)
		if injected, _ := ctx.Value(nativeSelectLockFailureKey{}).(*nativeSelectLockFailureExec); injected != nil {
			injected.BackgroundExec = bh
			injected.created++
			return injected
		}
		return bh
	}
	return func() { NewBackgroundExec = original }
}

type nativeSelectLockFailureExec struct {
	BackgroundExec
	session                                   *Session
	epoch                                     *authorizationEpoch
	scenario, first, second                   string
	result                                    ExecResult
	commands                                  map[string]int
	created, closed, metadata                 int
	beginErr, workErr, commitErr, rollbackErr error
}

func (f *nativeSelectLockFailureExec) Exec(ctx context.Context, sql string) error {
	f.commands[sql]++
	if f.scenario == "record" {
		return f.BackgroundExec.Exec(ctx, sql)
	}
	switch sql {
	case "begin;":
		if err := f.BackgroundExec.Exec(ctx, sql); err != nil {
			return err
		}
		if f.scenario == "begin failure" {
			f.session.GetPrivilegeCache().invalidate()
			return f.beginErr // includes a partially created transaction
		}
		return nil
	case "commit;":
		f.session.GetPrivilegeCache().invalidate()
		return f.commitErr
	case "rollback;":
		if err := f.BackgroundExec.Exec(ctx, sql); err != nil {
			return err
		}
		if f.scenario == "target error and rollback failure" || f.scenario == "boundary error and rollback failure" {
			return f.rollbackErr
		}
		return nil
	}
	f.result = &MysqlResultSet{}
	if strings.HasPrefix(sql, "select viewdef, owner") {
		f.metadata++
		mrs := &MysqlResultSet{}
		mrs.AddColumn(&MysqlColumn{})
		mrs.AddColumn(&MysqlColumn{})
		mrs.AddRow([]interface{}{`{"security_type":"INVOKER"}`, int64(f.session.GetTenantInfo().GetDefaultRoleID())})
		f.result = mrs
	} else if strings.Contains(sql, "mo_role_privs") {
		first := strings.Contains(sql, "'"+f.first+"'")
		second := strings.Contains(sql, "'"+f.second+"'")
		if second && f.scenario == "target error and rollback failure" {
			f.session.GetPrivilegeCache().invalidate()
			return f.workErr
		}
		if second && f.scenario == "denial and commit failure" {
			return nil
		}
		if first || second || strings.Contains(sql, "'v'") {
			f.result = newMrsForWithGrantOptionPrivilege([][]interface{}{{PrivilegeTypeUpdate, true}})
			if first && strings.HasPrefix(f.scenario, "boundary error") {
				f.session.GetPrivilegeCache().invalidate()
				f.epoch.mu.Lock()
				f.epoch.err = f.workErr
				f.epoch.mu.Unlock()
			} else if first && f.scenario == "prefix success and commit failure" {
				f.session.GetPrivilegeCache().invalidate()
			}
		}
	}
	return nil
}

func (f *nativeSelectLockFailureExec) ClearExecResultSet() {
	if f.scenario == "record" {
		f.BackgroundExec.ClearExecResultSet()
	}
}
func (f *nativeSelectLockFailureExec) GetExecResultSet() []interface{} {
	if f.scenario == "record" {
		return f.BackgroundExec.GetExecResultSet()
	}
	return []interface{}{f.result}
}
func (f *nativeSelectLockFailureExec) Close() {
	f.closed++
	f.BackgroundExec.Close()
}

func AssertNativeSelectLockFinalizationForTest(t *testing.T, ctx context.Context, compiler *TxnCompilerContext, q *plan.Query, scenario string) {
	t.Helper()
	ses := compiler.GetSession().(*Session)
	ctx, epoch := beginAuthorizationEpoch(ctx, ses, &ExecCtx{})
	defer epoch.end()
	ok, _, err := authenticateSelectLockReferences(ctx, ses, q)
	require.NoError(t, err)
	require.True(t, ok)
	valid, cached := ses.GetPrivilegeCache().getActiveRoleGrant(ses.GetTenantInfo().GetUserID(), ses.GetTenantInfo().GetDefaultRoleID())
	require.True(t, valid && cached)
	f := &nativeSelectLockFailureExec{session: ses, epoch: epoch, scenario: scenario,
		first: q.SelectLockReferences[0].ObjectRef.ObjName, second: q.SelectLockReferences[1].ObjectRef.ObjName,
		commands: map[string]int{}, beginErr: moerr.NewTxnNeedRetryNoCtx(), workErr: moerr.NewTxnNeedRetryNoCtx(),
		commitErr: moerr.NewTxnNeedRetryNoCtx(), rollbackErr: moerr.NewInternalErrorNoCtx("injected rollback failure")}
	ok, _, err = authenticateSelectLockReferences(context.WithValue(ctx, nativeSelectLockFailureKey{}, f), ses, q)
	require.Error(t, err)
	if scenario != "prefix success and commit failure" {
		require.False(t, ok)
	}
	require.Equal(t, 1, f.created)
	require.Equal(t, 1, f.closed)
	require.Equal(t, 1, f.commands["begin;"])
	require.Equal(t, 1, f.commands["rollback;"])
	require.Nil(t, f.BackgroundExec.(*backExec).backSes, "real background session must be closed")
	require.Nil(t, ses.GetPrivilegeCache().activeRoleGrant.Load(), "failure must not publish membership into the invalidated generation")
	switch scenario {
	case "begin failure":
		require.ErrorIs(t, err, f.beginErr)
		require.Zero(t, f.commands["commit;"])
	case "target error and rollback failure":
		require.ErrorIs(t, err, f.workErr)
		require.ErrorIs(t, err, f.rollbackErr)
		require.Zero(t, f.commands["commit;"])
		require.Equal(t, 2, f.metadata, "must reach the second target in the batch")
	case "boundary error", "boundary error and rollback failure":
		require.ErrorIs(t, err, f.workErr)
		require.NotErrorIs(t, err, f.commitErr, "boundary errors must roll back without attempting commit")
		if scenario == "boundary error and rollback failure" {
			require.ErrorIs(t, err, f.rollbackErr)
		}
		require.Zero(t, f.commands["commit;"])
		require.Equal(t, 1, f.metadata, "must stop before the second target or fallback")
	default:
		require.ErrorIs(t, err, f.commitErr)
		require.Equal(t, 1, f.commands["commit;"])
		if scenario == "prefix success and commit failure" {
			require.Equal(t, 1, f.metadata, "failed prefix commit must prevent fallback")
		} else {
			require.Equal(t, 2, f.metadata)
		}
	}
}

func AssertNativeSelectLockMixedFallbackForTest(t *testing.T, ctx context.Context, compiler *TxnCompilerContext, q *plan.Query) {
	t.Helper()
	ses := compiler.GetSession().(*Session)
	ctx, epoch := beginAuthorizationEpoch(ctx, ses, &ExecCtx{})
	defer epoch.end()
	ok, _, err := authenticateSelectLockReferences(ctx, ses, q)
	require.NoError(t, err)
	require.True(t, ok)
	f := &nativeSelectLockFailureExec{scenario: "record", commands: map[string]int{}}
	ok, _, err = authenticateSelectLockReferences(context.WithValue(ctx, nativeSelectLockFailureKey{}, f), ses, q)
	require.NoError(t, err)
	require.True(t, ok)
	require.GreaterOrEqual(t, f.created, 2, "the two view targets must use independent transactions in a mixed query")
	require.Equal(t, f.created, f.closed)
	require.Equal(t, f.created, f.commands["begin;"])
	require.Equal(t, f.created, f.commands["commit;"])
	require.Zero(t, f.commands["rollback;"])
}
