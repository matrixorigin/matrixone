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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type indexPrivilegeProbeReportKey struct{}

func TestIndexPrivilegeProbeReportsOnlyRealDenials(t *testing.T) {
	ctx := context.WithValue(context.Background(), indexPrivilegeProbeReportKey{}, true)
	var denials atomic.Int32
	previousReporter := errutil.GetReportErrorFunc()
	errutil.SetErrorReporter(func(reportCtx context.Context, err error, depth int) {
		if reportCtx != nil && reportCtx.Value(indexPrivilegeProbeReportKey{}) == true &&
			strings.Contains(err.Error(), "do not have privilege to execute the statement") {
			denials.Add(1)
		}
		previousReporter(reportCtx, err, depth)
	})
	t.Cleanup(func() { errutil.SetErrorReporter(previousReporter) })

	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"})
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, input: &UserInput{}}
	table := func(name string) *tree.TableName {
		return tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{}, nil)
	}

	t.Run("ordinary statement", func(t *testing.T) {
		denials.Store(0)
		require.NoError(t, checkFrontendIndexTargetWithCatalog(execCtx, &tree.Select{}))
		require.Zero(t, denials.Load())
	})
	t.Run("ordinary clone without transaction", func(t *testing.T) {
		denials.Store(0)
		err := checkCloneDestinationAtAccount(execCtx, ses, table("ordinary"), 0, 0)
		require.ErrorContains(t, err, "missing transaction for clone destination admission")
		require.Zero(t, denials.Load())
	})
	t.Run("protected clone remains denied first", func(t *testing.T) {
		denials.Store(0)
		err := checkCloneDestinationAtAccount(execCtx, ses, table(catalog.IndexTableNamePrefix+"protected"), 0, 0)
		require.ErrorContains(t, err, "do not have privilege")
		require.EqualValues(t, 1, denials.Load())
	})
}

func TestIndexRelationPrivilegePrecedence(t *testing.T) {
	ctx := context.Background()
	newSession := func(tenant *TenantInfo, real, internal bool) *Session {
		ses := &Session{isInternal: internal}
		ses.SetFromRealUser(real)
		ses.SetTenantInfo(tenant)
		return ses
	}
	ordinary := newSession(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"}, true, false)
	privileged := newSession(&TenantInfo{Tenant: "sys", User: "root", DefaultRole: "moadmin"}, true, false)
	internalReal := newSession(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"}, true, true)
	internalSession := newSession(nil, false, true)
	privilegedBackground := newSession(&TenantInfo{Tenant: "sys", User: "root", DefaultRole: "moadmin"}, false, false)
	missingUser := newSession(&TenantInfo{Tenant: "sys", DefaultRole: "moadmin"}, true, false)
	background := &backSession{}
	serverInput := &UserInput{isInternalInput: true}
	sameStatement := &UserInput{isInternalInput: true, sameStatementAuthorization: true}
	withCaller := func(ses *Session) context.Context { return context.WithValue(ctx, hiddenIndexCallerKey{}, ses) }

	cases := []struct {
		name    string
		execCtx *ExecCtx
		allowed bool
	}{
		{"missing execution context", nil, false},
		{"missing session and request context", &ExecCtx{}, false},
		{"missing tenant", &ExecCtx{reqCtx: ctx, ses: newSession(nil, true, false)}, false},
		{"missing user", &ExecCtx{reqCtx: ctx, ses: missingUser}, false},
		{"ordinary foreground", &ExecCtx{reqCtx: ctx, ses: ordinary}, false},
		{"privileged foreground", &ExecCtx{reqCtx: ctx, ses: privileged}, true},
		{"privileged foreground without request context", &ExecCtx{ses: privileged}, true},
		{"real user marked internal", &ExecCtx{reqCtx: ctx, ses: internalReal}, false},
		{"server-labeled foreground user", &ExecCtx{reqCtx: ctx, ses: ordinary, input: serverInput}, false},
		{"non-user internal session", &ExecCtx{reqCtx: ctx, ses: internalSession}, true},
		{"non-user privileged session", &ExecCtx{reqCtx: ctx, ses: privilegedBackground}, true},
		{"server-owned without session", &ExecCtx{reqCtx: ctx, input: serverInput}, true},
		{"server-owned background with caller", &ExecCtx{reqCtx: withCaller(ordinary), ses: background, input: serverInput}, true},
		{"same-statement ordinary caller", &ExecCtx{reqCtx: withCaller(ordinary), ses: background, input: sameStatement}, false},
		{"same-statement privileged caller", &ExecCtx{reqCtx: withCaller(privileged), ses: background, input: sameStatement}, true},
		{"adopted caller precedes internal session", &ExecCtx{reqCtx: withCaller(ordinary), ses: internalSession}, false},
		{"adopted missing identity precedes internal session", &ExecCtx{reqCtx: withCaller(missingUser), ses: internalSession}, false},
	}
	index := catalog.IndexTableNamePrefix + "protected"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkResolvedIndexRelation(tc.execCtx, index, index, "", 0)
			if tc.allowed {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "do not have privilege to execute the statement")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		})
	}
	require.NoError(t, checkResolvedIndexRelation(nil, "ordinary", "ordinary", catalog.SystemOrdinaryRel, 0))
}

func TestIndexRelationAccessCallerAndInternalScope(t *testing.T) {
	ctx := context.Background()
	caller := &Session{}
	caller.SetFromRealUser(true)
	caller.SetTenantInfo(&TenantInfo{Tenant: "tenant", User: "u", DefaultRole: "accountadmin"})
	index := catalog.SecondaryIndexTableNamePrefix + "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
	direct := &ExecCtx{reqCtx: ctx, ses: caller}
	require.Error(t, checkResolvedIndexRelation(direct, index, index, "", 0))
	require.NoError(t, checkResolvedIndexRelation(direct, "base", "base", catalog.SystemOrdinaryRel, 0))
	require.Error(t, checkResolvedIndexRelation(direct, "renamed", "renamed", catalog.SystemOrdinaryRel, features.IndexTable))
	require.Error(t, checkResolvedIndexRelation(direct, "renamed", "renamed", "unregistered_plugin_kind", 0))
	missingUser := &Session{}
	missingUser.SetFromRealUser(true)
	missingUser.SetTenantInfo(&TenantInfo{Tenant: "sys", DefaultRole: "moadmin"})
	require.Error(t, checkResolvedIndexRelation(&ExecCtx{reqCtx: ctx, ses: missingUser}, index, index, "", 0))
	// The internal marker used for a SET expression does not make client SQL
	// trusted, including when the expression binds a subquery.
	direct.input = &UserInput{isInternalInput: true, sameStatementAuthorization: true}
	require.Error(t, checkResolvedIndexRelation(direct, index, index, "", 0))

	bodyCtx := withHiddenIndexCaller(process.WithTrustedIndexRelationAccess(ctx), caller)
	back := &backSession{feSessionImpl: feSessionImpl{upstream: caller}}
	body := &ExecCtx{reqCtx: bodyCtx, ses: back, input: &UserInput{}}
	require.Error(t, checkResolvedIndexRelation(body, index, index, "", 0))
	require.Same(t, caller, hiddenIndexCaller(withHiddenIndexCaller(bodyCtx, back)))

	proc := &process.Process{Ctx: bodyCtx, Base: &process.BaseProcess{}}
	stampHiddenIndexCaller(proc, caller)
	require.Equal(t, "tenant", proc.GetSessionInfo().Account)
	require.Equal(t, "accountadmin", proc.GetSessionInfo().Role)
	require.Error(t, process.CheckIndexRelationAccess(proc, index, "", 0))

	caller.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "root", DefaultRole: "moadmin"})
	require.NoError(t, checkResolvedIndexRelation(body, index, index, "", 0))
	stampHiddenIndexCaller(proc, caller)
	require.NoError(t, process.CheckIndexRelationAccess(proc, index, "", 0))
	caller.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "root", DefaultRole: "reader"})
	require.Error(t, checkResolvedIndexRelation(body, index, index, "", 0))

	// Only an explicitly server-owned input is exempt on a back executor.
	body.input = &UserInput{isInternalInput: true}
	require.NoError(t, checkResolvedIndexRelation(body, index, index, "", 0))
	body.input.sameStatementAuthorization = true
	require.Error(t, checkResolvedIndexRelation(body, index, index, "", 0))
}

func TestCloneIndexAdmissionRestoresSourceScopeOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = defines.AttachAccountId(context.Background(), 8)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	storage := mock_frontend.NewMockEngine(ctrl)
	ses, _ := newObservedProtocolSession()
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"})
	ses.txnHandler = InitTxnHandler("", storage, proc.Ctx, txnOp)
	execCtx := &ExecCtx{reqCtx: proc.Ctx, ses: ses, proc: proc, input: &UserInput{}}
	tcc := &TxnCompilerContext{execCtx: execCtx}
	ses.txnCompileCtx = tcc
	prior := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 12}, Tenant: &plan.SnapshotTenant{TenantID: 8}}
	tcc.SetSnapshot(prior)
	stmt, err := mysql.ParseOne(proc.Ctx, "create table mo_catalog.dst clone mo_catalog.src {MO_TS=0}", 1)
	require.NoError(t, err)
	defer stmt.Free()
	clone := stmt.(*tree.CloneTable)
	clone.IsRestore, clone.IsRestoreByTS, clone.FromAccount = true, true, 0
	err = checkExplicitIndexTargetCatalog(execCtx, explicitIndexTarget{
		database: "mo_catalog", table: "src", at: clone.SrcTable.AtTsExpr, cloneSource: clone,
	})
	require.Error(t, err)
	require.Same(t, prior, tcc.GetSnapshot())
}

func TestCloneDestinationAdmissionResolvedAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := defines.AttachAccountId(context.Background(), 0)
	proc.Ctx = ctx
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	storage := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	storage.EXPECT().Database(gomock.Any(), "mo_catalog", txnOp).Return(db, nil).AnyTimes()
	relation := mock_frontend.NewMockRelation(ctrl)
	def := &plan.TableDef{Name: "shadow", TableType: catalog.SystemOrdinaryRel}
	relation.EXPECT().GetTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef { return def }).AnyTimes()
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).AnyTimes()
	var seen []uint32
	var lookupErr error
	db.EXPECT().Relation(gomock.Any(), gomock.Any(), nil).DoAndReturn(func(c context.Context, name string, _ any) (engine.Relation, error) {
		account, err := defines.GetAccountId(c)
		require.NoError(t, err)
		seen = append(seen, account)
		require.Equal(t, "shadow", name, "foreign target must not use the caller's temporary alias")
		if lookupErr != nil {
			return nil, lookupErr
		}
		return relation, nil
	}).AnyTimes()
	ses, _ := newObservedProtocolSession()
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"})
	ses.tempTables, ses.tempTablesRev = make(map[string]string), make(map[string]string)
	ses.AddTempTable("mo_catalog", "shadow", "__mo_tmp_00112233445566778899aabbccddeeff_mo_catalog_shadow")
	ses.txnHandler = InitTxnHandler("", storage, ctx, txnOp)
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, proc: proc, input: &UserInput{}}
	tcc := &TxnCompilerContext{execCtx: execCtx}
	ses.txnCompileCtx = tcc
	prior := &plan.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: 19}}
	tcc.SetSnapshot(prior)
	priorSub := &plan.SubscriptionMeta{DbName: "mo_catalog", AccountId: 19}
	tcc.SetQueryingSubscription(priorSub)
	table := tree.NewTableName("shadow", tree.ObjectNamePrefix{SchemaName: "mo_catalog", ExplicitSchema: true}, nil)

	require.NoError(t, checkCloneDestinationAtAccount(execCtx, ses, table, 0, 7))
	require.Equal(t, []uint32{7}, seen)
	require.Same(t, ctx, tcc.GetContext())
	require.Same(t, prior, tcc.GetSnapshot())
	require.Same(t, priorSub, tcc.GetQueryingSubscription())

	def = &plan.TableDef{Name: "shadow", TableType: catalog.SystemOrdinaryRel, FeatureFlag: features.IndexTable}
	require.ErrorContains(t, checkCloneDestinationAtAccount(execCtx, ses, table, 0, 7), "privilege")
	require.Same(t, ctx, tcc.GetContext())
	require.Same(t, priorSub, tcc.GetQueryingSubscription())
	// Zero is a valid target tenant ID, not a missing-value sentinel.
	require.ErrorContains(t, checkCloneDestinationAtAccount(execCtx, ses, table, 7, 0), "privilege")
	require.Equal(t, uint32(0), seen[len(seen)-1])
	require.Same(t, ctx, tcc.GetContext())

	lookupErr = errors.New("target catalog failed")
	require.ErrorIs(t, checkCloneDestinationAtAccount(execCtx, ses, table, 0, 7), lookupErr)
	require.Same(t, ctx, tcc.GetContext())
	require.Same(t, prior, tcc.GetSnapshot())
	require.Same(t, priorSub, tcc.GetQueryingSubscription())
}

func TestCloneDestinationDeferredOnlyForExplicitAccount(t *testing.T) {
	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"})
	execCtx := &ExecCtx{reqCtx: context.Background(), ses: ses}
	name := func(table string) tree.TableName {
		return *tree.NewTableName(tree.Identifier(table), tree.ObjectNamePrefix{SchemaName: "db", ExplicitSchema: true}, nil)
	}
	src, dst := name("src"), name("dst")
	account := &tree.ToAccountOpt{AccountName: "target"}
	for _, stmt := range []tree.Statement{
		&tree.CloneTable{SrcTable: src, CreateTable: tree.CreateTable{Table: dst}, ToAccountOpt: account},
		&tree.DataBranchCreateTable{SrcTable: src, CreateTable: tree.CreateTable{Table: dst}, ToAccountOpt: account},
	} {
		var targets []explicitIndexTarget
		err := checkFrontendIndexTargetWithResolver(execCtx, stmt, func(target explicitIndexTarget) error {
			targets = append(targets, target)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, targets, 2)
		require.False(t, targets[0].deferDestinationCatalog)
		require.True(t, targets[1].deferDestinationCatalog)
	}
	reserved := name(catalog.SecondaryIndexTableNamePrefix + "reserved")
	stmt := &tree.CloneTable{SrcTable: src, CreateTable: tree.CreateTable{Table: reserved}, ToAccountOpt: account}
	err := checkFrontendIndexTargetWithResolver(execCtx, stmt, func(explicitIndexTarget) error { return nil })
	require.ErrorContains(t, err, "privilege", "reserved destination names are denied before deferral")
}

func TestIndexRelationAccessFrontendTargets(t *testing.T) {
	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", DefaultRole: "accountadmin"})
	index := tree.Identifier(catalog.SecondaryIndexTableNamePrefix + "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd")
	ctx := context.Background()
	require.Error(t, checkFrontendIndexTarget(ctx, ses, &tree.CreatePitr{Level: tree.PITRLEVELTABLE, TableName: index}))
	require.Error(t, checkFrontendIndexTarget(ctx, ses, &tree.RestoreSnapShot{Level: tree.RESTORELEVELTABLE, TableName: index}))
	require.NoError(t, checkFrontendIndexTarget(ctx, ses, &tree.CreatePitr{Level: tree.PITRLEVELTABLE, TableName: "base"}))
}

// TestIndexRelationDDLPreflight exercises parsed target operands, including
// every member of multi-target DDL and both sides of a rename. The real SQL
// integration test independently proves that the rejection precedes COMMIT.
func TestIndexRelationDDLPreflight(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", DefaultRole: "accountadmin"})
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, input: &UserInput{}, effectiveTxnDefaultDatabase: "db"}
	index := "__mo_index_secondary_0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
	for _, tc := range []struct {
		name, sql string
		deny      bool
	}{
		{"truncate", "truncate table `%s`", true},
		{"drop second", "drop table normal, `%s`", true},
		{"alter", "alter table `%s` add column x int", true},
		{"alter destination", "alter table normal rename to `%s`", true},
		{"rename second source", "rename table normal to other, `%s` to another", true},
		{"rename destination", "rename table normal to `%s`", true},
		{"create reserved", "create table `%s` (x int)", true},
		{"create like source", "create table normal like `%s`", true},
		{"create index target", "create index i on `%s` (x)", true},
		{"drop index target", "drop index i on `%s`", true},
		{"dump target", "dump table `%s` to 'stage://index-test/'", true},
		{"load target", "load table `%s` from 'stage://index-test/'", true},
		{"ordinary truncate", "truncate table normal", false},
		{"ordinary rename", "rename table normal to other", false},
		{"ordinary create", "create table normal (x int)", false},
		{"reserved cte", "with `%s` as (select 1 as x) select x from `%s`", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := tc.sql
			if tc.name == "reserved cte" {
				sql = fmt.Sprintf(sql, index, index)
			} else if tc.deny {
				sql = fmt.Sprintf(sql, index)
			}
			stmt, err := mysql.ParseOne(ctx, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			err = checkFrontendIndexTargetInExec(execCtx, stmt)
			if tc.deny {
				require.ErrorContains(t, err, "privilege")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIndexRelationDDLPreflightPreparedAndOrigin(t *testing.T) {
	ctx := context.Background()
	caller := &Session{}
	caller.SetFromRealUser(true)
	caller.SetTenantInfo(&TenantInfo{Tenant: "sys", DefaultRole: "reader"})
	stmt, err := mysql.ParseOne(ctx, "truncate table `__mo_index_secondary_0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd`", 1)
	require.NoError(t, err)
	defer stmt.Free()
	caller.prepareStmts = map[string]*PrepareStmt{"p": {PrepareStmt: stmt, defaultDatabase: "original_db"}}
	effective, database, err := effectiveStatementForTxn(ctx, caller, &tree.Execute{Name: "p"})
	require.NoError(t, err)
	require.Equal(t, "original_db", database)
	require.True(t, isImplicitCommitStatement(effective))
	require.ErrorContains(t, checkFrontendIndexTargetInExec(&ExecCtx{reqCtx: ctx, ses: caller}, effective), "privilege")

	back := &backSession{feSessionImpl: feSessionImpl{upstream: caller}}
	bodyCtx := withHiddenIndexCaller(ctx, caller)
	body := &ExecCtx{reqCtx: bodyCtx, ses: back, input: &UserInput{}}
	require.ErrorContains(t, checkFrontendIndexTargetInExec(body, effective), "privilege")
	body.input.isInternalInput = true
	require.NoError(t, checkFrontendIndexTargetInExec(body, effective))
	body.input.sameStatementAuthorization = true
	require.ErrorContains(t, checkFrontendIndexTargetInExec(body, effective), "privilege")
}

func TestIndexRelationDDLResolvedTargets(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", DefaultRole: "accountadmin"})
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, input: &UserInput{}}
	table := func(db, name string) *tree.TableName {
		return tree.NewTableName(tree.Identifier(name), tree.ObjectNamePrefix{SchemaName: tree.Identifier(db), ExplicitSchema: db != ""}, nil)
	}

	var seen []explicitIndexTarget
	resolve := func(target explicitIndexTarget) error {
		seen = append(seen, target)
		if target.table == "renamed_index" {
			return checkResolvedIndexRelation(execCtx, target.table, target.table, catalog.SystemIndexRel, 0)
		}
		return nil
	}
	stmt := &tree.DropTable{IfExists: true, Names: tree.TableNames{table("db", "missing"), table("db", "renamed_index")}}
	require.ErrorContains(t, checkFrontendIndexTargetWithResolver(execCtx, stmt, resolve), "privilege")
	require.Len(t, seen, 2)
	require.Equal(t, "db", seen[1].database)
	require.Equal(t, "renamed_index", seen[1].table)

	// A historical source can be an index even after the current name has
	// disappeared or changed kind. Both snapshots must be checked.
	seen = nil
	restore := &tree.RestoreSnapShot{Level: tree.RESTORELEVELTABLE, DatabaseName: "db", TableName: "old_name", SnapShotName: "snap"}
	err := checkFrontendIndexTargetWithResolver(execCtx, restore, func(target explicitIndexTarget) error {
		seen = append(seen, target)
		if target.snapshotName != "" {
			return checkResolvedIndexRelation(execCtx, target.table, target.table, catalog.SystemIndexRel, 0)
		}
		return nil
	})
	require.ErrorContains(t, err, "privilege")
	require.Len(t, seen, 2)
	require.Empty(t, seen[0].snapshotName)
	require.Equal(t, "snap", seen[1].snapshotName)

	sentinel := errors.New("catalog lookup failed")
	err = checkFrontendIndexTargetWithResolver(execCtx, &tree.TruncateTable{Name: table("db", "base")}, func(explicitIndexTarget) error {
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	// The pre-transaction phase does not invent a missing-table error; the
	// same statement is checked again once its transaction exists.
	require.NoError(t, checkExplicitIndexTargetCatalog(execCtx, explicitIndexTarget{database: "db", table: "base"}))
}

func TestIndexRelationDDLResolvedNamespace(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", User: "alice", DefaultRole: "accountadmin"})
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses, effectiveTxnDefaultDatabase: "db2"}
	for _, tc := range []struct {
		name, sql  string
		persistent bool
	}{
		{"rename", "rename table db1.base to destination", true},
		{"alter", "alter table db1.base rename to destination", false},
		{"qualified destination", "alter table db1.base rename to db2.destination", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(ctx, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			var seen []explicitIndexTarget
			err = checkFrontendIndexTargetWithResolver(execCtx, stmt, func(target explicitIndexTarget) error {
				seen = append(seen, target)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, seen, 2)
			require.Equal(t, "db1", seen[0].database)
			require.Equal(t, "db1", seen[1].database)
			require.Equal(t, "destination", seen[1].table)
			require.Equal(t, tc.persistent, seen[0].withoutTemporaryAlias)
			require.Equal(t, tc.persistent, seen[1].withoutTemporaryAlias)
		})
	}

	var publicationTarget explicitIndexTarget
	snapshot := &tree.CreateSnapShot{Object: tree.ObjectInfo{
		SLevel:  tree.SnapshotLevelType{Level: tree.SNAPSHOTLEVELTABLE},
		ObjName: "publisher_db.renamed", AccountName: "publisher", PubName: "pub",
	}}
	err := checkFrontendIndexTargetWithResolver(execCtx, snapshot, func(target explicitIndexTarget) error {
		publicationTarget = target
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "publisher_db", publicationTarget.database)
	require.Equal(t, "renamed", publicationTarget.table)
	require.Equal(t, "publisher", publicationTarget.publisherAccount)
	require.Equal(t, "pub", publicationTarget.publication)
}

func TestIndexRelationTemporaryAndPersistentNamespaces(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", User: "alice", DefaultRole: "accountadmin"})
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses}
	name := func(table string) tree.TableName {
		return *tree.NewTableName(tree.Identifier(table), tree.ObjectNamePrefix{SchemaName: "db", ExplicitSchema: true}, nil)
	}
	shadow, base := name("shadow"), name("base")
	cloneTemp := &tree.CloneTable{SrcTable: base, CreateTable: tree.CreateTable{Temporary: true, Table: shadow}}
	for _, tc := range []struct {
		name string
		stmt tree.Statement
		want []explicitIndexTarget
	}{
		{"create temp", &tree.CreateTable{Temporary: true, Table: shadow}, []explicitIndexTarget{{database: "db", table: "shadow", temporaryOnly: true}}},
		{"clone temp", cloneTemp, []explicitIndexTarget{{database: "db", table: "base", cloneSource: cloneTemp}, {database: "db", table: "shadow", temporaryOnly: true}}},
		{"dump", &tree.DumpTable{Table: &shadow}, []explicitIndexTarget{{database: "db", table: "shadow", withoutTemporaryAlias: true}}},
		{"load", &tree.LoadTable{Table: &shadow}, []explicitIndexTarget{{database: "db", table: "shadow", withoutTemporaryAlias: true}}},
		{"truncate", &tree.TruncateTable{Name: &shadow}, []explicitIndexTarget{{database: "db", table: "shadow"}}},
		{"snapshot", &tree.CreateSnapShot{Object: tree.ObjectInfo{SLevel: tree.SnapshotLevelType{Level: tree.SNAPSHOTLEVELTABLE}, ObjName: "db.shadow"}}, []explicitIndexTarget{{database: "db", table: "shadow", withoutTemporaryAlias: true}}},
		{"pitr", &tree.CreatePitr{Level: tree.PITRLEVELTABLE, DatabaseName: "db", TableName: "shadow"}, []explicitIndexTarget{{database: "db", table: "shadow", withoutTemporaryAlias: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []explicitIndexTarget
			err := checkFrontendIndexTargetWithResolver(execCtx, tc.stmt, func(target explicitIndexTarget) error {
				got = append(got, target)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// Exercise catalog preflight with two real resolver paths: the temp alias is
// ordinary, while its same-name persistent relation is a physical index.
func TestIndexRelationCatalogAdmissionWithTemporaryShadow(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	storage := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	storage.EXPECT().Database(gomock.Any(), "mo_catalog", txnOp).Return(db, nil).AnyTimes()
	ses, _ := newObservedProtocolSession()
	ses.SetFromRealUser(true)
	ses.SetTenantInfo(&TenantInfo{Tenant: "sys", User: "reader", DefaultRole: "public"})
	ses.tempTables, ses.tempTablesRev = make(map[string]string), make(map[string]string)
	ses.txnHandler = InitTxnHandler("", storage, proc.Ctx, txnOp)
	const physical = "__mo_tmp_00112233445566778899aabbccddeeff_mo_catalog_shadow"
	ses.AddTempTable("mo_catalog", "shadow", physical)
	const sourcePhysical = "__mo_tmp_00112233445566778899aabbccddeeff_mo_catalog_source"
	const occupiedPhysical = "__mo_tmp_00112233445566778899aabbccddeeff_mo_catalog_occupied"
	ses.AddTempTable("mo_catalog", "source", sourcePhysical)
	ses.AddTempTable("mo_catalog", "occupied", occupiedPhysical)
	execCtx := &ExecCtx{reqCtx: proc.Ctx, ses: ses, proc: proc, input: &UserInput{}}
	tcc := &TxnCompilerContext{execCtx: execCtx}
	ses.txnCompileCtx = tcc
	for _, tc := range []struct {
		name string
		def  *plan.TableDef
	}{
		{"shadow", &plan.TableDef{Name: "shadow", TableType: catalog.SystemOrdinaryRel, FeatureFlag: features.IndexTable}},
		{physical, &plan.TableDef{Name: physical, TableType: catalog.SystemTemporaryTable}},
		{sourcePhysical, &plan.TableDef{Name: sourcePhysical, TableType: catalog.SystemTemporaryTable}},
		{occupiedPhysical, &plan.TableDef{Name: occupiedPhysical, TableType: catalog.SystemTemporaryTable, FeatureFlag: features.IndexTable}},
		{"missing_definition", nil},
	} {
		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableDef(gomock.Any()).Return(tc.def).AnyTimes()
		if tc.def != nil {
			relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).AnyTimes()
		}
		db.EXPECT().Relation(gomock.Any(), tc.name, nil).Return(relation, nil).AnyTimes()
	}
	target := tree.NewTableName("shadow", tree.ObjectNamePrefix{SchemaName: "mo_catalog", ExplicitSchema: true}, nil)
	require.NoError(t, checkFrontendIndexTargetWithCatalog(execCtx, &tree.TruncateTable{Name: target}))
	require.ErrorContains(t, checkFrontendIndexTargetWithCatalog(execCtx, &tree.CreatePitr{
		Level: tree.PITRLEVELTABLE, DatabaseName: "mo_catalog", TableName: "shadow",
	}), "privilege")
	require.ErrorContains(t, checkFrontendIndexTargetWithCatalog(execCtx, &tree.CreateSnapShot{Object: tree.ObjectInfo{
		SLevel: tree.SnapshotLevelType{Level: tree.SNAPSHOTLEVELTABLE}, ObjName: "mo_catalog.shadow",
	}}), "privilege")
	require.ErrorContains(t, checkFrontendIndexTargetWithCatalog(execCtx, &tree.DumpTable{Table: target}), "privilege")
	missing := tree.NewTableName("missing_definition", tree.ObjectNamePrefix{SchemaName: "mo_catalog", ExplicitSchema: true}, nil)
	require.ErrorContains(t, checkFrontendIndexTargetWithCatalog(execCtx, &tree.TruncateTable{Name: missing}), "table definition is unavailable")
	_, _, err := tcc.Resolve("mo_catalog", "missing_definition", nil, nil)
	require.ErrorContains(t, err, "table definition is unavailable")
	_, absent, err := tcc.ResolveTemporaryTable("mo_catalog", "protected", nil)
	require.NoError(t, err)
	require.Nil(t, absent)
	_, present, err := tcc.ResolveTemporaryTable("mo_catalog", "source", nil)
	require.NoError(t, err)
	require.True(t, present.IsTemporary)
	_, _, err = tcc.ResolveTemporaryTable("mo_catalog", "occupied", nil)
	require.ErrorContains(t, err, "privilege")

	// No lookup of the unrelated persistent destination is needed when the
	// source is temporary and the destination has no temporary alias.
	for _, tc := range []struct {
		sql, want string
	}{
		{"alter table mo_catalog.source rename to protected", ""},
		{"alter table mo_catalog.source rename to occupied", "privilege"},
		{"alter table mo_catalog.source rename to `__mo_index_secondary_reserved`", "privilege"},
	} {
		stmt, parseErr := mysql.ParseOne(proc.Ctx, tc.sql, 1)
		require.NoError(t, parseErr)
		err = checkFrontendIndexTargetWithCatalog(execCtx, stmt)
		stmt.Free()
		if tc.want == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.want)
		}
	}
}
