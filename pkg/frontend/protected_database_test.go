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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func testTableName(dbName, tableName string) tree.TableName {
	return tree.NewUnresolvedObjectName(dbName, tableName).ToTableName()
}

func protectedTargetsFromStatement(stmt tree.Statement) []string {
	return determinePrivilegeSetOfStatement(stmt).writeDatabaseTargets
}

func newProtectedDatabaseTestSessionWithCurrentDB(dbName string) *Session {
	proto := &MysqlProtocolImpl{}
	ses := &Session{feSessionImpl: feSessionImpl{respr: NewMysqlResp(proto)}}
	ses.respr.SetStr(DBNAME, dbName)
	return ses
}

func TestProtectedDatabaseSetFromString(t *testing.T) {
	require.Nil(t, protectedDatabaseSetFromString(nil, ""))
	require.Nil(t, protectedDatabaseSetFromString(nil, " , , "))
	require.Equal(t, map[string]struct{}{"db1": {}, "CamelDB": {}}, protectedDatabaseSetFromString(nil, " db1, CamelDB "))
	require.Equal(t, map[string]struct{}{"db1": {}, "cameldb": {}}, protectedDatabaseSetFromString(&Session{}, " db1, CamelDB "))

	ses := newProtectedDatabaseTestSessionWithCurrentDB("current_db")
	require.Equal(t, "", normalizeProtectedDatabaseName(ses, ""))
	require.Nil(t, protectedDatabaseSetFromString(ses, ","))
	require.Equal(t, map[string]struct{}{"db1": {}}, protectedDatabaseSetFromString(ses, "db1,"))
}

func TestProtectedDatabaseWriteTargetsFromDataBranchKeepsPrivileges(t *testing.T) {
	tableStmt := &tree.DataBranchCreateTable{}
	tableStmt.CreateTable.Table = testTableName("dst_db", "dst_tbl")
	tablePriv := determinePrivilegeSetOfStatement(tableStmt)
	require.Equal(t, []string{"dst_db"}, tablePriv.writeDatabaseTargets)

	databaseStmt := tree.NewDataBranchCreateDatabase()
	databaseStmt.DstDatabase = tree.Identifier("dst_db")
	databasePriv := determinePrivilegeSetOfStatement(databaseStmt)
	require.Equal(t, []string{"dst_db"}, databasePriv.writeDatabaseTargets)
}

func TestProtectedDatabaseWriteTargetsFromClone(t *testing.T) {
	cloneTable := tree.NewCloneTable()
	cloneTable.CreateTable.Table = testTableName("dst_db", "dst_tbl")
	cloneTable.SrcTable = testTableName("src_db", "src_tbl")
	require.Equal(t, []string{"dst_db"}, protectedTargetsFromStatement(cloneTable))

	cloneDatabase := tree.NewCloneDatabase()
	cloneDatabase.DstDatabase = tree.Identifier("dst_db")
	cloneDatabase.SrcDatabase = tree.Identifier("src_db")
	require.Equal(t, []string{"dst_db"}, protectedTargetsFromStatement(cloneDatabase))
}

func TestProtectedDatabaseWriteTargetsFromMultipleObjects(t *testing.T) {
	dropTable := tree.NewDropTable(false, tree.TableNames{
		ptrTo(testTableName("protected_db", "t1")),
		ptrTo(testTableName("normal_db", "t2")),
	})
	require.Equal(t, []string{"protected_db", "normal_db"}, protectedTargetsFromStatement(dropTable))

	renameTable := tree.NewRenameTable([]*tree.AlterTable{
		{
			Table: ptrTo(testTableName("src_db", "t")),
			Options: tree.AlterTableOptions{
				tree.NewAlterOptionTableName(tree.NewUnresolvedObjectName("dst_db", "t2")),
			},
		},
	})
	require.Equal(t, []string{"src_db", "dst_db"}, protectedTargetsFromStatement(renameTable))
}

func TestProtectedDatabaseWriteTargetsFromRestore(t *testing.T) {
	require.Empty(t, protectedTargetsFromStatement(&tree.RestoreSnapShot{
		Level: tree.RESTORELEVELACCOUNT,
	}))
	require.Equal(t, []string{"dst_db"}, protectedTargetsFromStatement(&tree.RestoreSnapShot{
		Level:        tree.RESTORELEVELDATABASE,
		DatabaseName: tree.Identifier("dst_db"),
	}))
}

func TestPrivilegeTipWritesDatabase(t *testing.T) {
	require.False(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeSelect}))
	require.False(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeValues}))
	require.True(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeInsert}))
	require.True(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeUpdate}))
	require.True(t, privilegeTipWritesDatabase(privilegeTips{typ: PrivilegeTypeDelete}))
}

func TestProtectedDatabaseNameFollowsLowerCaseTableNames(t *testing.T) {
	ses := &Session{}
	protectedDatabases := protectedDatabaseSetFromString(ses, "CamelDB")

	require.False(t, checkProtectedDatabaseWriteWithSet(context.Background(), ses, protectedDatabases, "CamelDB"))
	require.False(t, checkProtectedDatabaseWriteWithSet(context.Background(), ses, protectedDatabases, "cameldb"))

	caseSensitiveSes := &Session{
		feSessionImpl: feSessionImpl{
			sesSysVars: &SystemVariables{mp: map[string]interface{}{"lower_case_table_names": int64(0)}},
		},
	}
	caseSensitiveProtectedDatabases := protectedDatabaseSetFromString(caseSensitiveSes, "CamelDB")
	require.False(t, checkProtectedDatabaseWriteWithSet(context.Background(), caseSensitiveSes, caseSensitiveProtectedDatabases, "CamelDB"))
	require.True(t, checkProtectedDatabaseWriteWithSet(context.Background(), caseSensitiveSes, caseSensitiveProtectedDatabases, "cameldb"))

	caseInsensitivePreserveNameSes := &Session{
		feSessionImpl: feSessionImpl{
			sesSysVars: &SystemVariables{mp: map[string]interface{}{"lower_case_table_names": int64(2)}},
		},
	}
	caseInsensitivePreserveNameProtectedDatabases := protectedDatabaseSetFromString(caseInsensitivePreserveNameSes, "CamelDB")
	require.False(t, checkProtectedDatabaseWriteWithSet(context.Background(), caseInsensitivePreserveNameSes, caseInsensitivePreserveNameProtectedDatabases, "CamelDB"))
	require.False(t, checkProtectedDatabaseWriteWithSet(context.Background(), caseInsensitivePreserveNameSes, caseInsensitivePreserveNameProtectedDatabases, "cameldb"))
}

func TestCheckProtectedDatabaseWriteWithSet(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	protectedDatabases := protectedDatabaseSetFromString(ses, "protected_db,CamelDB")

	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, nil, "protected_db"))
	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases))
	require.True(t, checkProtectedDatabaseWriteWithSet(ctx, nil, protectedDatabases, "normal_db"))
	require.False(t, checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, "normal_db", "protected_db"))
	require.False(t, checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, " CamelDB "))
	require.False(t, checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, "cameldb"))
}

func TestCheckProtectedDatabaseWriteWithSetUsesCurrentDatabaseForEmptyTarget(t *testing.T) {
	ctx := context.Background()
	ses := newProtectedDatabaseTestSessionWithCurrentDB("protected_db")
	protectedDatabases := protectedDatabaseSetFromString(ses, "protected_db")

	require.False(t, checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, ""))
}

func TestUnqualifiedProtectedDatabaseWritesUseCurrentDatabase(t *testing.T) {
	ctx := context.Background()
	ses := newProtectedDatabaseTestSessionWithCurrentDB("protected_db")
	protectedDatabases := protectedDatabaseSetFromString(ses, "protected_db")

	testCases := []string{
		"create table t (a int)",
		"drop table t",
		"create table t as select 1",
	}

	for _, sql := range testCases {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			require.False(t, checkProtectedDatabaseWriteWithSet(ctx, ses, protectedDatabases, protectedTargetsFromStatement(stmt)...))
		})
	}
}

func TestCanWriteProtectedDatabase(t *testing.T) {
	require.False(t, canWriteProtectedDatabase(nil))
	require.False(t, canWriteProtectedDatabase(&Session{}))

	accountAdminSession := &Session{}
	accountAdminSession.SetTenantInfo(&TenantInfo{Tenant: "account1", DefaultRole: accountAdminRoleName})
	require.True(t, canWriteProtectedDatabase(accountAdminSession))

	moAdminSession := &Session{}
	moAdminSession.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, DefaultRole: moAdminRoleName})
	require.True(t, canWriteProtectedDatabase(moAdminSession))

	normalSession := &Session{}
	normalSession.SetTenantInfo(&TenantInfo{Tenant: "account1", DefaultRole: "normal_role"})
	require.False(t, canWriteProtectedDatabase(normalSession))
}

func ptrTo[T any](v T) *T {
	return &v
}

func TestReadProtectedPolicyCatalog(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	ses.SetTenantInfo(&TenantInfo{TenantID: 42})
	sql := "select variable_value from mo_catalog.mo_mysql_compatibility_mode where account_id = 42 and system_variables = true and variable_name = 'protected_databases'"
	for _, tc := range []struct {
		name     string
		rows     [][]interface{}
		queryErr error
		want     string
		fail     bool
	}{
		{name: "missing uses empty default"},
		{name: "one policy", rows: [][]interface{}{{"db1,db2"}}, want: "db1,db2"},
		{name: "null fails closed", rows: [][]interface{}{{nil}}, fail: true},
		{name: "duplicate fails closed", rows: [][]interface{}{{"db1"}, {"db2"}}, fail: true},
		{name: "query error", queryErr: errors.New("catalog unavailable"), fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[sql] = newMrsForSqlForShowDatabases(tc.rows)
			bh.sql2err[sql] = tc.queryErr
			got, err := readProtectedPolicy(ctx, ses, bh)
			if tc.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	bh := &backgroundExecTest{}
	bh.init()
	_, err := readProtectedPolicy(canceled, ses, bh)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, bh.executedSQLs)
}

func TestProtectedPolicyCertificateInvalidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", DefaultRole: "r", TenantID: 3})
	reader := &authorizationVersionReader{}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, reader, nil, nil)
	pu.SV.SetDefaultValues()
	setPu("", pu)
	snapshot := timestamp.Timestamp{PhysicalTime: 100}
	proof := protectedPolicyCertificate{account: 3, reusableFrom: snapshot}
	require.True(t, proof.valid(t.Context(), ses, snapshot))
	reader.versions[0].Revision++
	require.False(t, proof.valid(t.Context(), ses, snapshot))
	reader.versions[0].Revision--
	require.False(t, proof.valid(t.Context(), ses, timestamp.Timestamp{PhysicalTime: 99}))
	ses.SetTenantInfo(&TenantInfo{TenantID: 4})
	require.False(t, proof.valid(t.Context(), ses, snapshot))
	ses.SetTenantInfo(&TenantInfo{TenantID: 3})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.False(t, proof.valid(canceled, ses, snapshot))
}

type protectedPolicyTargetEngine struct {
	engine.Engine
	snapshot   timestamp.Timestamp
	generation uint64
	supported  bool
	table      api.TableID
	min        timestamp.Timestamp
	versions   [4]engine.TableContentVersion
}

func (e *protectedPolicyTargetEngine) AcquirePeerTableReadSnapshot(_ context.Context, table api.TableID, min timestamp.Timestamp) (timestamp.Timestamp, uint64, bool, error) {
	e.table, e.min = table, min
	return e.snapshot, e.generation, e.supported, nil
}

func (e *protectedPolicyTargetEngine) ReadTableContentVersions(_ context.Context, _ timestamp.Timestamp, deps []engine.TableContentDependency, versions []engine.TableContentVersion) bool {
	if len(deps) != len(e.versions) || len(versions) != len(e.versions) {
		return false
	}
	copy(versions, e.versions[:])
	return true
}

func TestProtectedPolicyCachedSnapshotRequiresSameReplayGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", DefaultRole: "r", TenantID: 3})
	min := timestamp.Timestamp{PhysicalTime: 10}
	snapshot := timestamp.Timestamp{PhysicalTime: 20}
	versions := [4]engine.TableContentVersion{
		{Replay: 7, Revision: 1},
		{Replay: 7, Revision: 2},
		{Replay: 7, Revision: 3},
		{Replay: 7, Revision: 4},
	}
	eng := &protectedPolicyTargetEngine{snapshot: snapshot, generation: 7, supported: true, versions: versions}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, nil, nil)
	pu.SV.SetDefaultValues()
	setPu("", pu)
	proof := protectedPolicyCertificate{
		account: 3,
		dependency: [4]engine.TableContentDependency{
			{DatabaseID: catalog.MO_CATALOG_ID, TableID: 400},
			{DatabaseID: catalog.MO_CATALOG_ID, TableID: catalog.MO_DATABASE_ID},
			{DatabaseID: catalog.MO_CATALOG_ID, TableID: catalog.MO_TABLES_ID},
			{DatabaseID: catalog.MO_CATALOG_ID, TableID: catalog.MO_COLUMNS_ID},
		},
		version: versions, reusableFrom: min, value: "guarded",
	}

	got, supported, err := protectedPolicyCachedSnapshot(t.Context(), ses, proof)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, snapshot, got)
	require.Equal(t, api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: 400, AccId: 3, DbName: catalog.MO_CATALOG, TbName: "mo_mysql_compatibility_mode"}, eng.table)
	require.Equal(t, ses.getLastCommitTS(), eng.min)

	eng.generation++
	got, supported, err = protectedPolicyCachedSnapshot(t.Context(), ses, proof)
	require.NoError(t, err)
	require.True(t, supported, "a supported but stale generation must use the stronger fallback")
	require.True(t, got.IsEmpty())

	eng.supported = false
	_, supported, err = protectedPolicyCachedSnapshot(t.Context(), ses, proof)
	require.NoError(t, err)
	require.False(t, supported, "an older TN must select the existing ordered barrier")
}

type policyConcurrentEngine struct {
	engine.Engine
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (e *policyConcurrentEngine) AcquirePeerLogtailReadBarrier(ctx context.Context) (timestamp.Timestamp, bool, error) {
	if e.calls.Add(1) == 1 {
		close(e.entered)
	}
	select {
	case <-e.release:
		return timestamp.Timestamp{PhysicalTime: 100}, true, nil
	case <-ctx.Done():
		return timestamp.Timestamp{}, true, ctx.Err()
	}
}

func (*policyConcurrentEngine) ReadTableContentVersions(ctx context.Context, _ timestamp.Timestamp, _ []engine.TableContentDependency, out []engine.TableContentVersion) bool {
	out[0] = engine.TableContentVersion{}
	return ctx.Err() == nil
}

type policyConcurrentTxnClient struct{ client.TxnClient }

func (*policyConcurrentTxnClient) WaitLogTailAppliedAt(ctx context.Context, _ timestamp.Timestamp) (timestamp.Timestamp, error) {
	return timestamp.Timestamp{PhysicalTime: 101}, ctx.Err()
}

func TestProtectedPolicyConcurrentStatementReads(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", DefaultRole: "r", TenantID: 3})
	ses.SetFromRealUser(true)
	eng := &policyConcurrentEngine{entered: make(chan struct{}), release: make(chan struct{})}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, nil, nil)
	pu.SV.SetDefaultValues()
	pu.TxnClient = &policyConcurrentTxnClient{}
	setPu("", pu)
	ctx := t.Context()
	execution := &ExecCtx{reqCtx: ctx, ses: ses}
	ses.GetTxnCompileCtx().SetExecCtx(execution)
	ses.GetPrivilegeCache().protectedPolicy = protectedPolicyCertificate{account: 3, reusableFrom: timestamp.Timestamp{PhysicalTime: 99}, value: "guarded"}
	const n = 24
	var ready, done sync.WaitGroup
	ready.Add(n)
	done.Add(n)
	start := make(chan struct{})
	failures := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			value, err := ses.GetTxnCompileCtx().ResolveVariable(ProtectedDatabases, true, true)
			if err != nil {
				failures <- err
			} else if value != "guarded" {
				failures <- fmt.Errorf("unexpected policy %q", value)
			}
		}()
	}
	ready.Wait()
	close(start)
	<-eng.entered
	close(eng.release)
	done.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	require.EqualValues(t, 1, eng.calls.Load(), "a statement loads the policy once")
}

func TestProtectedPolicyCanceledLoadCanRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetTenantInfo(&TenantInfo{Tenant: "a", User: "u", DefaultRole: "r", TenantID: 3})
	ses.SetFromRealUser(true)
	eng := &policyConcurrentEngine{entered: make(chan struct{}), release: make(chan struct{})}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, nil, nil)
	pu.SV.SetDefaultValues()
	pu.TxnClient = &policyConcurrentTxnClient{}
	setPu("", pu)
	ctx, cancel := context.WithCancel(t.Context())
	execution := &ExecCtx{reqCtx: ctx, ses: ses}
	ses.GetTxnCompileCtx().SetExecCtx(execution)
	ses.GetPrivilegeCache().protectedPolicy = protectedPolicyCertificate{account: 3, reusableFrom: timestamp.Timestamp{PhysicalTime: 99}, value: "guarded"}
	done := make(chan error, 1)
	go func() { _, err := ses.GetTxnCompileCtx().ResolveVariable(ProtectedDatabases, true, true); done <- err }()
	<-eng.entered
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.False(t, execution.protectedPolicyLoaded)
	execution.reqCtx = t.Context()
	close(eng.release)
	value, err := ses.GetTxnCompileCtx().ResolveVariable(ProtectedDatabases, true, true)
	require.NoError(t, err)
	require.Equal(t, "guarded", value)
}
