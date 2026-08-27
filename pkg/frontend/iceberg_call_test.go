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
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
)

var _ IcebergMaintenanceCallExecutor = IcebergMaintenanceProcedureExecutor{}

func TestParseIcebergBuiltinCallForFrontendSelfHandle(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_rewrite_data_files('ksa_gold.sales.orders', 'ref=main,target_file_size=268435456')", 1)
	if err != nil {
		t.Fatalf("parse Iceberg CALL: %v", err)
	}
	call, ok := stmt.(*tree.CallStmt)
	if !ok {
		t.Fatalf("expected CallStmt, got %T", stmt)
	}
	parsed, ok, err := parseIcebergBuiltinCall(context.Background(), call)
	if err != nil {
		t.Fatalf("parse frontend Iceberg CALL: %v", err)
	}
	if !ok {
		t.Fatalf("expected Iceberg builtin call")
	}
	if parsed.Parsed.Operation != maintenance.OperationRewriteDataFiles ||
		parsed.Parsed.TargetID.Catalog != "ksa_gold" ||
		parsed.Parsed.TargetID.Namespace != "sales" ||
		parsed.Parsed.TargetID.Table != "orders" ||
		parsed.Parsed.Options["ref"] != "main" {
		t.Fatalf("unexpected parsed Iceberg CALL: %+v", parsed)
	}
}

func TestParseIcebergRegisterAccessCallForFrontendSelfHandle(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_register_access('tiera', 'account_id=1,external_principal=local,endpoint=localhost,region=us-east-1,bucket=mo-iceberg,policy_state=disabled')", 1)
	if err != nil {
		t.Fatalf("parse Iceberg access CALL: %v", err)
	}
	call := stmt.(*tree.CallStmt)
	parsed, ok, err := parseIcebergBuiltinCall(context.Background(), call)
	if err != nil {
		t.Fatalf("parse frontend Iceberg access CALL: %v", err)
	}
	if !ok {
		t.Fatalf("expected Iceberg builtin call")
	}
	if parsed.Name != icebergRegisterAccessProcedure || parsed.Target != "tiera" ||
		parsed.Parsed.Options["account_id"] != "1" ||
		parsed.Parsed.Options["external_principal"] != "local" ||
		parsed.Parsed.Options["policy_state"] != "disabled" {
		t.Fatalf("unexpected parsed Iceberg access CALL: %+v", parsed)
	}
}

func TestParseIcebergUnregisterAccessCallForFrontendSelfHandle(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_unregister_access('tiera')", 1)
	if err != nil {
		t.Fatalf("parse Iceberg access unregister CALL: %v", err)
	}
	call := stmt.(*tree.CallStmt)
	parsed, ok, err := parseIcebergBuiltinCall(context.Background(), call)
	if err != nil {
		t.Fatalf("parse frontend Iceberg access unregister CALL: %v", err)
	}
	if !ok {
		t.Fatalf("expected Iceberg builtin call")
	}
	if parsed.Name != icebergUnregisterAccessProcedure || parsed.Target != "tiera" || parsed.Options != "" {
		t.Fatalf("unexpected parsed Iceberg access unregister CALL: %+v", parsed)
	}
}

func TestExecuteIcebergRegisterAccessCallCommitsMetadataAtomically(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()
	catalogSQL := icebergsql.GetCatalogByNameSQL(0, "tiera") + " for update"
	bh.sql2result[catalogSQL] = icebergCallResult([]interface{}{uint32(0), uint64(7), "tiera", "rest", "https://catalog.example/rest"})
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()

	ses := &Session{}
	ses.SetAccountId(0)
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, User: rootName, DefaultRole: moAdminRoleName})
	results, err := executeIcebergRegisterAccessCall(ctx, ses, IcebergBuiltinProcedureCall{
		Name:   icebergRegisterAccessProcedure,
		Target: "tiera",
		Options: "scope=cluster,account_id=0,external_principal=ci-local,catalog_uri=https://catalog.example/rest," +
			"endpoint=localhost,region=us-east-1,bucket=mo-iceberg",
	})
	if err != nil {
		t.Fatalf("register Iceberg access: %v", err)
	}
	if len(results) != 1 || results[0].GetRowCount() != 1 {
		t.Fatalf("unexpected register result: %+v", results)
	}
	if len(bh.executedSQLs) != 5 || bh.executedSQLs[0] != "begin;" || bh.executedSQLs[1] != catalogSQL || bh.executedSQLs[4] != "commit;" {
		t.Fatalf("unexpected register transaction SQL:\n%s", strings.Join(bh.executedSQLs, "\n"))
	}
	if !strings.Contains(bh.executedSQLs[2], "insert into mo_catalog.mo_iceberg_principal_map") ||
		!strings.Contains(bh.executedSQLs[3], "insert into mo_catalog.mo_iceberg_residency_policy") {
		t.Fatalf("registration did not write both metadata rows:\n%s", strings.Join(bh.executedSQLs, "\n"))
	}
}

func TestExecuteIcebergUnregisterAccessCallCommitsAtomicCleanup(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()
	catalogSQL := icebergsql.GetCatalogByNameSQL(0, "tiera") + " for update"
	countSQL := "select count(*) from mo_catalog.mo_iceberg_residency_policy where (scope_type = 'cluster' or account_id = 0) and catalog_id = 7"
	bh.sql2result[catalogSQL] = icebergCallResult([]interface{}{uint32(0), uint64(7), "tiera", "rest", "https://catalog.example/rest"})
	bh.sql2result[countSQL] = icebergCallResult([]interface{}{uint64(0)})
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()

	ses := &Session{}
	ses.SetAccountId(0)
	ses.SetTenantInfo(&TenantInfo{
		Tenant:      sysAccountName,
		User:        rootName,
		DefaultRole: moAdminRoleName,
	})
	results, err := executeIcebergUnregisterAccessCall(ctx, ses, IcebergBuiltinProcedureCall{
		Name:   icebergUnregisterAccessProcedure,
		Target: "tiera",
	})
	if err != nil {
		t.Fatalf("unregister Iceberg access: %v", err)
	}
	if len(results) != 1 || results[0].GetRowCount() != 1 {
		t.Fatalf("unexpected unregister result: %+v", results)
	}
	wantSQL := []string{
		"begin;",
		catalogSQL,
		"delete from mo_catalog.mo_iceberg_residency_policy where catalog_id = 7 and (scope_type = 'cluster' or (scope_type = 'account' and account_id = 0))",
		countSQL,
		"delete from mo_catalog.mo_iceberg_principal_map where account_id = 0 and catalog_id = 7",
		"commit;",
	}
	if strings.Join(bh.executedSQLs, "\n") != strings.Join(wantSQL, "\n") {
		t.Fatalf("unexpected unregister SQL:\n%s", strings.Join(bh.executedSQLs, "\n"))
	}
}

func TestExecuteIcebergUnregisterAccessCallRollsBackOnCleanupFailure(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()
	catalogSQL := icebergsql.GetCatalogByNameSQL(0, "tiera") + " for update"
	deleteSQL := "delete from mo_catalog.mo_iceberg_residency_policy where catalog_id = 7 and (scope_type = 'cluster' or (scope_type = 'account' and account_id = 0))"
	bh.sql2result[catalogSQL] = icebergCallResult([]interface{}{uint32(0), uint64(7), "tiera", "rest", "https://catalog.example/rest"})
	bh.sql2err[deleteSQL] = errors.New("delete failed")
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()

	ses := &Session{}
	ses.SetAccountId(0)
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, User: rootName, DefaultRole: moAdminRoleName})
	_, err := executeIcebergUnregisterAccessCall(ctx, ses, IcebergBuiltinProcedureCall{
		Name:   icebergUnregisterAccessProcedure,
		Target: "tiera",
	})
	if err == nil || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected cleanup failure, got %v", err)
	}
	wantSQL := []string{"begin;", catalogSQL, deleteSQL, "rollback;"}
	if strings.Join(bh.executedSQLs, "\n") != strings.Join(wantSQL, "\n") {
		t.Fatalf("unexpected rollback SQL:\n%s", strings.Join(bh.executedSQLs, "\n"))
	}
}

func TestExecuteIcebergUnregisterAccessCallDoesNotReturnCommittedResultWhenCommitFails(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()
	catalogSQL := icebergsql.GetCatalogByNameSQL(0, "tiera") + " for update"
	countSQL := "select count(*) from mo_catalog.mo_iceberg_residency_policy where (scope_type = 'cluster' or account_id = 0) and catalog_id = 7"
	bh.sql2result[catalogSQL] = icebergCallResult([]interface{}{uint32(0), uint64(7), "tiera", "rest", "https://catalog.example/rest"})
	bh.sql2result[countSQL] = icebergCallResult([]interface{}{uint64(0)})
	bh.sql2err["commit;"] = errors.New("commit failed")
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()

	ses := &Session{}
	ses.SetAccountId(0)
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, User: rootName, DefaultRole: moAdminRoleName})
	results, err := executeIcebergUnregisterAccessCall(ctx, ses, IcebergBuiltinProcedureCall{
		Name:   icebergUnregisterAccessProcedure,
		Target: "tiera",
	})
	if err == nil || !strings.Contains(err.Error(), "commit failed") {
		t.Fatalf("expected commit failure, got %v", err)
	}
	if results != nil {
		t.Fatalf("commit failure returned a committed result: %+v", results)
	}
	if got := strings.Join(bh.executedSQLs, "\n"); !strings.HasSuffix(got, "commit;\nrollback;") {
		t.Fatalf("commit failure did not roll back:\n%s", got)
	}
}

func TestExecuteIcebergUnregisterAccessCallKeepsPrincipalForRemainingScope(t *testing.T) {
	ctx := context.Background()
	bh := &backgroundExecTest{}
	bh.init()
	catalogSQL := icebergsql.GetCatalogByNameSQL(9, "tiera") + " for update"
	countSQL := "select count(*) from mo_catalog.mo_iceberg_residency_policy where (scope_type = 'cluster' or account_id = 9) and catalog_id = 7"
	bh.sql2result[catalogSQL] = icebergCallResult([]interface{}{uint32(9), uint64(7), "tiera", "rest", "https://catalog.example/rest"})
	bh.sql2result[countSQL] = icebergCallResult([]interface{}{uint64(1)})
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()

	ses := &Session{}
	ses.SetAccountId(9)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant1", User: "admin", DefaultRole: accountAdminRoleName})
	_, err := executeIcebergUnregisterAccessCall(ctx, ses, IcebergBuiltinProcedureCall{
		Name:   icebergUnregisterAccessProcedure,
		Target: "tiera",
	})
	if err != nil {
		t.Fatalf("unregister account-scoped Iceberg access: %v", err)
	}
	wantSQL := []string{
		"begin;",
		catalogSQL,
		"delete from mo_catalog.mo_iceberg_residency_policy where catalog_id = 7 and scope_type = 'account' and account_id = 9",
		countSQL,
		"commit;",
	}
	if strings.Join(bh.executedSQLs, "\n") != strings.Join(wantSQL, "\n") {
		t.Fatalf("principal should remain while another access scope exists:\n%s", strings.Join(bh.executedSQLs, "\n"))
	}
}

func TestIcebergUnregisterAccessScopeAuthorization(t *testing.T) {
	ctx := context.Background()
	sys := &TenantInfo{Tenant: sysAccountName, DefaultRole: moAdminRoleName}
	account := &TenantInfo{Tenant: "tenant1", DefaultRole: accountAdminRoleName}

	if scope, err := icebergUnregisterAccessScope(ctx, sys, nil); err != nil || scope != icebergAccessScopeAll {
		t.Fatalf("moadmin default scope: scope=%q err=%v", scope, err)
	}
	if scope, err := icebergUnregisterAccessScope(ctx, account, nil); err != nil || scope != "account" {
		t.Fatalf("accountadmin default scope: scope=%q err=%v", scope, err)
	}
	if _, err := icebergUnregisterAccessScope(ctx, account, map[string]string{"scope": "cluster"}); err == nil || !strings.Contains(err.Error(), "moadmin") {
		t.Fatalf("expected accountadmin cluster-scope rejection, got %v", err)
	}
	if _, err := icebergUnregisterAccessScope(ctx, sys, map[string]string{"scope": "cluster", "scope_type": "all"}); err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("expected conflicting scope alias rejection, got %v", err)
	}
}

func TestExecuteIcebergUnregisterAccessCallRejectsUnauthorizedTargetsAndOptions(t *testing.T) {
	ctx := context.Background()
	accountAdmin := &Session{}
	accountAdmin.SetAccountId(9)
	accountAdmin.SetTenantInfo(&TenantInfo{Tenant: "tenant1", DefaultRole: accountAdminRoleName})

	tests := []struct {
		name    string
		options string
		want    string
	}{
		{name: "unsupported option", options: "external_principal=ci-local", want: "not supported"},
		{name: "system account without scope", options: "account_id=0", want: "requires account_id"},
		{name: "different account", options: "account_id=10", want: "only target the current account"},
		{name: "cluster scope", options: "scope=cluster", want: "requires moadmin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executeIcebergUnregisterAccessCall(ctx, accountAdmin, IcebergBuiltinProcedureCall{
				Name: icebergUnregisterAccessProcedure, Target: "tiera", Options: tt.options,
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("unregister options %q: got %v, want error containing %q", tt.options, err, tt.want)
			}
		})
	}

	if _, err := executeIcebergUnregisterAccessCall(ctx, nil, IcebergBuiltinProcedureCall{Name: icebergUnregisterAccessProcedure, Target: "tiera"}); err == nil || !strings.Contains(err.Error(), "requires a session") {
		t.Fatalf("nil session: got %v, want session error", err)
	}
}

func icebergCallResult(row []interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for range row {
		mrs.AddColumn(&MysqlColumn{})
	}
	mrs.AddRow(row)
	return mrs
}

func TestIcebergAccessPolicyStateValidation(t *testing.T) {
	ctx := context.Background()
	state, err := icebergAccessPolicyState(ctx, map[string]string{})
	if err != nil || state != "enabled" {
		t.Fatalf("expected default enabled state, got %q err %v", state, err)
	}
	state, err = icebergAccessPolicyState(ctx, map[string]string{"state": "Audit"})
	if err != nil || state != "audit" {
		t.Fatalf("expected audit state alias, got %q err %v", state, err)
	}
	if _, err := icebergAccessPolicyState(ctx, map[string]string{"policy_state": "delete"}); err == nil ||
		!strings.Contains(err.Error(), "enabled, disabled, or audit") {
		t.Fatalf("expected invalid policy_state error, got %v", err)
	}
}

func TestParseIcebergBuiltinCallRejectsInvalidArgsBeforeStoredProcedureLookup(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call iceberg_rewrite_manifests(42)", 1)
	if err != nil {
		t.Fatalf("parse Iceberg CALL: %v", err)
	}
	call := stmt.(*tree.CallStmt)
	_, ok, err := parseIcebergBuiltinCall(context.Background(), call)
	if !ok {
		t.Fatalf("expected Iceberg builtin call")
	}
	if err == nil || !strings.Contains(err.Error(), "requires target as a string literal") {
		t.Fatalf("expected string literal validation error, got %v", err)
	}
}

func TestQualifiedIcebergCallFallsBackToStoredProcedurePath(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "call app.iceberg_rewrite_manifests('ksa_gold.sales.orders')", 1)
	if err != nil {
		t.Fatalf("parse qualified CALL: %v", err)
	}
	call := stmt.(*tree.CallStmt)
	_, ok, err := parseIcebergBuiltinCall(context.Background(), call)
	if err != nil {
		t.Fatalf("qualified call should not be parsed as Iceberg builtin: %v", err)
	}
	if ok {
		t.Fatalf("qualified procedure names should remain stored procedure calls")
	}
}

func TestExecuteIcebergBuiltinCallDefaultsToNotSupported(t *testing.T) {
	call := IcebergBuiltinProcedureCall{
		Name: "iceberg_rewrite_manifests",
		Parsed: maintenance.ParsedCall{
			Target: "ksa_gold.sales.orders",
		},
	}
	_, err := executeIcebergBuiltinCall(context.Background(), nil, call)
	if err == nil || !strings.Contains(err.Error(), "recognized but not implemented") {
		t.Fatalf("expected default not-supported error, got %v", err)
	}
}

func TestExecuteIcebergBuiltinCallUsesRuntimeExecutor(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	old, hadOld := rt.GetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey)
	defer func() {
		if hadOld {
			rt.SetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey, old)
		} else {
			rt.SetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey, nil)
		}
	}()
	var got IcebergBuiltinProcedureCall
	rt.SetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey, IcebergMaintenanceCallExecutorFunc(func(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
		got = call
		return nil, nil
	}))

	call := IcebergBuiltinProcedureCall{
		Name: "iceberg_expire_snapshots",
		Parsed: maintenance.ParsedCall{
			Operation: maintenance.OperationExpireSnapshots,
			Target:    "ksa_gold.sales.orders",
			TargetID:  maintenance.TargetIdentifier{Catalog: "ksa_gold", Namespace: "sales", Table: "orders"},
		},
	}
	_, err := executeIcebergBuiltinCall(context.Background(), nil, call)
	if err != nil {
		t.Fatalf("execute with runtime executor: %v", err)
	}
	if got.Parsed.Operation != maintenance.OperationExpireSnapshots || got.Parsed.TargetID.Catalog != "ksa_gold" {
		t.Fatalf("runtime executor received unexpected call: %+v", got)
	}
}

func TestIcebergMaintenanceCallExecutorFallsBackToGlobalRuntime(t *testing.T) {
	globalRT := moruntime.ServiceRuntime("")
	oldGlobal, hadGlobal := globalRT.GetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey)
	defer func() {
		if hadGlobal {
			globalRT.SetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey, oldGlobal)
		} else {
			globalRT.SetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey, nil)
		}
	}()

	globalRT.SetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey, IcebergMaintenanceCallExecutorFunc(func(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
		return nil, nil
	}))

	executor, ok := icebergMaintenanceCallExecutorFromRuntime("iceberg-missing-maintenance-executor-test")
	if !ok || executor == nil {
		t.Fatalf("expected global Iceberg maintenance executor fallback")
	}
}

func TestIcebergMaintenanceProcedureExecutorRunsDispatcher(t *testing.T) {
	parsed, err := maintenance.ParseProcedureCall("iceberg_rewrite_manifests", "ksa_gold.sales.orders", "ref=main")
	if err != nil {
		t.Fatalf("parse procedure call: %v", err)
	}
	var runnerReq maintenance.Request
	executor := IcebergMaintenanceProcedureExecutor{
		Executor: maintenance.ProcedureExecutor{
			Resolver: frontendFakeMaintenanceResolver{
				resolution: maintenance.ProcedureCatalogResolution{CatalogID: 42},
			},
			Dispatcher: maintenance.Dispatcher{
				Runners: map[maintenance.Operation]maintenance.Runner{
					maintenance.OperationRewriteManifests: maintenance.RunnerFunc(func(ctx context.Context, req maintenance.Request) (maintenance.Result, error) {
						runnerReq = req
						return maintenance.Result{
							SnapshotAfter:      "101",
							RewrittenFileCount: 2,
							RemovedFileCount:   1,
							CommitID:           "commit-1",
							Verified:           true,
						}, nil
					}),
				},
			},
		},
	}
	results, err := executor.ExecuteParsedIcebergMaintenanceCall(context.Background(), 7, 11, 22, "stmt-1", IcebergBuiltinProcedureCall{
		Name:   "iceberg_rewrite_manifests",
		Target: "ksa_gold.sales.orders",
		Parsed: parsed,
	})
	if err != nil {
		t.Fatalf("execute maintenance procedure: %v", err)
	}
	if runnerReq.AccountID != 7 || runnerReq.CatalogID != 42 || runnerReq.IdempotencyKey != "stmt-1" {
		t.Fatalf("unexpected runner request: %+v", runnerReq)
	}
	if runnerReq.RoleID != 11 || runnerReq.UserID != 22 {
		t.Fatalf("expected role/user to be propagated, got %+v", runnerReq)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result set, got %d", len(results))
	}
	mrs, ok := results[0].(*MysqlResultSet)
	if !ok {
		t.Fatalf("expected MysqlResultSet, got %T", results[0])
	}
	if mrs.GetRowCount() != 1 || mrs.GetColumnCount() != 7 {
		t.Fatalf("unexpected result shape: rows=%d cols=%d", mrs.GetRowCount(), mrs.GetColumnCount())
	}
	snapshot, err := mrs.GetString(context.Background(), 0, 2)
	if err != nil || snapshot != "101" {
		t.Fatalf("unexpected snapshot result %q err=%v", snapshot, err)
	}
	rewritten, err := mrs.GetUint64(context.Background(), 0, 3)
	if err != nil || rewritten != 2 {
		t.Fatalf("unexpected rewritten count %d err=%v", rewritten, err)
	}
}

type frontendFakeMaintenanceResolver struct {
	resolution maintenance.ProcedureCatalogResolution
	err        error
}

func (r frontendFakeMaintenanceResolver) ResolveMaintenanceCatalog(ctx context.Context, accountID uint32, catalogName string) (maintenance.ProcedureCatalogResolution, error) {
	if r.err != nil {
		return maintenance.ProcedureCatalogResolution{}, r.err
	}
	return r.resolution, nil
}
