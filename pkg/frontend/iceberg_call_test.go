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
	"strings"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	results, err := executor.ExecuteParsedIcebergMaintenanceCall(context.Background(), 7, "stmt-1", IcebergBuiltinProcedureCall{
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
