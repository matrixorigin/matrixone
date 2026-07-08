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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const icebergBuiltinProcedurePrefix = "iceberg_"
const icebergRegisterAccessProcedure = "iceberg_register_access"
const IcebergMaintenanceCallExecutorRuntimeKey = "iceberg.maintenance.call.executor"

type IcebergMaintenanceCallExecutor interface {
	ExecuteIcebergMaintenanceCall(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error)
}

type IcebergMaintenanceCallExecutorFunc func(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error)

func (f IcebergMaintenanceCallExecutorFunc) ExecuteIcebergMaintenanceCall(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
	return f(ctx, ses, call)
}

type IcebergBuiltinProcedureCall struct {
	Name    string
	Target  string
	Options string
	Parsed  maintenance.ParsedCall
}

type IcebergMaintenanceProcedureExecutor struct {
	Executor maintenance.ProcedureExecutor
}

func (e IcebergMaintenanceProcedureExecutor) ExecuteIcebergMaintenanceCall(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
	if ses == nil {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg builtin procedure execution requires a session")
	}
	tenant := ses.GetTenantInfo()
	var roleID, userID uint64
	if tenant != nil {
		roleID = uint64(tenant.GetDefaultRoleID())
		userID = uint64(tenant.GetUserID())
	}
	return e.ExecuteParsedIcebergMaintenanceCall(ctx, ses.GetAccountId(), roleID, userID, ses.GetStmtId().String(), call)
}

func (e IcebergMaintenanceProcedureExecutor) ExecuteParsedIcebergMaintenanceCall(ctx context.Context, accountID uint32, roleID uint64, userID uint64, statementID string, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
	req := maintenance.ProcedureExecutionRequestFromParsed(accountID, statementID, call.Parsed)
	req.RoleID = roleID
	req.UserID = userID
	result, err := e.Executor.Execute(ctx, req)
	if err != nil {
		return nil, api.ToMOErr(ctx, err)
	}
	return []ExecResult{icebergMaintenanceProcedureResultSet(call.Parsed.Operation, result)}, nil
}

func parseIcebergBuiltinCall(ctx context.Context, call *tree.CallStmt) (IcebergBuiltinProcedureCall, bool, error) {
	if !isIcebergBuiltinProcedure(call) {
		return IcebergBuiltinProcedureCall{}, false, nil
	}
	name := strings.TrimSpace(tree.String(call.Name, dialect.MYSQL))
	if len(call.Args) < 1 || len(call.Args) > 2 {
		return IcebergBuiltinProcedureCall{}, true, moerr.NewInvalidInputf(ctx, "Iceberg builtin procedure %s requires target and optional options string arguments", name)
	}
	target, err := icebergBuiltinProcedureStringArg(ctx, name, "target", call.Args[0])
	if err != nil {
		return IcebergBuiltinProcedureCall{}, true, err
	}
	options := ""
	if len(call.Args) == 2 {
		options, err = icebergBuiltinProcedureStringArg(ctx, name, "options", call.Args[1])
		if err != nil {
			return IcebergBuiltinProcedureCall{}, true, err
		}
	}
	if strings.EqualFold(name, icebergRegisterAccessProcedure) {
		if options == "" {
			return IcebergBuiltinProcedureCall{}, true, moerr.NewInvalidInput(ctx, "Iceberg builtin procedure iceberg_register_access requires options string argument")
		}
		return IcebergBuiltinProcedureCall{
			Name:    name,
			Target:  target,
			Options: options,
			Parsed:  maintenance.ParsedCall{Target: target, Options: icebergBuiltinOptions(options)},
		}, true, nil
	}
	parsed, err := maintenance.ParseProcedureCall(name, target, options)
	if err != nil {
		return IcebergBuiltinProcedureCall{}, true, api.ToMOErr(ctx, err)
	}
	return IcebergBuiltinProcedureCall{
		Name:    name,
		Target:  target,
		Options: options,
		Parsed:  parsed,
	}, true, nil
}

func executeIcebergBuiltinCall(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
	if strings.EqualFold(call.Name, icebergRegisterAccessProcedure) {
		return executeIcebergRegisterAccessCall(ctx, ses, call)
	}
	service := ""
	if ses != nil {
		service = ses.GetService()
	}
	if executor, ok := icebergMaintenanceCallExecutorFromRuntime(service); ok {
		return executor.ExecuteIcebergMaintenanceCall(ctx, ses, call)
	}
	return nil, moerr.NewNotSupportedf(ctx, "Iceberg builtin procedure %s for %s is recognized but not implemented in this phase", call.Name, call.Parsed.Target)
}

func executeIcebergRegisterAccessCall(ctx context.Context, ses FeSession, call IcebergBuiltinProcedureCall) ([]ExecResult, error) {
	if ses == nil || ses.GetTenantInfo() == nil {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires a session")
	}
	tenant := ses.GetTenantInfo()
	if !tenant.IsAdminRole() {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires accountadmin or moadmin role")
	}
	opts := icebergBuiltinOptions(call.Options)
	targetAccountID, err := icebergAccessAccountID(ctx, ses, opts)
	if err != nil {
		return nil, err
	}
	if targetAccountID == 0 && !tenant.IsSysTenant() {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires account_id")
	}
	if !tenant.IsSysTenant() && targetAccountID != ses.GetAccountId() {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration can only target the current account")
	}
	scopeType := strings.ToLower(firstNonEmptyIcebergAccessOption(opts, "scope_type", "scope"))
	if scopeType == "" {
		scopeType = model.ResidencyScopeAccount
		if tenant.IsSysTenant() {
			scopeType = model.ResidencyScopeCluster
		}
	}
	if scopeType == model.ResidencyScopeCluster && !tenant.IsSysTenant() {
		return nil, moerr.NewInvalidInput(ctx, "cluster Iceberg access registration requires moadmin role")
	}
	if scopeType != model.ResidencyScopeCluster && scopeType != model.ResidencyScopeAccount {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration scope must be cluster or account")
	}

	roleID, err := icebergAccessUint64Option(ctx, opts, "role_id", uint64(tenant.GetDefaultRoleID()))
	if err != nil {
		return nil, err
	}
	userID, err := icebergAccessUint64Option(ctx, opts, "user_id", uint64(tenant.GetUserID()))
	if err != nil {
		return nil, err
	}
	if targetAccountID != 0 && roleID == model.PrincipalUnspecifiedID && userID == model.PrincipalUnspecifiedID {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires role_id or user_id")
	}
	externalPrincipal := strings.TrimSpace(firstNonEmptyIcebergAccessOption(opts, "external_principal", "principal"))
	if externalPrincipal == "" {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires external_principal")
	}
	endpoint := strings.TrimSpace(firstNonEmptyIcebergAccessOption(opts, "endpoint", "allowed_endpoint"))
	region := strings.TrimSpace(firstNonEmptyIcebergAccessOption(opts, "region", "allowed_region"))
	bucket := strings.TrimSpace(firstNonEmptyIcebergAccessOption(opts, "bucket", "allowed_bucket"))
	if endpoint == "" || region == "" || bucket == "" {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires endpoint, region, and bucket")
	}
	policyState, err := icebergAccessPolicyState(ctx, opts)
	if err != nil {
		return nil, err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	catalogID, err := queryIcebergCatalogID(ctx, bh, targetAccountID, call.Target)
	if err != nil {
		return nil, err
	}
	if catalogID == 0 {
		return nil, moerr.NewInvalidInputf(ctx, "iceberg catalog %s does not exist for account %d", call.Target, targetAccountID)
	}
	catalogURI := strings.TrimSpace(opts["catalog_uri"])
	if catalogURI == "" {
		catalogURI, err = queryIcebergCatalogURI(ctx, bh, targetAccountID, catalogID)
		if err != nil {
			return nil, err
		}
	}
	if catalogURI == "" {
		return nil, moerr.NewInvalidInput(ctx, "Iceberg access registration requires catalog_uri")
	}

	policyAccountID := targetAccountID
	if scopeType == model.ResidencyScopeCluster {
		policyAccountID = 0
	}
	policy := model.ResidencyPolicy{
		ScopeType:         scopeType,
		AccountID:         policyAccountID,
		CatalogID:         catalogID,
		AllowedCatalogURI: catalogURI,
		AllowedEndpoint:   endpoint,
		AllowedRegion:     region,
		AllowedBucket:     bucket,
		PolicyState:       policyState,
	}
	policy, err = sqliceberg.NormalizeResidencyPolicyStorageIdentity(ctx, policy)
	if err != nil {
		return nil, err
	}
	catalogURI = policy.AllowedCatalogURI
	if policyState == model.ResidencyPolicyEnabled {
		if err := sqliceberg.ValidateResidencyPolicy(ctx, policy); err != nil {
			return nil, err
		}
	}
	scopeJSON := firstNonEmptyIcebergAccessOption(opts, "scope_json")
	if scopeJSON == "" {
		scopeJSON = "{}"
	}
	if err := bh.Exec(ctx, fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,catalog_id,mo_role_id,mo_user_id,external_principal,scope_json,created_by,version) values (%d,%d,%d,%d,%s,%s,%d,1) on duplicate key update external_principal = %s, scope_json = %s, updated_at = utc_timestamp, version = version + 1",
		sqliceberg.TablePrincipalMap,
		targetAccountID,
		catalogID,
		roleID,
		userID,
		quoteIcebergSQLString(externalPrincipal),
		quoteIcebergSQLString(scopeJSON),
		tenant.GetUserID(),
		quoteIcebergSQLString(externalPrincipal),
		quoteIcebergSQLString(scopeJSON),
	)); err != nil {
		return nil, err
	}
	if err := bh.Exec(ctx, fmt.Sprintf(
		"insert into mo_catalog.%s(scope_type,account_id,catalog_id,allowed_catalog_uri,allowed_endpoint,allowed_region,allowed_bucket,policy_state,created_by,version) values (%s,%d,%d,%s,%s,%s,%s,%s,%d,1) on duplicate key update policy_state = %s, updated_at = utc_timestamp, version = version + 1",
		sqliceberg.TableResidencyPolicy,
		quoteIcebergSQLString(scopeType),
		policyAccountID,
		catalogID,
		quoteIcebergSQLString(policy.AllowedCatalogURI),
		quoteIcebergSQLString(policy.AllowedEndpoint),
		quoteIcebergSQLString(policy.AllowedRegion),
		quoteIcebergSQLString(policy.AllowedBucket),
		quoteIcebergSQLString(policyState),
		tenant.GetUserID(),
		quoteIcebergSQLString(policyState),
	)); err != nil {
		return nil, err
	}
	return []ExecResult{icebergRegisterAccessResultSet(targetAccountID, catalogID, externalPrincipal, scopeType)}, nil
}

func queryIcebergCatalogURI(ctx context.Context, bh BackgroundExec, accountID uint32, catalogID uint64) (string, error) {
	sql := fmt.Sprintf(
		"select uri from mo_catalog.%s where account_id = %d and catalog_id = %d",
		sqliceberg.TableCatalogs,
		accountID,
		catalogID,
	)
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return "", err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return "", err
	}
	if len(erArray) == 0 || erArray[0].GetRowCount() == 0 {
		return "", nil
	}
	return erArray[0].GetString(ctx, 0, 0)
}

func icebergAccessAccountID(ctx context.Context, ses FeSession, opts map[string]string) (uint32, error) {
	value := strings.TrimSpace(opts["account_id"])
	if value == "" {
		return ses.GetAccountId(), nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, moerr.NewInvalidInput(ctx, "Iceberg access registration account_id must be an unsigned integer")
	}
	return uint32(parsed), nil
}

func icebergAccessUint64Option(ctx context.Context, opts map[string]string, key string, fallback uint64) (uint64, error) {
	value := strings.TrimSpace(opts[key])
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, moerr.NewInvalidInputf(ctx, "Iceberg access registration %s must be an unsigned integer", key)
	}
	return parsed, nil
}

func icebergAccessPolicyState(ctx context.Context, opts map[string]string) (string, error) {
	state := strings.ToLower(strings.TrimSpace(firstNonEmptyIcebergAccessOption(opts, "policy_state", "state")))
	if state == "" {
		return model.ResidencyPolicyEnabled, nil
	}
	switch state {
	case model.ResidencyPolicyEnabled, model.ResidencyPolicyDisabled, model.ResidencyPolicyAudit:
		return state, nil
	default:
		return "", moerr.NewInvalidInput(ctx, "Iceberg access registration policy_state must be enabled, disabled, or audit")
	}
}

func firstNonEmptyIcebergAccessOption(opts map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(opts[key]); value != "" {
			return value
		}
	}
	return ""
}

func icebergMaintenanceCallExecutorFromRuntime(service string) (IcebergMaintenanceCallExecutor, bool) {
	if executor, ok := icebergMaintenanceCallExecutorFromRuntimeExact(service); ok {
		return executor, true
	}
	if strings.TrimSpace(service) != "" {
		return icebergMaintenanceCallExecutorFromRuntimeExact("")
	}
	return nil, false
}

func icebergBuiltinOptions(options string) map[string]string {
	out := make(map[string]string)
	for _, item := range strings.Split(options, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		if key == "" {
			continue
		}
		value := ""
		if len(parts) == 2 {
			value = strings.Trim(strings.TrimSpace(parts[1]), `'"`)
		}
		out[key] = value
	}
	return out
}

func icebergMaintenanceCallExecutorFromRuntimeExact(service string) (IcebergMaintenanceCallExecutor, bool) {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return nil, false
	}
	value, ok := rt.GetGlobalVariables(IcebergMaintenanceCallExecutorRuntimeKey)
	if !ok || value == nil {
		return nil, false
	}
	executor, ok := value.(IcebergMaintenanceCallExecutor)
	return executor, ok
}

func isIcebergBuiltinProcedure(call *tree.CallStmt) bool {
	if call == nil || call.Name == nil || !call.Name.HasNoNameQualifier() {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(tree.String(call.Name, dialect.MYSQL)))
	return strings.HasPrefix(name, icebergBuiltinProcedurePrefix)
}

func icebergBuiltinProcedureStringArg(ctx context.Context, procedure, argName string, expr tree.Expr) (string, error) {
	var out string
	switch value := expr.(type) {
	case *tree.StrVal:
		out = strings.TrimSpace(value.String())
	case *tree.NumVal:
		if value.Kind() == tree.Str {
			out = strings.TrimSpace(value.String())
		}
	}
	if out == "" {
		return "", moerr.NewInvalidInputf(ctx, "Iceberg builtin procedure %s requires %s as a string literal", procedure, argName)
	}
	return out, nil
}

func icebergMaintenanceProcedureResultSet(operation maintenance.Operation, result maintenance.Result) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, colSpec := range []icebergShowColumn{
		{name: "operation", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "status", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "snapshot_after", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "rewritten_file_count", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "removed_file_count", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "commit_id", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "verified", typ: defines.MYSQL_TYPE_TINY},
	} {
		col := new(MysqlColumn)
		col.SetName(colSpec.name)
		col.SetColumnType(colSpec.typ)
		mrs.AddColumn(col)
	}
	status := maintenance.StatusCommitted
	if result.Unknown {
		status = maintenance.StatusUnknown
	}
	mrs.AddRow([]interface{}{
		string(operation),
		status,
		result.SnapshotAfter,
		result.RewrittenFileCount,
		result.RemovedFileCount,
		result.CommitID,
		result.Verified,
	})
	return mrs
}

func icebergRegisterAccessResultSet(accountID uint32, catalogID uint64, externalPrincipal, scopeType string) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, colSpec := range []icebergShowColumn{
		{name: "operation", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "status", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "account_id", typ: defines.MYSQL_TYPE_LONG},
		{name: "catalog_id", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "external_principal", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "scope_type", typ: defines.MYSQL_TYPE_VARCHAR},
	} {
		col := new(MysqlColumn)
		col.SetName(colSpec.name)
		col.SetColumnType(colSpec.typ)
		mrs.AddColumn(col)
	}
	mrs.AddRow([]interface{}{
		"register_access",
		"committed",
		accountID,
		catalogID,
		externalPrincipal,
		scopeType,
	})
	return mrs
}
