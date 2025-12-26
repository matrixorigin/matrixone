// Copyright 2025 Matrix Origin
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

package function

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	utiljson "github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type featureRegistryRow struct {
	FeatureCode string          `json:"feature_code"`
	Description string          `json:"description"`
	ScopeSpec   json.RawMessage `json:"scope_spec"`
	Enabled     bool            `json:"enabled"`
}

type featureLimitRow struct {
	AccountID   int64  `json:"account_id"`
	FeatureCode string `json:"feature_code"`
	Scope       string `json:"scope"`
	Quota       int64  `json:"quota"`
}

type featureScopeSpec struct {
	AllowedScope []string `json:"allowed_scope"`
}

func moFeatureGetInternalExecutor(proc *process.Process) (executor.SQLExecutor, error) {
	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return nil, moerr.NewNotSupported(proc.Ctx, "no implement sqlExecutor")
	}
	return v.(executor.SQLExecutor), nil
}

func moFeatureInternalExecOpts(proc *process.Process) executor.Options {
	opts := executor.Options{}.
		WithDatabase(moCatalog).
		WithAccountID(sysAccountID).
		WithTxn(proc.GetTxnOperator()).
		WithTimeZone(proc.GetSessionInfo().TimeZone)
	if proc.GetTxnOperator() != nil {
		opts = opts.WithDisableIncrStatement()
	}
	return opts
}

func moFeatureEscapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func moFeatureRegistryQueryOne(exec executor.SQLExecutor, proc *process.Process, featureCode string) (*featureRegistryRow, error) {
	featureCode = strings.ToUpper(strings.TrimSpace(featureCode))
	sql := fmt.Sprintf(
		"select feature_code, description, scope_spec, enabled from %s.%s where feature_code = '%s'",
		moCatalog, catalog.MO_FEATURE_REGISTRY, moFeatureEscapeSQLString(featureCode),
	)
	res, err := exec.Exec(proc.Ctx, sql, moFeatureInternalExecOpts(proc))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var (
		found bool
		row   featureRegistryRow
	)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows <= 0 {
			return true
		}
		found = true
		row.FeatureCode = string(cols[0].GetBytesAt(0))
		row.Description = string(cols[1].GetBytesAt(0))
		scopeSpecBytes := cols[2].GetBytesAt(0)
		if cols[2].GetType().Oid == types.T_json && len(scopeSpecBytes) > 0 {
			var bj bytejson.ByteJson
			if err := bj.Unmarshal(scopeSpecBytes); err == nil {
				if visible, err := bj.MarshalJSON(); err == nil {
					scopeSpecBytes = visible
				}
			}
		}
		row.ScopeSpec = append(row.ScopeSpec[:0], scopeSpecBytes...)
		row.Enabled = vector.GetFixedAtNoTypeCheck[bool](cols[3], 0)
		return true
	})
	if !found {
		return nil, nil
	}
	return &row, nil
}

func moFeatureLimitQueryOne(exec executor.SQLExecutor, proc *process.Process, accountID int64, featureCode, scope string) (*featureLimitRow, error) {
	featureCode = strings.ToUpper(strings.TrimSpace(featureCode))
	sql := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = '%s'",
		moCatalog,
		catalog.MO_FEATURE_LIMIT,
		accountID,
		moFeatureEscapeSQLString(featureCode),
		moFeatureEscapeSQLString(scope),
	)
	res, err := exec.Exec(proc.Ctx, sql, moFeatureInternalExecOpts(proc))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var (
		found bool
		row   featureLimitRow
	)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows <= 0 {
			return true
		}
		found = true
		row.AccountID = accountID
		row.FeatureCode = featureCode
		row.Scope = scope
		row.Quota = vector.GetFixedAtNoTypeCheck[int64](cols[0], 0)
		return true
	})
	if !found {
		return nil, nil
	}
	return &row, nil
}

func moFeatureRequireSysAccount(proc *process.Process) error {
	accountId, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return err
	}
	if accountId != sysAccountID {
		return moerr.NewNotSupported(proc.Ctx, "only support sys account")
	}
	return nil
}

func moFeatureAppendPrettyResult(rs *vector.FunctionResult[types.Varlena], ok bool, old any, now any) error {
	out := map[string]any{
		"old": old,
		"now": now,
	}
	return rs.AppendBytes(utiljson.Pretty(out), false)
}

func moFeatureValidateLimitTarget(proc *process.Process, exec executor.SQLExecutor, featureCode, scope string) error {
	reg, err := moFeatureRegistryQueryOne(exec, proc, featureCode)
	if err != nil {
		return err
	}
	if reg == nil {
		return moerr.NewInvalidInputf(proc.Ctx, "unknown feature_code %q", featureCode)
	}
	if !reg.Enabled {
		return moerr.NewInvalidInputf(proc.Ctx, "feature %q is disabled", featureCode)
	}

	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope == "" {
		return nil
	}

	var spec featureScopeSpec
	if err := json.Unmarshal(reg.ScopeSpec, &spec); err != nil {
		return moerr.NewInvalidInputf(proc.Ctx, "invalid scope_spec for feature %q: %v", featureCode, err)
	}
	for _, allowed := range spec.AllowedScope {
		if scope == strings.ToLower(strings.TrimSpace(allowed)) {
			return nil
		}
	}
	return moerr.NewInvalidInputf(proc.Ctx, "scope %q is not allowed for feature %q", scope, featureCode)
}

func MoFeatureRegistryUpsert(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
	if length != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_feature_registry_upsert can only be called with const args")
	}
	if err := moFeatureRequireSysAccount(proc); err != nil {
		return err
	}

	pFeatureCode := vector.GenerateFunctionStrParameter(parameters[0])
	pDescription := vector.GenerateFunctionStrParameter(parameters[1])
	pScopeSpec := vector.GenerateFunctionStrParameter(parameters[2])
	pEnabled := vector.GenerateFunctionFixedTypeParameter[bool](parameters[3])
	rs := vector.MustFunctionResult[types.Varlena](result)

	featureCodeBytes, n0 := pFeatureCode.GetStrValue(0)
	descriptionBytes, n1 := pDescription.GetStrValue(0)
	scopeSpecBytes, n2 := pScopeSpec.GetStrValue(0)
	enabled, n3 := pEnabled.GetValue(0)
	if n0 || n1 || n2 || n3 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_feature_registry_upsert does not support null args")
	}

	featureCode := strings.ToUpper(strings.TrimSpace(string(featureCodeBytes)))
	description := string(descriptionBytes)
	scopeSpec := string(scopeSpecBytes)

	exec, err := moFeatureGetInternalExecutor(proc)
	if err != nil {
		return err
	}

	oldRow, err := moFeatureRegistryQueryOne(exec, proc, featureCode)
	if err != nil {
		return err
	}

	enabledLit := "false"
	if enabled {
		enabledLit = "true"
	}
	upsertSQL := fmt.Sprintf(
		"insert into %s.%s(feature_code, description, scope_spec, enabled) values('%s', '%s', cast('%s' as json), %s) "+
			"on duplicate key update description = values(description), scope_spec = values(scope_spec), enabled = values(enabled)",
		moCatalog,
		catalog.MO_FEATURE_REGISTRY,
		moFeatureEscapeSQLString(featureCode),
		moFeatureEscapeSQLString(description),
		moFeatureEscapeSQLString(scopeSpec),
		enabledLit,
	)
	res, err := exec.Exec(proc.Ctx, upsertSQL, moFeatureInternalExecOpts(proc))
	if err != nil {
		return err
	}
	res.Close()

	nowRow, err := moFeatureRegistryQueryOne(exec, proc, featureCode)
	if err != nil {
		return err
	}
	if nowRow == nil {
		return moerr.NewInternalError(proc.Ctx, "feature registry upsert succeeded but row not found")
	}
	return moFeatureAppendPrettyResult(rs, true, oldRow, nowRow)
}

func MoFeatureLimitUpsert(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, _ *FunctionSelectList) error {
	if length != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_feature_limit_upsert can only be called with const args")
	}
	if err := moFeatureRequireSysAccount(proc); err != nil {
		return err
	}

	pAccountID := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
	pFeatureCode := vector.GenerateFunctionStrParameter(parameters[1])
	pScope := vector.GenerateFunctionStrParameter(parameters[2])
	pQuota := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
	rs := vector.MustFunctionResult[types.Varlena](result)

	accountID, n0 := pAccountID.GetValue(0)
	featureCodeBytes, n1 := pFeatureCode.GetStrValue(0)
	scopeBytes, n2 := pScope.GetStrValue(0)
	quota, n3 := pQuota.GetValue(0)
	if n0 || n1 || n2 || n3 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_feature_limit_upsert does not support null args")
	}
	if accountID < 0 {
		return moerr.NewInvalidInput(proc.Ctx, "account_id must be non-negative")
	}
	if quota < -1 || quota > math.MaxInt16 {
		return moerr.NewInvalidInputf(proc.Ctx, "quota must be within [-1, %d]", math.MaxInt16)
	}

	featureCode := strings.ToUpper(strings.TrimSpace(string(featureCodeBytes)))
	scope := strings.ToLower(strings.TrimSpace(string(scopeBytes)))

	exec, err := moFeatureGetInternalExecutor(proc)
	if err != nil {
		return err
	}

	if err := moFeatureValidateLimitTarget(proc, exec, featureCode, scope); err != nil {
		return err
	}

	oldRow, err := moFeatureLimitQueryOne(exec, proc, accountID, featureCode, scope)
	if err != nil {
		return err
	}

	upsertSQL := fmt.Sprintf(
		"insert into %s.%s(account_id, feature_code, scope, quota) values(%d, '%s', '%s', %d) "+
			"on duplicate key update quota = values(quota)",
		moCatalog,
		catalog.MO_FEATURE_LIMIT,
		accountID,
		moFeatureEscapeSQLString(featureCode),
		moFeatureEscapeSQLString(scope),
		quota,
	)
	res, err := exec.Exec(proc.Ctx, upsertSQL, moFeatureInternalExecOpts(proc))
	if err != nil {
		return err
	}
	res.Close()

	nowRow, err := moFeatureLimitQueryOne(exec, proc, accountID, featureCode, scope)
	if err != nil {
		return err
	}
	if nowRow == nil {
		return moerr.NewInternalError(proc.Ctx, "feature limit upsert succeeded but row not found")
	}
	return moFeatureAppendPrettyResult(rs, true, oldRow, nowRow)
}
