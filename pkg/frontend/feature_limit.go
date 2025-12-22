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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	featureCodeSnapshot = "SNAPSHOT"
	featureCodeBranch   = "BRANCH"
)

const (
	defaultFeatureLimitForSys = -1
	defaultBranchLimit        = 50
	defaultSnapshotLimit      = 50
)

type moFeatureScopeSpec struct {
	AllowedScope []string `json:"allowed_scope"`
}

func checkSnapshotQuota(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	increment int64,
	level string,
) (err error) {
	return featureLimitChecker(ctx, ses, bh, featureCodeSnapshot, level, increment)
}

func checkBranchQuota(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	increment int64,
) (err error) {
	return featureLimitChecker(ctx, ses, bh, featureCodeBranch, "", increment)
}

func featureLimitChecker(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	featureCode string,
	featureScope string,
	increment int64,
) (err error) {
	var (
		limitQuota int64
		sql        string
		sqlRet     executor.Result
		accName    = ses.GetTenantInfo().Tenant
		accId      = ses.GetTenantInfo().TenantID
	)

	defer func() {
		sqlRet.Close()
	}()

	if limitQuota, err = queryQuota(ctx, ses, bh, accId, featureCode, featureScope); err != nil {
		return err
	}

	if limitQuota == 0 {
		// disabled this feature
		return moerr.NewInternalErrorNoCtxf(
			"feature %s with scope %s has disabled for account %s",
			featureCode, featureScope, accName,
		)
	} else if limitQuota < 0 {
		// unlimited
		return nil
	}

	if increment > limitQuota {
		return moerr.NewInternalErrorNoCtxf(
			"feature %s with scope %s has reached the limit of %d",
			featureCode, featureScope, limitQuota,
		)
	}

	if featureCode == featureCodeSnapshot {
		sql = fmt.Sprintf(
			"select count(*) from %s.%s where account_name = '%s' and level = '%s'",
			catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, accName, featureScope,
		)
	} else if featureCode == featureCodeBranch {
		ctx = defines.AttachAccountId(ctx, sysAccountID)
		sql = fmt.Sprintf(
			"select count(*) from %s.%s where creator = %d and table_deleted = false",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, accId,
		)
	} else {
		return moerr.NewInternalErrorNoCtxf("no such feature %s with scope %s", featureCode, featureScope)
	}

	if sqlRet, err = runSql(
		ctx, ses, bh, sql, nil, nil,
	); err != nil {
		return err
	}

	if len(sqlRet.Batches) == 0 || sqlRet.Batches[0].RowCount() == 0 {
		// zero snapshot created
		return nil
	}

	pinned := vector.GetFixedAtNoTypeCheck[int64](sqlRet.Batches[0].Vecs[0], 0)
	if pinned+increment > limitQuota {
		return moerr.NewInternalErrorNoCtxf(
			"feature %s with scope %s has reached the limit of %d",
			featureCode, featureScope, limitQuota,
		)
	}

	return nil
}

func queryQuota(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accId uint32,
	code string,
	scope string,
) (quota int64, err error) {

	var (
		sqlRet executor.Result
	)

	defer func() {
		sqlRet.Close()
	}()

	ctx = defines.AttachAccountId(ctx, sysAccountID)
	code = strings.ToUpper(strings.TrimSpace(code))
	scope = strings.ToLower(strings.TrimSpace(scope))

	enabled, allowedScopes, exist, err := queryFeatureRegistry(ctx, ses, bh, code)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 0, moerr.NewInternalErrorNoCtxf("feature %s is not registered", code)
	}
	if !enabled {
		return 0, nil
	}
	if scope != "" && !allowedScopes[scope] {
		return 0, moerr.NewInternalErrorNoCtxf("feature %s does not allow scope %s", code, scope)
	}

	sql := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accId, code, scope,
	)

	if sqlRet, err = runSql(
		ctx, ses, bh, sql, nil, nil,
	); err != nil {
		return 0, err
	}

	if len(sqlRet.Batches) == 0 || sqlRet.Batches[0].RowCount() == 0 {
		// no record for this account, init
		if code == featureCodeSnapshot {
			quota = defaultSnapshotLimit
		} else {
			quota = defaultBranchLimit
		}

		if accId == sysAccountID {
			quota = defaultFeatureLimitForSys
		}

		sql = fmt.Sprintf(
			"insert into %s.%s(account_id, feature_code, scope, quota) values(%d, '%s', '%s', %d) on duplicate key update quota = quota;",
			catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accId, code, scope, quota,
		)

		if _, err = runSql(
			ctx, ses, bh, sql, nil, nil,
		); err != nil {
			return 0, err
		}

		return quota, nil
	}

	if len(sqlRet.Batches) > 1 || sqlRet.Batches[0].RowCount() > 1 {
		return 0, moerr.NewInternalErrorNoCtxf("query quota for %s(%s) failed", code, scope)
	}

	quota = vector.GetFixedAtNoTypeCheck[int64](sqlRet.Batches[0].Vecs[0], 0)
	return quota, nil
}

func queryFeatureRegistry(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	code string,
) (enabled bool, allowedScopes map[string]bool, exist bool, err error) {
	var (
		sqlRet executor.Result
		spec   moFeatureScopeSpec
	)

	defer func() {
		sqlRet.Close()
	}()

	allowedScopes = make(map[string]bool)

	sql := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, code,
	)

	if sqlRet, err = runSql(
		ctx, ses, bh, sql, nil, nil,
	); err != nil {
		return false, nil, false, err
	}

	if len(sqlRet.Batches) == 0 || sqlRet.Batches[0].RowCount() == 0 {
		return false, allowedScopes, false, nil
	}
	if len(sqlRet.Batches) > 1 || sqlRet.Batches[0].RowCount() > 1 {
		return false, nil, false, moerr.NewInternalErrorNoCtxf("query feature registry for %s failed", code)
	}

	enabledVec := sqlRet.Batches[0].Vecs[0]
	switch enabledVec.GetType().Oid {
	case types.T_bool:
		enabled = vector.GetFixedAtNoTypeCheck[bool](enabledVec, 0)
	case types.T_int8:
		enabled = vector.GetFixedAtNoTypeCheck[int8](enabledVec, 0) != 0
	case types.T_uint8:
		enabled = vector.GetFixedAtNoTypeCheck[uint8](enabledVec, 0) != 0
	default:
		return false, nil, false, moerr.NewInternalErrorNoCtxf(
			"invalid enabled type %s for feature %s",
			enabledVec.GetType().Oid.String(),
			code,
		)
	}

	scopeSpecVec := sqlRet.Batches[0].Vecs[1]
	if scopeSpecVec.IsNull(0) {
		return enabled, allowedScopes, true, nil
	}

	decoded := types.DecodeJson(scopeSpecVec.GetBytesAt(0))
	if err = json.Unmarshal([]byte(decoded.String()), &spec); err != nil {
		return false, nil, false, moerr.NewInternalErrorNoCtxf("invalid scope_spec for feature %s: %v", code, err)
	}
	for _, s := range spec.AllowedScope {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		allowedScopes[s] = true
	}
	return enabled, allowedScopes, true, nil
}
