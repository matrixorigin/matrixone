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
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
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

func checkBranchQuotaForAccount(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accountName string,
	accountID uint32,
	increment int64,
) error {
	return featureLimitCheckerForAccount(
		ctx, ses, bh, featureCodeBranch, "", accountName, accountID, increment,
	)
}

func branchQuotaUsageSQL(accountID uint32) string {
	return fmt.Sprintf(
		"select count(*) from %s.%s b join %s.%s t on b.table_id = t.rel_id where t.account_id = %d and b.table_deleted = false and b.level != '%s' for update",
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		accountID,
		databranchutils.AlterLineageLevel,
	)
}

func featureLimitChecker(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	featureCode string,
	featureScope string,
	increment int64,
) (err error) {
	return featureLimitCheckerForAccount(
		ctx,
		ses,
		bh,
		featureCode,
		featureScope,
		ses.GetTenantInfo().Tenant,
		ses.GetTenantInfo().TenantID,
		increment,
	)
}

func featureLimitCheckerForAccount(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	featureCode string,
	featureScope string,
	accName string,
	accId uint32,
	increment int64,
) (err error) {
	var (
		limitQuota  int64
		sql         string
		sqlRet      executor.Result
		lockingRead bool
	)

	defer func() {
		sqlRet.Close()
		if err == nil {
			err = writePendingDataBranchLineageOwnerLifecycle(ctx, bh)
		}
	}()

	// Feature limits are admission-control state. The owning mutation installs
	// its TN-ordered catalog frontier before writing the shared lifecycle gate;
	// do not advance that transaction snapshot again after the write. A branch
	// running inside an explicit SI transaction instead uses an independent short
	// RC transaction for this control-plane read, because advancing the caller's
	// fixed snapshot would violate its isolation.
	if featureCode == featureCodeBranch && featureLimitTxnUsesFixedSnapshot(bh) {
		limitQuota, err = queryQuotaInIndependentTxn(
			ctx, ses, accId, featureCode, featureScope,
		)
	} else {
		limitQuota, err = queryQuota(ctx, ses, bh, accId, featureCode, featureScope)
	}
	if err != nil {
		return err
	}
	// Serialize finite branch quota checks on the account's quota row. The lock
	// is held by the caller's background transaction through metadata insertion.
	if featureCode == featureCodeBranch && limitQuota > 0 {
		if err = checkBranchQuotaTxn(bh); err != nil {
			return err
		}
		if limitQuota, err = lockFeatureQuota(ctx, ses, bh, accId, featureCode, featureScope); err != nil {
			return err
		}
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
		// Exclude branch-managed rows from the per-account snapshot
		// quota — those are internal protection entries inserted by
		// `DATA BRANCH CREATE` and must not count against user quota
		// (design §7.3 / review PR#24313 blocking issue #2).
		sql = fmt.Sprintf(
			"select count(*) from %s.%s where account_name = '%s' and level = '%s' and kind != '%s'",
			catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, accName, featureScope,
			databranchutils.BranchSnapshotKind,
		)
	} else if featureCode == featureCodeBranch {
		ctx = defines.AttachAccountId(ctx, sysAccountID)
		lockingRead = true
		sql = branchQuotaUsageSQL(accId)
	} else {
		return moerr.NewInternalErrorNoCtxf("no such feature %s with scope %s", featureCode, featureScope)
	}

	if lockingRead {
		sqlRet, err = runSqlWithBackExec(ctx, ses, bh, sql)
	} else {
		sqlRet, err = runSql(ctx, ses, bh, sql, nil, nil)
	}
	if err != nil {
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

func checkBranchQuotaTxn(bh BackgroundExec) error {
	backExec, ok := bh.(*backExec)
	if !ok {
		return nil
	}
	txnOp := backExec.backSes.GetTxnHandler().GetTxn()
	if txnOp == nil {
		return moerr.NewInternalErrorNoCtx("missing transaction for finite branch quota")
	}
	txnMeta := txnOp.Txn()
	if txnMeta.Mode != pbtxn.TxnMode_Pessimistic || txnMeta.Isolation != pbtxn.TxnIsolation_RC {
		return moerr.NewInternalErrorNoCtx(
			"finite branch quota requires a pessimistic read committed transaction; retry outside the active transaction")
	}
	return nil
}

func featureLimitTxnUsesFixedSnapshot(bh BackgroundExec) bool {
	txnOp := backgroundExecTxnOperator(bh)
	return txnOp != nil && !txnOp.Txn().IsRCIsolation()
}

func queryQuotaInIndependentTxn(
	ctx context.Context,
	ses *Session,
	accID uint32,
	featureCode string,
	featureScope string,
) (quota int64, err error) {
	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{
		forcePessimisticRC:         true,
		cancelTxnCreateWithRequest: true,
	})
	defer bh.Close()

	if err = bh.Exec(ctx, "begin"); err != nil {
		return 0, err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	if err = advanceFeatureLimitSnapshot(ctx, ses, bh); err != nil {
		return 0, err
	}
	return queryQuota(ctx, ses, bh, accID, featureCode, featureScope)
}

func advanceFeatureLimitSnapshot(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) error {
	backExec, ok := bh.(*backExec)
	if !ok {
		// Lightweight executor doubles do not expose a transaction. Production
		// feature admission always uses backExec.
		return nil
	}
	if backExec == nil || backExec.backSes == nil || backExec.backSes.GetTxnHandler() == nil {
		return moerr.NewInternalErrorNoCtx("missing transaction handler for feature-limit snapshot refresh")
	}
	txnOp := backExec.backSes.GetTxnHandler().GetTxn()
	if txnOp == nil {
		return moerr.NewInternalErrorNoCtx("missing transaction for feature-limit snapshot refresh")
	}
	return advanceFeatureLimitTxnSnapshot(ctx, ses, txnOp)
}

func advanceFeatureLimitTxnSnapshot(
	ctx context.Context,
	ses *Session,
	txnOp TxnOperator,
) error {
	if txnOp == nil {
		return moerr.NewInternalErrorNoCtx("missing transaction for feature-limit snapshot refresh")
	}
	var (
		frontier timestamp.Timestamp
		err      error
	)

	if logtailReadBarrierSupported(ses) {
		frontier, err = ses.acquireLogtailReadBarrier(ctx)
	} else {
		var minimum timestamp.Timestamp
		minimum, err = ses.legacyLogtailReadFence(ctx)
		if err == nil {
			pu := getPuIfPresent(ses.GetService())
			if pu == nil || pu.TxnClient == nil {
				return moerr.NewInternalError(
					ctx, "missing transaction client for feature-limit snapshot refresh")
			}
			frontier, err = pu.TxnClient.WaitLogTailAppliedAt(ctx, minimum)
			if err == nil && frontier.Less(minimum) {
				return moerr.NewInternalError(
					ctx, "feature-limit snapshot did not reach the required timestamp")
			}
		}
	}
	if err != nil {
		return err
	}

	workspace := txnOp.GetWorkspace()
	if workspace == nil {
		return moerr.NewInternalErrorNoCtx("missing workspace for feature-limit snapshot refresh")
	}
	return workspace.AdvanceSnapshot(ctx, frontier)
}

func lockFeatureQuota(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	accId uint32,
	code string,
	scope string,
) (quota int64, err error) {
	var sqlRet executor.Result
	defer func() {
		sqlRet.Close()
	}()

	ctx = defines.AttachAccountId(ctx, sysAccountID)
	code = strings.ToUpper(strings.TrimSpace(code))
	scope = strings.ToLower(strings.TrimSpace(scope))
	sql := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = '%s' for update",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accId, code, scope,
	)

	if sqlRet, err = runSqlWithBackExec(ctx, ses, bh, sql); err != nil {
		return 0, err
	}
	sqlRet.Close()
	sqlRet = executor.Result{}

	// A locking read can wait behind the previous creator without refreshing
	// this transaction's snapshot. Retain the quota-row lock while installing a
	// new TN-ordered frontier, so both the locked quota and prior branch metadata
	// are visible to the re-read below.
	if err = advanceFeatureLimitSnapshot(ctx, ses, bh); err != nil {
		return 0, err
	}

	if sqlRet, err = runSqlWithBackExec(ctx, ses, bh, sql); err != nil {
		return 0, err
	}
	if len(sqlRet.Batches) != 1 || sqlRet.Batches[0].RowCount() != 1 {
		return 0, moerr.NewInternalErrorNoCtxf("lock quota for %s(%s) failed", code, scope)
	}

	return vector.GetFixedAtNoTypeCheck[int64](sqlRet.Batches[0].Vecs[0], 0), nil
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

		if code == featureCodeBranch {
			_, err = runSqlWithBackExec(ctx, ses, bh, sql)
		} else {
			_, err = runSql(ctx, ses, bh, sql, nil, nil)
		}
		if err != nil {
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
