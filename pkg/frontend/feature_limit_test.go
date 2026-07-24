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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func newFeatureLimitTestSession(t *testing.T) *Session {
	t.Helper()

	proc := testutil.NewProcess(t)
	service := "feature-limit-" + t.Name()
	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{
		SV:          &config.FrontendParameters{},
		FileService: proc.Base.FileService,
	})

	return &Session{
		feSessionImpl: feSessionImpl{service: service},
		proc:          proc,
	}
}

func TestCheckBranchQuotaLocksFiniteQuota(t *testing.T) {
	const (
		accountName = "account-a"
		accountID   = uint32(42)
	)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: accountName, TenantID: accountID})

	bh := &backgroundExecTest{}
	bh.init()

	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	lockedQuotaSQL := quotaSQL + " for update"
	countSQL := fmt.Sprintf(
		"select count(*) from %s.%s where creator = %d and table_deleted = false for update",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, accountID,
	)

	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[countSQL] = newMrsForSnapshotCount([][]interface{}{{int64(0)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(t, []string{registrySQL, quotaSQL, lockedQuotaSQL, lockedQuotaSQL, countSQL}, bh.executedSQLs)
}

func TestCheckBranchQuotaUsesLockedQuota(t *testing.T) {
	const accountID = uint32(42)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: "account-a", TenantID: accountID})

	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	lockedQuotaSQL := quotaSQL + " for update"

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(-1)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(t, []string{registrySQL, quotaSQL, lockedQuotaSQL, lockedQuotaSQL}, bh.executedSQLs)
}

func TestCheckBranchQuotaDoesNotLockUnlimitedQuota(t *testing.T) {
	const accountID = uint32(42)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: "account-a", TenantID: accountID})

	bh := &backgroundExecTest{}
	bh.init()
	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(-1)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(t, []string{registrySQL, quotaSQL}, bh.executedSQLs)
}

func TestCheckBranchQuotaInitializesMissingQuotaWithBackExec(t *testing.T) {
	const accountID = uint32(42)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: "account-a", TenantID: accountID})
	bh := &backgroundExecTest{}
	bh.init()

	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch,
	)
	insertSQL := fmt.Sprintf(
		"insert into %s.%s(account_id, feature_code, scope, quota) values(%d, '%s', '', %d) on duplicate key update quota = quota;",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeBranch, defaultBranchLimit,
	)
	lockedQuotaSQL := quotaSQL + " for update"
	countSQL := fmt.Sprintf(
		"select count(*) from %s.%s where creator = %d and table_deleted = false for update",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, accountID,
	)

	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit(nil)
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(defaultBranchLimit)}})
	bh.sql2result[countSQL] = newMrsForSnapshotCount([][]interface{}{{int64(0)}})

	require.NoError(t, checkBranchQuota(context.Background(), ses, bh, 1))
	require.Equal(
		t,
		[]string{registrySQL, quotaSQL, insertSQL, lockedQuotaSQL, lockedQuotaSQL, countSQL},
		bh.executedSQLs,
	)
}

func TestRunSqlWithBackExecBypassesInternalExecutor(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	bh := &backgroundExecTest{}
	bh.init()

	const sql = "select quota from mo_catalog.mo_feature_limit for update"
	bh.sql2result[sql] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})

	var internalCalled atomic.Bool
	rt := moruntime.NewRuntime(
		metadata.ServiceType_CN,
		ses.service,
		nil,
		moruntime.WithClock(clock.NewHLCClock(func() int64 { return time.Now().UnixNano() }, 0)),
	)
	moruntime.SetupServiceBasedRuntime(ses.service, rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(
		func(string) (executor.Result, error) {
			internalCalled.Store(true)
			return executor.Result{}, nil
		},
	))

	result, err := runSqlWithBackExec(context.Background(), ses, bh, sql)
	require.NoError(t, err)
	defer result.Close()
	require.False(t, internalCalled.Load())
	require.Equal(t, []string{sql}, bh.executedSQLs)
	require.Len(t, result.Batches, 1)
	require.Equal(t, 1, result.Batches[0].RowCount())
}

func TestCopyAndClearBackExecResultOwnership(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	original := buildPickStreamingBatch(
		t,
		ses.proc.Mp(),
		[]types.Type{types.T_int64.ToType()},
		[][]any{{int64(1)}},
	)
	bh := &backExec{backSes: &backSession{}}
	bh.backSes.pool = ses.proc.Mp()
	bh.backSes.resultBatches = []*batch.Batch{original}

	result, err := copyAndClearBackExecResult(ses, bh)
	require.NoError(t, err)
	defer result.Close()
	require.Empty(t, bh.backSes.resultBatches)
	require.Len(t, result.Batches, 1)
	require.NotSame(t, original, result.Batches[0])
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result.Batches[0].Vecs[0], 0))
}
