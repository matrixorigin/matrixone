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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type featureLimitBarrierEngine struct {
	engine.Engine
	acquire func(context.Context) (timestamp.Timestamp, error)
}

func (e *featureLimitBarrierEngine) AcquireLogtailReadBarrier(
	ctx context.Context,
) (timestamp.Timestamp, error) {
	return e.acquire(ctx)
}

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

func TestAdvanceFeatureLimitTxnSnapshotUsesTNOrderedBarrier(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, ses.GetService(), nil)
	moruntime.SetupServiceBasedRuntime(ses.GetService(), rt)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)

	frontier := timestamp.Timestamp{PhysicalTime: 80, LogicalTime: 9}
	setPu(ses.GetService(), &config.ParameterUnit{
		StorageEngine: &featureLimitBarrierEngine{acquire: func(context.Context) (
			timestamp.Timestamp, error,
		) {
			return frontier, nil
		}},
	})

	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().AdvanceSnapshot(gomock.Any(), frontier).Return(nil)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(workspace)

	require.NoError(t, advanceFeatureLimitTxnSnapshot(t.Context(), ses, txnOp))
}

func TestFeatureLimitTxnUsesIndependentSnapshotForSI(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnHandler := &TxnHandler{}
	txnHandler.SetShareTxn(txnOp)
	bh := &backExec{backSes: &backSession{
		feSessionImpl: feSessionImpl{txnHandler: txnHandler},
	}}

	txnOp.EXPECT().Txn().Return(pbtxn.TxnMeta{Isolation: pbtxn.TxnIsolation_SI})
	require.True(t, featureLimitTxnUsesFixedSnapshot(bh))
	txnOp.EXPECT().Txn().Return(pbtxn.TxnMeta{Isolation: pbtxn.TxnIsolation_RC})
	require.False(t, featureLimitTxnUsesFixedSnapshot(bh))
	require.False(t, featureLimitTxnUsesFixedSnapshot(&backgroundExecTest{}))
	require.False(t, featureLimitTxnUsesFixedSnapshot((*backExec)(nil)))
}

func TestAdvanceFeatureLimitTxnSnapshotUsesRollingUpgradeFence(t *testing.T) {
	ses := newFeatureLimitTestSession(t)
	rt := moruntime.NewRuntime(
		metadata.ServiceType_CN,
		ses.GetService(),
		nil,
		moruntime.WithClock(clock.NewHLCClock(func() int64 { return 100 }, 20*time.Nanosecond)),
	)
	moruntime.SetupServiceBasedRuntime(ses.GetService(), rt)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)

	minimum := timestamp.Timestamp{PhysicalTime: 121}
	applied := timestamp.Timestamp{PhysicalTime: 125, LogicalTime: 3}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), minimum).Return(applied, nil)
	setPu(ses.GetService(), &config.ParameterUnit{TxnClient: txnClient})
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().AdvanceSnapshot(gomock.Any(), applied).Return(nil)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(workspace)

	require.NoError(t, advanceFeatureLimitTxnSnapshot(t.Context(), ses, txnOp))
}

func TestAdvanceFeatureLimitTxnSnapshotFailsClosed(t *testing.T) {
	t.Run("missing transaction handler", func(t *testing.T) {
		ses := newFeatureLimitTestSession(t)
		require.ErrorContains(t,
			advanceFeatureLimitSnapshot(t.Context(), ses, (*backExec)(nil)),
			"missing transaction handler",
		)
	})

	t.Run("missing transaction", func(t *testing.T) {
		ses := newFeatureLimitTestSession(t)
		require.ErrorContains(t,
			advanceFeatureLimitTxnSnapshot(t.Context(), ses, nil),
			"missing transaction",
		)
	})

	t.Run("missing workspace", func(t *testing.T) {
		ses := newFeatureLimitTestSession(t)
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, ses.GetService(), nil)
		moruntime.SetupServiceBasedRuntime(ses.GetService(), rt)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
		frontier := timestamp.Timestamp{PhysicalTime: 80}
		setPu(ses.GetService(), &config.ParameterUnit{
			StorageEngine: &featureLimitBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				return frontier, nil
			}},
		})
		ctrl := gomock.NewController(t)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(nil)

		require.ErrorContains(t,
			advanceFeatureLimitTxnSnapshot(t.Context(), ses, txnOp),
			"missing workspace",
		)
	})

	t.Run("barrier failure", func(t *testing.T) {
		ses := newFeatureLimitTestSession(t)
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, ses.GetService(), nil)
		moruntime.SetupServiceBasedRuntime(ses.GetService(), rt)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
		wantErr := moerr.NewInternalErrorNoCtx("barrier unavailable")
		setPu(ses.GetService(), &config.ParameterUnit{
			StorageEngine: &featureLimitBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				return timestamp.Timestamp{}, wantErr
			}},
		})
		ctrl := gomock.NewController(t)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		require.ErrorIs(t,
			advanceFeatureLimitTxnSnapshot(t.Context(), ses, txnOp),
			wantErr,
		)
	})

	t.Run("legacy waiter below fence", func(t *testing.T) {
		ses := newFeatureLimitTestSession(t)
		rt := moruntime.NewRuntime(
			metadata.ServiceType_CN,
			ses.GetService(),
			nil,
			moruntime.WithClock(clock.NewHLCClock(func() int64 { return 100 }, 20*time.Nanosecond)),
		)
		moruntime.SetupServiceBasedRuntime(ses.GetService(), rt)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion38)
		ctrl := gomock.NewController(t)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().WaitLogTailAppliedAt(
			gomock.Any(), timestamp.Timestamp{PhysicalTime: 121},
		).Return(timestamp.Timestamp{PhysicalTime: 120}, nil)
		setPu(ses.GetService(), &config.ParameterUnit{TxnClient: txnClient})
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)

		require.ErrorContains(t,
			advanceFeatureLimitTxnSnapshot(t.Context(), ses, txnOp),
			"did not reach the required timestamp",
		)
	})

	t.Run("workspace failure", func(t *testing.T) {
		ses := newFeatureLimitTestSession(t)
		rt := moruntime.NewRuntime(metadata.ServiceType_CN, ses.GetService(), nil)
		moruntime.SetupServiceBasedRuntime(ses.GetService(), rt)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion39)
		frontier := timestamp.Timestamp{PhysicalTime: 80}
		setPu(ses.GetService(), &config.ParameterUnit{
			StorageEngine: &featureLimitBarrierEngine{acquire: func(context.Context) (
				timestamp.Timestamp, error,
			) {
				return frontier, nil
			}},
		})
		ctrl := gomock.NewController(t)
		wantErr := moerr.NewInternalErrorNoCtx("advance failed")
		workspace := mock_frontend.NewMockWorkspace(ctrl)
		workspace.EXPECT().AdvanceSnapshot(gomock.Any(), frontier).Return(wantErr)
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().GetWorkspace().Return(workspace)

		require.ErrorIs(t,
			advanceFeatureLimitTxnSnapshot(t.Context(), ses, txnOp),
			wantErr,
		)
	})
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
	countSQL := branchQuotaUsageSQL(accountID)

	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(1)}})
	bh.sql2result[countSQL] = newMrsForSnapshotCount([][]interface{}{{int64(0)}})

	require.NoError(t, checkBranchQuotaForAccount(
		context.Background(), ses, bh, accountName, accountID, 1))
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

	require.NoError(t, checkBranchQuotaForAccount(
		context.Background(), ses, bh, "account-a", accountID, 1))
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

	require.NoError(t, checkBranchQuotaForAccount(
		context.Background(), ses, bh, "account-a", accountID, 1))
	require.Equal(t, []string{registrySQL, quotaSQL}, bh.executedSQLs)
}

func TestCheckBranchQuotaForAccountUsesExplicitOwner(t *testing.T) {
	const (
		targetAccountName = "target-account"
		targetAccountID   = uint32(84)
	)

	ses := newFeatureLimitTestSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, TenantID: sysAccountID})

	bh := &backgroundExecTest{}
	bh.init()
	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeBranch,
	)
	quotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = ''",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, targetAccountID, featureCodeBranch,
	)
	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(0)}})

	err := checkBranchQuotaForAccount(
		context.Background(), ses, bh, targetAccountName, targetAccountID, 1,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has disabled for account "+targetAccountName)
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
	countSQL := branchQuotaUsageSQL(accountID)

	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{{int8(1), nil}})
	bh.sql2result[quotaSQL] = newMrsForFeatureLimit(nil)
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(defaultBranchLimit)}})
	bh.sql2result[countSQL] = newMrsForSnapshotCount([][]interface{}{{int64(0)}})

	require.NoError(t, checkBranchQuotaForAccount(
		context.Background(), ses, bh, "account-a", accountID, 1))
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
