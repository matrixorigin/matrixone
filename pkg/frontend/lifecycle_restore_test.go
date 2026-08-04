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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestLifecycleRestorePublishedRetryNeedsNoStaging(t *testing.T) {
	require.True(t, lifecycleRestoreAlreadyPublished(true, "DONE"))
	require.False(t, lifecycleRestoreAlreadyPublished(true, "IMPORTING"))
	require.False(t, lifecycleRestoreAlreadyPublished(false, "DONE"))
}

func TestHandleRestoreArchiveDatasetFailsBeforeExternalSideEffects(t *testing.T) {
	ctx := context.Background()
	require.ErrorContains(t, handleRestoreArchiveDataset(
		ctx,
		nil,
		&tree.RestoreArchiveDataset{},
	), "target is required")

	target := tree.NewTableName(
		"events_history",
		tree.ObjectNamePrefix{SchemaName: "history", ExplicitSchema: true},
		nil,
	)
	statement := &tree.RestoreArchiveDataset{
		DatasetID: "00112233-4455-6677-8899-aabbccddeeff",
		Target:    target,
	}
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	ses.SetTenantInfo(&TenantInfo{TenantID: 17})
	service := "lifecycle-restore-handler-" + t.Name()
	ses.service = service
	runtime := moruntime.NewRuntime(
		metadata.ServiceType_CN,
		service,
		nil,
		moruntime.WithClock(clock.NewHLCClock(
			func() int64 { return time.Now().UnixNano() },
			0,
		)),
	)
	moruntime.SetupServiceBasedRuntime(service, runtime)

	background := &backgroundExecTest{}
	background.init()
	featureSQL := `select enabled, scope_spec from mo_catalog.mo_feature_registry where feature_code = 'LIFECYCLE'`
	background.sql2result[featureSQL] = newMrsForFeatureRegistry(
		[][]interface{}{{int8(0), nil}},
	)
	stub := gostub.StubFunc(&NewBackgroundExec, background)
	t.Cleanup(stub.Reset)
	require.ErrorContains(t, handleRestoreArchiveDataset(ctx, ses, statement),
		"disabled by the cluster release gate")
	require.Empty(t, lifecycleRestoreSlots)
}

func TestLifecycleRestoreCNAdmissionIsFailFastAndExactlyOnce(t *testing.T) {
	slots := make(chan struct{}, 1)
	release, err := acquireLifecycleRestoreSlot(context.Background(), slots)
	require.NoError(t, err)
	require.Len(t, slots, 1)

	blockedRelease, err := acquireLifecycleRestoreSlot(context.Background(), slots)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrServiceUnavailable))
	require.Nil(t, blockedRelease)

	release()
	release()
	require.Empty(t, slots)

	release, err = acquireLifecycleRestoreSlot(context.Background(), slots)
	require.NoError(t, err)
	release()
}

func TestLifecycleRestoreRejectsExistingTargetBeforeImport(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 17)
	background := &backgroundExecTest{}
	background.init()
	checkSQL, err := getSqlForCheckDatabaseTableWithSnapshot(
		ctx,
		"history",
		"events_restore",
		17,
		0,
	)
	require.NoError(t, err)
	background.sql2result[checkSQL] = newMrsForCheckDatabaseTable(
		[][]interface{}{{int64(88)}},
	)

	err = rejectExistingLifecycleRestoreTarget(
		ctx,
		background,
		"history",
		"events_restore",
		17,
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTableAlreadyExists))
}

func TestFrontendResolveRejectsLifecycleRestoreStagingBeforeEngineAccess(t *testing.T) {
	proc := testutil.NewProc(nil)
	proc.Base.IsFrontend = true
	compilerContext := &TxnCompilerContext{execCtx: &ExecCtx{
		reqCtx: context.Background(),
		proc:   proc,
	}}

	for _, operation := range []string{
		"select",
		"insert",
		"update",
		"delete",
		"truncate",
		"alter",
		"drop",
		"rename",
	} {
		t.Run(operation, func(t *testing.T) {
			_, _, err := compilerContext.Resolve(
				"history",
				catalog.LifecycleRestoreTableNamePrefix+
					"0123456789abcdef0123456789abcdef",
				nil,
			)
			require.ErrorContains(t, err, "Lifecycle Restore staging")
		})
	}
}

func TestLifecycleRestoreRejectsReservedPublishTarget(t *testing.T) {
	require.Error(t, validateLifecycleRestoreTargetName(
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
	))
	require.NoError(t, validateLifecycleRestoreTargetName(
		catalog.LifecycleRestoreTableNamePrefix+"user_table",
	))
	require.NoError(t, validateLifecycleRestoreTargetName("events_history"))
}

func TestLifecycleRestoreCoordinatorUsesMOFaultControlPlane(t *testing.T) {
	point := lifecyclepkg.FaultBeforeRestoreInitialize
	fault.Enable()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(
			context.Background(),
			lifecyclepkg.MOFaultPointName(point),
		)
		fault.Disable()
	})
	require.NoError(t, fault.AddFaultPoint(
		context.Background(),
		lifecyclepkg.MOFaultPointName(point),
		":::",
		"echo",
		31,
		"frontend restore injection",
		false,
	))

	coordinator := newLifecycleRestoreCoordinator(nil, nil)
	err := coordinator.Faults.Inject(context.Background(), point)
	require.ErrorContains(t, err, "frontend restore injection")
}

func TestLifecycleSQLExecutorRequiresTypedRuntimeRegistration(t *testing.T) {
	service := "lifecycle-sql-executor-" + t.Name()
	runtime := moruntime.NewRuntime(
		metadata.ServiceType_CN,
		service,
		nil,
		moruntime.WithClock(clock.NewHLCClock(
			func() int64 { return time.Now().UnixNano() },
			0,
		)),
	)
	moruntime.SetupServiceBasedRuntime(service, runtime)

	_, err := lifecycleSQLExecutor(service)
	require.ErrorContains(t, err, "unavailable")

	runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, "not-an-executor")
	_, err = lifecycleSQLExecutor(service)
	require.ErrorContains(t, err, "invalid type")

	expected := executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, nil
	})
	runtime.SetGlobalVariables(moruntime.InternalSQLExecutor, expected)
	actual, err := lifecycleSQLExecutor(service)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestLifecycleDatabaseIDReadsExactlyOneCatalogRow(t *testing.T) {
	proc := testutil.NewProcess(t)
	resultForID := func(id uint64) executor.Result {
		value := batch.NewWithSize(1)
		value.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(
			value.Vecs[0], id, false, proc.Mp(),
		))
		value.SetRowCount(1)
		return executor.Result{Batches: []*batch.Batch{value}, Mp: proc.Mp()}
	}

	var executedSQL string
	databaseID, err := lifecycleDatabaseID(
		context.Background(),
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			executedSQL = sql
			return resultForID(42), nil
		}),
		17,
		"history's",
	)
	require.NoError(t, err)
	require.Equal(t, uint64(42), databaseID)
	require.True(t, strings.Contains(executedSQL, "history\\'s") ||
		strings.Contains(executedSQL, "history''s"))

	_, err = lifecycleDatabaseID(
		context.Background(),
		executor.NewMemExecutor(func(string) (executor.Result, error) {
			return executor.Result{}, nil
		}),
		17,
		"missing",
	)
	require.ErrorContains(t, err, "does not exist")

	expected := errors.New("catalog unavailable")
	_, err = lifecycleDatabaseID(
		context.Background(),
		executor.NewMemExecutor(func(string) (executor.Result, error) {
			return executor.Result{}, expected
		}),
		17,
		"history",
	)
	require.ErrorIs(t, err, expected)
}
