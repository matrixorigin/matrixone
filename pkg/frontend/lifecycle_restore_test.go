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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestLifecycleRestorePublishedRetryNeedsNoStaging(t *testing.T) {
	require.True(t, lifecycleRestoreAlreadyPublished(true, "DONE"))
	require.False(t, lifecycleRestoreAlreadyPublished(true, "IMPORTING"))
	require.False(t, lifecycleRestoreAlreadyPublished(false, "DONE"))
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
