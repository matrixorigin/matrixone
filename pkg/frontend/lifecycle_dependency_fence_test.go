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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestLifecycleFeatureConfigurationLockUsesSystemFeatureRow(t *testing.T) {
	bh := &lineagePublicationLockExec{}
	bh.init()
	ctx := defines.AttachAccountId(context.Background(), 42)

	require.NoError(t, lockLifecycleFeatureConfiguration(ctx, bh))
	require.Equal(t, uint32(catalog.System_Account), bh.accountID)
	require.Equal(t, []string{
		"update mo_catalog.mo_feature_registry set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'LIFECYCLE'",
	}, bh.executedSQLs)
}

func TestRejectLifecycleArchiveRestoreScope(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 17)
	scope := lifecycleArchiveRestoreScope{
		level:        tree.RESTORELEVELTABLE,
		accountID:    17,
		snapshotTS:   123456,
		databaseName: "db1",
		tableName:    "t1",
	}
	probes, err := lifecycleArchiveRestoreProbes(scope)
	require.NoError(t, err)
	require.Len(t, probes, 3)
	require.Contains(t, probes[0].sql, "action='ARCHIVE'")
	require.Contains(t, probes[0].sql, "logical_table_id in (select rel_logical_id")
	require.Contains(t, probes[0].sql, "reldatabase='db1'")
	require.Contains(t, probes[0].sql, "relname='t1'")
	require.NotContains(t, probes[0].sql, "logical_table_id=42")
	require.Contains(t, probes[0].sql, "{MO_TS = 123456}")
	require.Contains(t, probes[1].sql, "state<>'PURGED'")
	require.Contains(t, probes[2].sql, "mode in ('ARCHIVE_WHOLE','ARCHIVE_REWRITE')")
	require.Contains(t, probes[2].sql, "state<>'CLEANED'")
	for _, probe := range probes {
		_, parseErr := mysql.Parse(context.Background(), probe.sql, 1)
		require.NoError(t, parseErr)
	}

	for hit := range probes {
		base := &backgroundExecTest{}
		base.init()
		background := &lifecycleRestoreContextExec{backgroundExecTest: base}
		for index, probe := range probes {
			rows := [][]interface{}(nil)
			if index == hit {
				rows = [][]interface{}{{"owner"}}
			}
			base.sql2result[probe.sql] = newMrsForPasswordOfUser(rows)
		}

		err = rejectLifecycleArchiveRestoreScope(
			ctx,
			background,
			scope,
			"RESTORE SNAPSHOT",
		)
		require.ErrorContains(t, err, "Lifecycle Archive")
		require.Equal(t, probes[hit].accountID, background.accountIDs[hit])
	}
}

func TestLifecycleArchiveClusterRootProbeCoversCurrentAndHistoricalOwners(t *testing.T) {
	require.NotContains(t, lifecycleArchiveRootProbeSQL(0), "MO_TS")
	require.Contains(t, lifecycleArchiveRootProbeSQL(123456), "{MO_TS = 123456}")
	for _, ts := range []int64{0, 123456} {
		sql := lifecycleArchiveRootProbeSQL(ts)
		require.Contains(t, sql, "mode in ('ARCHIVE_WHOLE','ARCHIVE_REWRITE')")
		require.Contains(t, sql, "state<>'CLEANED'")
	}
}

func TestLifecycleArchiveRestoreScopeIsArchiveOnlyAndFailsClosed(t *testing.T) {
	scope := lifecycleArchiveRestoreScope{
		level:        tree.RESTORELEVELDATABASE,
		accountID:    17,
		databaseName: "db'1",
	}
	probes, err := lifecycleArchiveRestoreProbes(scope)
	require.NoError(t, err)
	require.Len(t, probes, 3)
	for _, probe := range probes {
		require.NotContains(t, probe.sql, "TTL_REWRITE")
		require.Contains(t, probe.sql, "reldatabase='db''1'")
	}

	base := &backgroundExecTest{}
	base.init()
	background := &lifecycleRestoreContextExec{backgroundExecTest: base}
	for _, probe := range probes {
		base.sql2result[probe.sql] = newMrsForPasswordOfUser(nil)
	}
	require.NoError(t, rejectLifecycleArchiveRestoreScope(
		context.Background(),
		background,
		scope,
		"RESTORE PITR",
	))
	require.Equal(t, []uint32{17, 17, catalog.System_Account}, background.accountIDs)

	base = &backgroundExecTest{}
	base.init()
	background = &lifecycleRestoreContextExec{backgroundExecTest: base}
	for _, probe := range probes {
		base.sql2result[probe.sql] = newMrsForPasswordOfUser(nil)
		base.sql2err[probe.sql] = moerr.NewNoSuchTableNoCtx(
			"mo_catalog",
			"mo_lifecycle_bindings",
		)
	}
	require.NoError(t, rejectLifecycleArchiveRestoreScope(
		context.Background(),
		background,
		scope,
		"RESTORE PITR",
	))

	wantErr := moerr.NewInternalErrorNoCtx("catalog read failed")
	base = &backgroundExecTest{}
	base.init()
	background = &lifecycleRestoreContextExec{backgroundExecTest: base}
	base.sql2err[probes[0].sql] = wantErr
	err = rejectLifecycleArchiveRestoreScope(
		context.Background(),
		background,
		scope,
		"RESTORE PITR",
	)
	require.ErrorIs(t, err, wantErr)
}

func TestLifecycleAccountRestoreRejectsTTLBindingWithoutChangingClusterGuard(t *testing.T) {
	direct, err := lifecycleArchiveRestoreProbes(lifecycleArchiveRestoreScope{
		level:             tree.RESTORELEVELACCOUNT,
		accountID:         17,
		rejectTTLBindings: true,
	})
	require.NoError(t, err)
	require.NotContains(t, direct[0].sql, "action='ARCHIVE'")

	clusterExpanded, err := lifecycleArchiveRestoreProbes(
		lifecycleArchiveRestoreScope{
			level:     tree.RESTORELEVELACCOUNT,
			accountID: 17,
		},
	)
	require.NoError(t, err)
	require.Contains(t, clusterExpanded[0].sql, "action='ARCHIVE'")
}

type lifecycleRestoreContextExec struct {
	*backgroundExecTest
	accountIDs []uint32
}

func (e *lifecycleRestoreContextExec) Exec(ctx context.Context, sql string) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	e.accountIDs = append(e.accountIDs, accountID)
	return e.backgroundExecTest.Exec(ctx, sql)
}
