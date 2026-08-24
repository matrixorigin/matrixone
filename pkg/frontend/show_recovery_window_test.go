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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type recoveryWindowExecRecorder struct {
	backgroundExecTest
}

func (recorder *recoveryWindowExecRecorder) Exec(ctx context.Context, sql string) error {
	recorder.sql2result[sql] = newMrsForShowTables(nil)
	return recorder.backgroundExecTest.Exec(ctx, sql)
}

func TestSearchTablesReadsCatalogAtSnapshotTimestamp(t *testing.T) {
	ses := newValidateSession(t)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant"})

	bh := &recoveryWindowExecRecorder{}
	bh.init()
	tableToSnaps, tableToPitrs, err := searchTables(
		context.Background(), ses, bh, tree.RECOVERYWINDOWLEVELDATABASE,
		"tenant", "db", "", nil,
		[]tableRecoveryWindowForSnapshot{{
			snapshotName: "snapshot",
			level:        tree.SNAPSHOTLEVELDATABASE.String(),
			ts:           42,
			databaseName: "db",
		}},
		1,
	)
	require.NoError(t, err)
	require.Empty(t, tableToSnaps)
	require.Empty(t, tableToPitrs)
	require.Len(t, bh.executedSQLs, 1)
	require.Contains(t, bh.executedSQLs[0], "`mo_catalog`.`mo_tables` {MO_TS = 42}")
	require.NotContains(t, bh.executedSQLs[0], "SNAPSHOT =")
}
