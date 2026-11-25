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

package v4_0_6

import (
	"strings"
	"testing"

	"github.com/prashantv/gostub"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 4)
	require.Len(t, clusterUpgEntries, 1)
	require.Equal(t, retireKafkaSinkDaemonTasks.UpgSql, clusterUpgEntries[0].UpgSql)
	require.Equal(t, mongodb.TableConnections, tenantUpgEntries[0].TableName)
	require.Equal(t, mongodb.TableMappings, tenantUpgEntries[1].TableName)
	for _, entry := range tenantUpgEntries[:2] {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create table mo_catalog.")
	}
	characterSetsTable := tenantUpgEntries[2]
	require.Equal(t, sysview.InformationDBConst, characterSetsTable.Schema)
	require.Equal(t, "CHARACTER_SETS", characterSetsTable.TableName)
	require.Equal(t, versions.CREATE_NEW_TABLE, characterSetsTable.UpgType)
	require.Equal(t, sysview.InformationSchemaCharacterSetsDDL, characterSetsTable.UpgSql)
	characterSets := tenantUpgEntries[3]
	require.Equal(t, sysview.InformationDBConst, characterSets.Schema)
	require.Equal(t, "CHARACTER_SETS", characterSets.TableName)
	require.Equal(t, versions.MODIFY_METADATA, characterSets.UpgType)
	require.Equal(t, sysview.InformationSchemaCharacterSetsData, characterSets.UpgSql)
	require.Contains(t, strings.ToLower(characterSets.PreSql), "delete from information_schema.character_sets")

	meta := Handler.Metadata()
	require.Equal(t, "4.0.6", meta.Version)
	require.Equal(t, "4.0.5", meta.MinUpgradeVersion)
	require.Equal(t, versions.Yes, meta.UpgradeTenant)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries)), meta.VersionOffset)
}

func TestEnsureInformationSchemaCharacterSetsTableIsIdempotent(t *testing.T) {
	entry := ensureInformationSchemaCharacterSetsTable()
	exists := false
	stub := gostub.Stub(&versions.CheckTableDefinition, func(_ executor.TxnExecutor, accountID uint32, schema, table string) (bool, error) {
		require.Equal(t, uint32(42), accountID)
		require.Equal(t, sysview.InformationDBConst, schema)
		require.Equal(t, "character_sets", table)
		return exists, nil
	})
	defer stub.Reset()

	var executed []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		if sql == entry.UpgSql {
			exists = true
		}
		return executor.Result{}, nil
	}, nil)

	require.NoError(t, entry.Upgrade(txn, 42))
	require.Equal(t, []string{sysview.InformationSchemaCharacterSetsDDL}, executed)

	executed = nil
	require.NoError(t, entry.Upgrade(txn, 42))
	require.Empty(t, executed)
}

func TestPopulateInformationSchemaCharacterSetsIsIdempotent(t *testing.T) {
	entry := populateInformationSchemaCharacterSets()
	populated := false
	var executed []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		switch {
		case strings.HasPrefix(sql, "SELECT 1 FROM information_schema.CHARACTER_SETS"):
			if populated {
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			}
		case sql == entry.UpgSql:
			populated = true
		}
		return executor.Result{}, nil
	}, nil)

	require.NoError(t, entry.Upgrade(txn, 42))
	require.Len(t, executed, 3)
	require.True(t, strings.HasPrefix(executed[0], "SELECT 1 FROM information_schema.CHARACTER_SETS"))
	require.Equal(t, entry.PreSql, executed[1])
	require.Equal(t, entry.UpgSql, executed[2])

	executed = nil
	require.NoError(t, entry.Upgrade(txn, 42))
	require.Len(t, executed, 1)
	require.True(t, strings.HasPrefix(executed[0], "SELECT 1 FROM information_schema.CHARACTER_SETS"))
}

func TestRetireKafkaSinkDaemonTasks(t *testing.T) {
	const filter = "task_metadata_executor = 4 and task_status in (0, 1, 3, 6, 7, 9)"
	require.Equal(t, filter, activeKafkaSinkTaskFilter())
	require.Equal(t, catalog.MOTaskDB, retireKafkaSinkDaemonTasks.Schema)
	require.Equal(t, catalog.MOSysDaemonTask, retireKafkaSinkDaemonTasks.TableName)
	require.Equal(t, versions.MODIFY_METADATA, retireKafkaSinkDaemonTasks.UpgType)
	require.Equal(t,
		"update mo_task.sys_daemon_task set task_status = 8, update_at = current_timestamp() where "+filter,
		retireKafkaSinkDaemonTasks.UpgSql,
	)

	checkSQL := "select 1 from mo_task.sys_daemon_task where " + filter + " limit 1"
	hasActiveTask := true
	var executed []string
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		switch sql {
		case checkSQL:
			if hasActiveTask {
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			}
		case retireKafkaSinkDaemonTasks.UpgSql:
			hasActiveTask = false
		}
		return executor.Result{}, nil
	}, nil)

	require.NoError(t, retireKafkaSinkDaemonTasks.Upgrade(txn, catalog.System_Account))
	require.Equal(t, []string{checkSQL, retireKafkaSinkDaemonTasks.UpgSql}, executed)

	executed = nil
	require.NoError(t, retireKafkaSinkDaemonTasks.Upgrade(txn, catalog.System_Account))
	require.Equal(t, []string{checkSQL}, executed,
		"an already-retired cluster must not execute the update again")
}
