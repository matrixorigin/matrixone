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

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestUpgradeEntries(t *testing.T) {
	require.Len(t, tenantUpgEntries, 2)
	require.Len(t, clusterUpgEntries, 1)
	require.Equal(t, retireKafkaSinkDaemonTasks.UpgSql, clusterUpgEntries[0].UpgSql)
	require.Equal(t, mongodb.TableConnections, tenantUpgEntries[0].TableName)
	require.Equal(t, mongodb.TableMappings, tenantUpgEntries[1].TableName)
	for _, entry := range tenantUpgEntries {
		require.Equal(t, versions.CREATE_NEW_TABLE, entry.UpgType)
		require.Contains(t, strings.ToLower(entry.UpgSql), "create table mo_catalog.")
	}

	meta := Handler.Metadata()
	require.Equal(t, "4.0.6", meta.Version)
	require.Equal(t, "4.0.5", meta.MinUpgradeVersion)
	require.Equal(t, versions.Yes, meta.UpgradeTenant)
	require.Equal(t, uint32(len(tenantUpgEntries)+len(clusterUpgEntries)), meta.VersionOffset)
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
