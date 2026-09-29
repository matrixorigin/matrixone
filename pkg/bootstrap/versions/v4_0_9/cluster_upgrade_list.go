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

package v4_0_9

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const snapshotQuotaTargetIndex = "idx_snapshot_quota_target"

// Only sys stores ACCOUNT snapshots for other tenants. The index keeps the
// second quota count bounded without adding an index to every tenant catalog.
var clusterUpgEntries = []versions.UpgradeEntry{
	{
		Schema:    catalog.MO_CATALOG,
		TableName: catalog.MO_SNAPSHOTS,
		UpgType:   versions.ADD_INDEX,
		UpgSql:    frontend.MoCatalogSysSnapshotQuotaIndexDDL,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckIndexDefinition(txn, accountID, catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, snapshotQuotaTargetIndex)
		},
	},
}
