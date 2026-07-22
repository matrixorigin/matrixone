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

package v4_0_3

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = makeIcebergP1TenantUpgradeEntries()

func makeIcebergP1TenantUpgradeEntries() []versions.UpgradeEntry {
	entries := make([]versions.UpgradeEntry, 0, len(icebergsql.P1WriteSystemTableDDLs))
	for _, def := range icebergsql.P1WriteSystemTableDDLs {
		table := def
		entries = append(entries, versions.UpgradeEntry{
			Schema:    table.Schema,
			TableName: table.Name,
			UpgType:   versions.CREATE_NEW_TABLE,
			UpgSql:    table.DDL,
			CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
				return versions.CheckTableDefinition(txn, accountId, table.Schema, table.Name)
			},
		})
	}
	return entries
}
