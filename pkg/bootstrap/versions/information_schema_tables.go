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

package versions

import (
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// InformationSchemaTablesUpgradeEntry refreshes TABLES for existing tenants.
func InformationSchemaTablesUpgradeEntry() UpgradeEntry {
	return UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "TABLES",
		UpgType:                 MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaTablesV118DDL,
		PreSql:                  "DROP VIEW IF EXISTS information_schema.TABLES;",
		RequiredProtocolVersion: defines.MORPCVersion118,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, definition, err := CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "TABLES")
			if err == nil && !exists {
				exists, definition, err = CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "tables")
			}
			return exists && definition == sysview.InformationSchemaTablesV118DDL, err
		},
	}
}
