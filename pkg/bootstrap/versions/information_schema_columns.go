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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// InformationSchemaColumnsUpgradeEntry refreshes the current COLUMNS definition.
// Historical and forward handlers share the same idempotent view repair.
func InformationSchemaColumnsUpgradeEntry() UpgradeEntry {
	return UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "COLUMNS",
		UpgType:                 MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaColumnsDDL,
		RequiredProtocolVersion: defines.MORPCVersion109,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, definition, err := CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "COLUMNS")
			if err == nil && !exists {
				exists, definition, err = CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "columns")
			}
			return exists && definition == sysview.InformationSchemaColumnsDDL, err
		},
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.COLUMNS;", sysview.InformationDBConst),
	}
}
