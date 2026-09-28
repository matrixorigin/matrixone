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

package v4_0_8

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upgradeInformationSchemaStatistics(),
}

func upgradeInformationSchemaStatistics() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "STATISTICS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaStatisticsDDL,
		RequiredProtocolVersion: defines.MORPCVersion41,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, definition, err := versions.CheckViewDefinition(
				txn, accountID, sysview.InformationDBConst, "STATISTICS")
			// With lower_case_table_names enabled, the persisted catalog name is lowercase.
			if err == nil && !exists {
				exists, definition, err = versions.CheckViewDefinition(
					txn, accountID, sysview.InformationDBConst, "statistics")
			}
			return exists && definition == sysview.InformationSchemaStatisticsDDL, err
		},
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.STATISTICS;", sysview.InformationDBConst),
	}
}
