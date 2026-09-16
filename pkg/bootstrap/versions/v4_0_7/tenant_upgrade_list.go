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

package v4_0_7

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// The index metadata provenance migration is NOT an entry: the set of tables it alters is
// only known at runtime (one metadata table per index, per account), which a fixed UpgSql
// cannot express. It runs from HandleTenantUpgrade, the same escape hatch v4_0_6 uses for
// upgradeLegacyForeignKeyMetadata.
var tenantUpgEntries = []versions.UpgradeEntry{
	upgradeInformationSchemaViews(),
}

// UpgradeInformationSchemaViewsAfterProtocolCheck reuses the guarded VIEWS
// migration for post-upgrade reconciliation. The caller checks the protocol
// immediately before calling this function so an ErrNotSupported from the
// DDL cannot be mistaken for a retryable protocol gate.
func UpgradeInformationSchemaViewsAfterProtocolCheck(
	txn executor.TxnExecutor,
	accountID uint32,
) error {
	return tenantUpgEntries[0].UpgradeAfterProtocolCheck(txn, accountID)
}

// upgradeInformationSchemaViews is deliberately scheduled in v4.0.7. The
// v4.0.6 entry only reaches tenants that were still below v4.0.6; a tenant
// already recorded at v4.0.6 must receive a new upgrade identity before the
// parser-derived VIEWS definition can be installed.
func upgradeInformationSchemaViews() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:                  sysview.InformationDBConst,
		TableName:               "VIEWS",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaViewsDDL,
		RequiredProtocolVersion: defines.MORPCVersion75,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, viewDef, err := versions.CheckViewDefinition(
				txn, accountID, sysview.InformationDBConst, "VIEWS")
			if err != nil {
				return false, err
			}
			return exists && viewDef == sysview.InformationSchemaViewsDDL, nil
		},
		PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;",
			sysview.InformationDBConst, "VIEWS"),
	}
}
