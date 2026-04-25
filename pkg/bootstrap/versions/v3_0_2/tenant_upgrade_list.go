// Copyright 2025 Matrix Origin
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

package v3_0_2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	upg_information_schema_columns,
	upg_information_schema_columns_geometry_srid,
	upg_information_schema_statistics,
}

var upg_information_schema_columns = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "COLUMNS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaColumnsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "COLUMNS")
		if err != nil {
			return false, err
		}
		if exists && viewDef == sysview.InformationSchemaColumnsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "COLUMNS"),
}

// upg_information_schema_columns_geometry_srid re-applies the COLUMNS view to ensure
// the SRS_ID column reflecting geometry SRID attributes is present after the geometry backport.
var upg_information_schema_columns_geometry_srid = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "COLUMNS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaColumnsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "COLUMNS")
		if err != nil {
			return false, err
		}
		if exists && viewDef == sysview.InformationSchemaColumnsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "COLUMNS"),
}

// Re-apply the STATISTICS view change in a newer tenant upgrade version so
// tenants already marked as 3.0.1 also execute the fix during compatibility upgrades.
var upg_information_schema_statistics = versions.UpgradeEntry{
	Schema:    sysview.InformationDBConst,
	TableName: "STATISTICS",
	UpgType:   versions.MODIFY_VIEW,
	UpgSql:    sysview.InformationSchemaStatisticsDDL,
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exists, viewDef, err := versions.CheckViewDefinition(txn, accountId, sysview.InformationDBConst, "STATISTICS")
		if err != nil {
			return false, err
		}

		if exists && viewDef == sysview.InformationSchemaStatisticsDDL {
			return true, nil
		}
		return false, nil
	},
	PreSql: fmt.Sprintf("DROP VIEW IF EXISTS %s.%s;", sysview.InformationDBConst, "STATISTICS"),
}
