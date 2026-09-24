// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	{
		Schema:                  catalog.MO_CATALOG,
		TableName:               catalog.MODatabaseDefaults,
		UpgType:                 versions.CREATE_NEW_TABLE,
		UpgSql:                  catalog.MoDatabaseDefaultsDDL,
		RequiredProtocolVersion: defines.MORPCVersion95,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, catalog.MODatabaseDefaults)
		},
	},
	{
		Schema:                  sysview.InformationDBConst,
		TableName:               "SCHEMATA",
		UpgType:                 versions.MODIFY_VIEW,
		UpgSql:                  sysview.InformationSchemaSchemataDDL,
		PreSql:                  "DROP VIEW IF EXISTS information_schema.SCHEMATA;",
		RequiredProtocolVersion: defines.MORPCVersion95,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			exists, definition, err := versions.CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "SCHEMATA")
			if err == nil && !exists {
				exists, definition, err = versions.CheckViewDefinition(txn, accountID, sysview.InformationDBConst, "schemata")
			}
			return exists && definition == sysview.InformationSchemaSchemataDDL, err
		},
	},
}
