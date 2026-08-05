// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	newMongoDBCatalogTable(mongodb.TableConnections, mongodb.ConnectionsDDL),
	newMongoDBCatalogTable(mongodb.TableMappings, mongodb.MappingsDDL),
	populateInformationSchemaCharacterSets(),
}

func populateInformationSchemaCharacterSets() versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    sysview.InformationDBConst,
		TableName: "CHARACTER_SETS",
		UpgType:   versions.MODIFY_METADATA,
		PreSql: "DELETE FROM information_schema.CHARACTER_SETS " +
			"WHERE lower(CHARACTER_SET_NAME) IN ('binary','utf8','utf8mb4')",
		UpgSql: sysview.InformationSchemaCharacterSetsData,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDataExist(txn, accountID,
				"SELECT 1 FROM information_schema.CHARACTER_SETS "+
					"WHERE CHARACTER_SET_NAME = 'binary' AND DEFAULT_COLLATE_NAME = 'binary' AND MAXLEN = 1 "+
					"AND EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
					"WHERE CHARACTER_SET_NAME = 'utf8' AND DEFAULT_COLLATE_NAME = 'utf8_bin' AND MAXLEN = 4) "+
					"AND EXISTS (SELECT 1 FROM information_schema.CHARACTER_SETS "+
					"WHERE CHARACTER_SET_NAME = 'utf8mb4' AND DEFAULT_COLLATE_NAME = 'utf8mb4_bin' AND MAXLEN = 4) "+
					"LIMIT 1")
		},
	}
}

func newMongoDBCatalogTable(name, ddl string) versions.UpgradeEntry {
	return versions.UpgradeEntry{
		Schema:    catalog.MO_CATALOG,
		TableName: name,
		UpgType:   versions.CREATE_NEW_TABLE,
		UpgSql:    ddl,
		CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
			return versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, name)
		},
	}
}
