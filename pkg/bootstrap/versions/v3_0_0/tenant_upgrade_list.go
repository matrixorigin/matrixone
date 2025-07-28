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

package v3_0_0

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	drop_mo_retention,
}

var drop_mo_retention = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: "mo_retention",
	UpgType:   versions.DROP_TABLE,
	UpgSql:    fmt.Sprintf("drop table if exists %s.%s", catalog.MO_CATALOG, "mo_retention"),
	CheckFunc: func(txn executor.TxnExecutor, accountId uint32) (bool, error) {
		exist, err := versions.CheckTableDefinition(txn, accountId, catalog.MO_CATALOG, "mo_retention")
		return !exist, err
	},
}
