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

package v4_0_7

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var tenantUpgEntries = []versions.UpgradeEntry{
	partitionExpressionBinaryStorage,
}

// partition_expression has always contained the binary protobuf encoding of
// plan.Expr.  Earlier schemas declared it VARCHAR, which lets existing raw
// bytes survive but conflicts with compatible-mode UTF-8 validation on new
// writes.  Changing only the column type preserves those bytes while making
// their binary contract explicit.
var partitionExpressionBinaryStorage = versions.UpgradeEntry{
	Schema:    catalog.MO_CATALOG,
	TableName: catalog.MOPartitionTables,
	UpgType:   versions.MODIFY_COLUMN,
	UpgSql: fmt.Sprintf(
		"alter table %s.%s modify column partition_expression varbinary(2048) not null",
		catalog.MO_CATALOG,
		catalog.MOPartitionTables,
	),
	CheckFunc: func(txn executor.TxnExecutor, accountID uint32) (bool, error) {
		column, err := versions.CheckTableColumn(
			txn,
			accountID,
			catalog.MO_CATALOG,
			catalog.MOPartitionTables,
			"partition_expression",
		)
		if err != nil {
			return false, err
		}
		return column.IsExits && strings.EqualFold(column.ColType, "VARBINARY") && column.ChatLength == 2048, nil
	},
}
