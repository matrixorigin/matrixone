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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// InformationSchemaViewDefinitionMatches accepts only the two exact statements
// authored by initialization and identity-preserving reconciliation.
func InformationSchemaViewDefinitionMatches(actual, expected string) bool {
	return actual == expected || actual == strings.Replace(expected, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
}

func CheckInformationSchemaViewDefinition(tx executor.TxnExecutor, account uint32, name string) (bool, string, error) {
	exists, definition, err := CheckViewDefinition(tx, account, sysview.InformationDBConst, name)
	if err != nil || exists || name == strings.ToLower(name) {
		return exists, definition, err
	}
	return CheckViewDefinition(tx, account, sysview.InformationDBConst, strings.ToLower(name))
}

// InformationSchemaViewUpgradeEntry shares definition checks and replacement
// across version handlers, startup and recurring maintenance.
func InformationSchemaViewUpgradeEntry(name, definition string, protocol int64) UpgradeEntry {
	return UpgradeEntry{
		Schema: sysview.InformationDBConst, TableName: name, UpgType: MODIFY_VIEW,
		UpgSql:                  strings.Replace(definition, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1),
		RequiredProtocolVersion: protocol,
		CheckFunc: func(tx executor.TxnExecutor, account uint32) (bool, error) {
			exists, actual, err := CheckInformationSchemaViewDefinition(tx, account, name)
			return exists && InformationSchemaViewDefinitionMatches(actual, definition), err
		},
	}
}

// reconcileLegacyInformationSchemaView keeps the old logical identity when a
// released catalog used a table for this derived view. The caller owns the
// transaction: DROP and CREATE must commit or roll back together.
func (u *UpgradeEntry) reconcileLegacyInformationSchemaView(tx executor.TxnExecutor, account uint32) error {
	switch u.TableName {
	case "KEY_COLUMN_USAGE", "TABLE_PRIVILEGES", "TABLE_CONSTRAINTS":
	default:
		return moerr.NewInvalidInputNoCtxf("unsupported legacy system view %s", u.TableName)
	}
	if u.Schema != sysview.InformationDBConst || u.PreSql != "" || u.PostSql != "" ||
		!strings.HasPrefix(u.UpgSql, "CREATE OR REPLACE VIEW information_schema.") {
		return moerr.NewInvalidInputNoCtx("invalid legacy information_schema view entry")
	}
	option := UpgradeStatementOption(account)
	result, err := tx.Exec("select rel_id, rel_logical_id, relkind from mo_catalog.mo_tables where account_id = current_account_id() and reldatabase = 'information_schema' and (relname = ? or relname = ?) limit 2",
		option.WithParams([]string{u.TableName, strings.ToLower(u.TableName)}).WithMaxResultBytes(CatalogUpgradePageBytes))
	if err != nil {
		return err
	}
	var physical, logical uint64
	var kind string
	count := 0
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			count++
			physical = vector.GetFixedAtWithTypeCheck[uint64](cols[0], i)
			logical = vector.GetFixedAtWithTypeCheck[uint64](cols[1], i)
			kind = cols[2].GetStringAt(i)
		}
		return true
	})
	result.Close()
	if count > 1 || (count == 1 && (physical == 0 || (kind != catalog.SystemOrdinaryRel && kind != catalog.SystemViewRel))) {
		return moerr.NewInvalidStateNoCtxf("unexpected legacy information_schema.%s catalog identity", u.TableName)
	}
	if count == 1 && kind == catalog.SystemOrdinaryRel {
		if logical == 0 {
			logical = physical
		}
		result, err = tx.Exec(fmt.Sprintf("DROP TABLE %s", sqlquote.QualifiedIdent(u.Schema, u.TableName)), option)
		if err != nil {
			return err
		}
		result.Close()
		option = option.WithKeepLogicalId(logical)
	}
	result, err = tx.Exec(u.UpgSql, option)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}
