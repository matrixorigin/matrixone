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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const CatalogDataPageSize = 128

// TenantPageCursor is volatile. It is advanced only after the caller commits
// the page; a restart replays the idempotent scan from the beginning.
type TenantPageCursor struct {
	Set                     bool
	FunctionID              int32
	Database, Table, Column string
}

// mo_columns uses a serialized composite primary key. Comparing that key
// gives the storage layer a block-range predicate instead of sorting and
// filtering every earlier catalog row on each page.
func columnKeyBound(account uint32, c TenantPageCursor, op string) string {
	return fmt.Sprintf("__mo_cpkey_col %s serial(cast(%d as int unsigned), %s, %s, %s)",
		op, account, sqlquote.String(c.Database), sqlquote.String(c.Table), sqlquote.String(c.Column))
}

// UpgradeTenantEntryPage reconciles one entry or a keyset page. The returned
// cursor must not be published until the enclosing transaction commits.
func (v *versionHandle) UpgradeTenantEntryPage(_ context.Context, tenantID int32, index int, tx executor.TxnExecutor, objectLimit uint64, cursor *TenantPageCursor) (bool, error) {
	if index < 0 || index >= len(tenantUpgEntries) || cursor == nil {
		return false, moerr.NewInvalidInputNoCtxf("invalid tenant upgrade entry %d or page cursor", index)
	}
	entry := tenantUpgEntries[index]
	tx = limitedCatalogTxn{TxnExecutor: tx, limit: objectLimit}
	account := uint32(tenantID)
	option := versions.UpgradeStatementOption(account)
	udf := entry.TableName == "mo_user_defined_function" && entry.UpgType == versions.MODIFY_METADATA
	columns := entry.TableName == catalog.MO_COLUMNS && entry.UpgType == versions.MODIFY_METADATA
	if !udf && !columns {
		if err := entry.Upgrade(tx, account); err != nil {
			return false, err
		}
		if entry.UpgType == versions.ADD_INDEX {
			ready, err := entry.CheckFunc(tx, account)
			if err != nil {
				return false, err
			}
			if !ready {
				return false, &IndexConflict{fmt.Sprintf("index on %s.%s did not become ready", entry.Schema, entry.TableName)}
			}
		}
		return false, nil
	}
	// Most catalogs are already correct. Probe once before opening a keyset
	// scan; later pages must not repeat this whole-table check.
	if !cursor.Set {
		done, err := entry.CheckFunc(tx, account)
		if err != nil || done {
			return false, err
		}
	}
	if entry.RequiredProtocolVersion > 0 {
		if err := versions.CheckCommonProtocolVersion(tx, entry.RequiredProtocolVersion); err != nil {
			return false, err
		}
	}
	var query string
	if udf {
		query = "select function_id from mo_catalog.mo_user_defined_function"
		if cursor.Set {
			query += fmt.Sprintf(" where function_id > %d", cursor.FunctionID)
		}
		query += fmt.Sprintf(" order by function_id limit %d", CatalogDataPageSize)
	} else {
		query = fmt.Sprintf("select att_database, att_relname, attname from mo_catalog.mo_columns where account_id = %d and %s", account, moColumnsUnsignedValueMismatchPredicate)
		if cursor.Set {
			query += " and " + columnKeyBound(account, *cursor, ">")
		}
		query += fmt.Sprintf(" order by __mo_cpkey_col limit %d", CatalogDataPageSize)
	}
	result, err := tx.Exec(query, option)
	if err != nil {
		return false, err
	}
	next := *cursor
	count := 0
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			next.Set = true
			if udf {
				next.FunctionID = vector.GetFixedAtWithTypeCheck[int32](cols[0], i)
			} else {
				next.Database, next.Table, next.Column = cols[0].GetStringAt(i), cols[1].GetStringAt(i), cols[2].GetStringAt(i)
			}
			count++
		}
		return true
	})
	result.Close()
	if count == 0 {
		return false, nil
	}
	var bounds, update string
	if udf {
		bounds = fmt.Sprintf("function_id <= %d", next.FunctionID)
		if cursor.Set {
			bounds += fmt.Sprintf(" and function_id > %d", cursor.FunctionID)
		}
		update = fmt.Sprintf("update mo_catalog.mo_user_defined_function set arg_types = %s where %s and arg_types != %s", catalog.UserDefinedFunctionArgumentTypesSQL, bounds, catalog.UserDefinedFunctionArgumentTypesSQL)
	} else {
		bounds = columnKeyBound(account, next, "<=")
		if cursor.Set {
			bounds += " and " + columnKeyBound(account, *cursor, ">")
		}
		update = fmt.Sprintf("update mo_catalog.mo_columns set att_is_unsigned = 1 where %s and %s", bounds, moColumnsUnsignedMismatchPredicate)
		option = option.WithMoColumnsUpdate()
	}
	result, err = tx.Exec(update, option)
	if err != nil {
		return false, err
	}
	result.Close()
	*cursor = next
	return true, nil
}
