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
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// IndexConflict reports a catalog object that cannot be safely reconciled by
// automatically creating an index with the expected name.
type IndexConflict struct{ Message string }

func (e *IndexConflict) Error() string { return e.Message }

// checkCatalogIndex validates the relation definition, then verifies that the
// catalog points at a single existing physical index table. mo_task indexes do
// not have mo_indexes entries, so only their relation definition is available.
func checkCatalogIndex(txn executor.TxnExecutor, accountID uint32, schema, table, name string, unique bool, columns ...string) (bool, error) {
	result, err := txn.Exec("SHOW CREATE TABLE "+sqlquote.QualifiedIdent(schema, table), executor.StatementOption{}.WithAccountID(accountID))
	if err != nil {
		return false, err
	}
	var createSQL string
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 1 && len(cols) == 2 {
			createSQL = cols[1].GetStringAt(0)
		}
		return false
	})
	result.Close()
	statements, err := mysql.Parse(context.Background(), createSQL, 1)
	if err != nil {
		return false, err
	}
	defer func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	}()
	if len(statements) != 1 {
		return false, moerr.NewInternalErrorNoCtx("missing table definition during index upgrade")
	}
	definition, ok := statements[0].(*tree.CreateTable)
	if !ok {
		return false, moerr.NewInternalErrorNoCtx("invalid table definition during index upgrade")
	}
	found := false
	for _, def := range definition.Defs {
		var indexName string
		var parts []*tree.KeyPart
		var actualUnique bool
		switch index := def.(type) {
		case *tree.Index:
			indexName, parts = index.Name, index.KeyParts
			if index.KeyType != tree.INDEX_TYPE_INVALID && index.KeyType != tree.INDEX_TYPE_BTREE {
				if strings.EqualFold(indexName, name) {
					return false, &IndexConflict{fmt.Sprintf("index %s.%s.%s uses an unexpected algorithm", schema, table, name)}
				}
			}
		case *tree.UniqueIndex:
			indexName, parts, actualUnique = index.GetIndexName(), index.KeyParts, true
		default:
			continue
		}
		if !strings.EqualFold(indexName, name) {
			continue
		}
		if found || actualUnique != unique || len(parts) != len(columns) {
			return false, &IndexConflict{fmt.Sprintf("index %s.%s.%s has an unexpected definition", schema, table, name)}
		}
		for i, part := range parts {
			if part == nil || part.ColName == nil || part.Expr != nil || part.Length != 0 || part.Direction == tree.Descending ||
				!strings.EqualFold(part.ColName.ColName(), columns[i]) {
				return false, &IndexConflict{fmt.Sprintf("index %s.%s.%s has unexpected columns", schema, table, name)}
			}
		}
		found = true
	}
	if !found || schema == catalog.MOTaskDB {
		return found, nil
	}
	query := fmt.Sprintf("select count(distinct i.id), count(distinct i.index_table_name), "+
		"count(distinct case when b.relkind = 'i' then b.relname end) "+
		"from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on t.rel_id = i.table_id "+
		"left join mo_catalog.mo_tables b on b.reldatabase = t.reldatabase and b.relname = i.index_table_name and b.account_id = t.account_id "+
		"where t.account_id = %d and t.reldatabase = %s and t.relname = %s and i.name = %s",
		accountID, sqlquote.String(schema), sqlquote.String(table), sqlquote.String(name))
	result, err = txn.Exec(query, executor.StatementOption{}.WithAccountID(accountID))
	if err != nil {
		return false, err
	}
	valid := false
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 1 && len(cols) == 3 {
			valid = vector.GetFixedAtWithTypeCheck[int64](cols[0], 0) == 1 &&
				vector.GetFixedAtWithTypeCheck[int64](cols[1], 0) == 1 &&
				vector.GetFixedAtWithTypeCheck[int64](cols[2], 0) == 1
		}
		return false
	})
	result.Close()
	if !valid {
		return false, &IndexConflict{fmt.Sprintf("index %s.%s.%s has missing or inconsistent physical catalog state", schema, table, name)}
	}
	return true, nil
}
