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

package compile

import (
	"context"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func withAlterCopySourceTable(ctx context.Context, option executor.StatementOption) context.Context {
	// A typed nil masks inherited COPY metadata for unrelated nested statements.
	// txnExecutor.Exec restores the parent context on all return paths.
	return context.WithValue(ctx, defines.AlterCopySourceTableKey{}, option.AlterCopySourceTable())
}

func alterCopyReferencedIndexName(name string, renames map[string]string) string {
	if name == "" || plan2.IndexNamesEqual(name, "PRIMARY") {
		return name
	}
	for original, final := range renames {
		if plan2.IndexNamesEqual(name, original) {
			return final
		}
	}
	return name
}

// One CASE evaluates every binding against its original value, including name
// swaps. Like the existing FK transfer SQL, this runs with the caller's tenant
// context: mo_foreign_keys is account-scoped, not a system-tenant update.
func alterCopyIndexRenameCatalogSQL(database, table string, renames map[string]string) string {
	names := make([]string, 0, len(renames))
	for original, final := range renames {
		if original != "" && original != final && !plan2.IndexNamesEqual(original, "PRIMARY") {
			names = append(names, original)
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	var sql strings.Builder
	sql.WriteString("update `mo_catalog`.`mo_foreign_keys` set referenced_index_name = case lower(referenced_index_name)")
	for _, original := range names {
		sql.WriteString(" when " + sqlquote.String(strings.ToLower(original)) + " then " + sqlquote.String(renames[original]))
	}
	sql.WriteString(" else referenced_index_name end where refer_db_name = " + sqlquote.String(database))
	sql.WriteString(" and refer_table_name = " + sqlquote.String(table))
	return sql.String()
}
