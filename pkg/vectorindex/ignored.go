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

package vectorindex

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
)

const ignoredRowsPerSql = 1024

// IgnoredRow is a source row an index cannot store: its primary key as a SQL literal, the
// reason, and the commit time (physical) of the change.
type IgnoredRow struct {
	Pk     string
	Reason string
	Ts     int64
}

// IgnoredColumns names the ignored table's columns. The caller passes the catalog's names:
// this package cannot import catalog, which reaches it again through cuvs in a gpu build.
type IgnoredColumns struct {
	Pk, Reason, Ts string
}

// IgnoredRowsSql returns the statements that record rows in the ignored table db.table.
func IgnoredRowsSql(db, table string, cols IgnoredColumns, rows []IgnoredRow) []string {
	var sqls []string
	for start := 0; start < len(rows); start += ignoredRowsPerSql {
		end := min(start+ignoredRowsPerSql, len(rows))
		var b strings.Builder
		fmt.Fprintf(&b, "REPLACE INTO %s (%s, %s, %s) VALUES ", sqlquote.QualifiedIdent(db, table),
			sqlquote.Ident(cols.Pk), sqlquote.Ident(cols.Reason), sqlquote.Ident(cols.Ts))
		for i, r := range rows[start:end] {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "(%s, %s, %d)", r.Pk, sqlquote.String(r.Reason), r.Ts)
		}
		sqls = append(sqls, b.String())
	}
	return sqls
}
