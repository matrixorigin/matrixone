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

package compile

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// CheckIgnoredRows fails a build that read rows source rows and left out more of them than the
// limit of the index's ignored table ignoredDef in database db, or all of them.
func CheckIgnoredRows(ctx CompileContext, ignoredDef *plan.IndexDef, db string, rows int64) error {
	limit, err := catalog.IndexParamMaxBadRecords(ignoredDef.IndexAlgoParams)
	if err != nil {
		return err
	}
	tbl := sqlquote.QualifiedIdent(db, ignoredDef.IndexTableName)
	var n int64
	if err = readRows(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl), func(cols []*vector.Vector, i int) {
		n = vector.GetFixedAtNoTypeCheck[int64](cols[0], i)
	}); err != nil || n == 0 || n <= limit && n < rows {
		return err
	}
	var pk, detail string
	if err = readRows(ctx, fmt.Sprintf("SELECT CAST(%s AS VARCHAR), %s, %s FROM %s LIMIT 1",
		sqlquote.Ident(catalog.IndexIgnored_TblCol_Pk), sqlquote.Ident(catalog.IndexIgnored_TblCol_Code),
		sqlquote.Ident(catalog.IndexIgnored_TblCol_Reason), tbl), func(cols []*vector.Vector, i int) {
		pk = cols[0].GetStringAt(i)
		if cols[2].IsNull(uint64(i)) {
			detail = moerr.MessageOf(uint16(vector.GetFixedAtNoTypeCheck[int32](cols[1], i)))
		} else {
			detail = cols[2].GetStringAt(i)
		}
	}); err != nil {
		return err
	}
	if n > limit {
		return moerr.NewIndexTooManyBadRecordsNoCtx(limit, pk, detail)
	}
	return moerr.NewIndexAllRowsBadNoCtx(n, pk, detail)
}

// readRows calls f for each row of the result of sql.
func readRows(ctx CompileContext, sql string, f func(cols []*vector.Vector, i int)) error {
	rs, err := ctx.RunSqlWithResult(sql)
	if err != nil {
		return err
	}
	defer rs.Close()
	rs.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			f(cols, i)
		}
		return true
	})
	return nil
}
