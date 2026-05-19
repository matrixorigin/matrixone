// Copyright 2022 Matrix Origin
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

package table_function

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// runSqlFunc matches the signature of sqlexec.RunSql so callers can pass their
// own per-algorithm mockable variable (ivfpq_runSql / cagra_runSql / …).
type runSqlFunc func(*sqlexec.SqlProcess, string) (executor.Result, error)

// quoteIdent wraps `ident` in backticks and doubles any embedded backticks —
// the standard MySQL identifier escape. Without this, a column or table name
// containing a backtick (e.g. `a“b`) would break out of the quoted-identifier
// context and let an attacker append arbitrary SQL.
func quoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// fetchSrcTableRowCount runs `SELECT count(*) FROM `db`.`src“ and returns the
// row count. Used by index create paths to auto-populate IndexCapacity when
// the user did not set it upfront.
func fetchSrcTableRowCount(proc *process.Process, runSql runSqlFunc, db, src string) (int64, error) {
	sql := fmt.Sprintf("SELECT count(*) FROM %s.%s", quoteIdent(db), quoteIdent(src))
	res, err := runSql(sqlexec.NewSqlProcess(proc), sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if len(res.Batches) == 0 || res.Batches[0].RowCount() != 1 {
		return 0, moerr.NewInternalError(proc.Ctx, "failed to determine source table row count")
	}
	return vector.GetFixedAtWithTypeCheck[int64](res.Batches[0].Vecs[0], 0), nil
}
