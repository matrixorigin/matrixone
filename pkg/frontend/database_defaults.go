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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func databaseDefaultSystemVariable(ses FeSession, name string) (any, bool, error) {
	if name != "character_set_database" && name != "collation_database" {
		return nil, false, nil
	}
	if !plan2.DatabaseDefaultsEnabled(ses.GetService()) {
		return nil, false, nil
	}
	if name == "character_set_database" {
		return "utf8mb4", true, nil
	}
	compiler := ses.GetTxnCompileCtx()
	database := ses.GetDatabaseName()
	if compiler != nil && compiler.execCtx != nil && database != "" && !plan2.DatabaseDefaultsSystemDatabase(database) {
		var defaults *plan.DatabaseDefaults
		var err error
		proc := compiler.GetProcess()
		if proc != nil && proc.GetTxnOperator() != nil {
			defaults, err = plan2.GetDatabaseDefaults(compiler, database, nil)
		} else {
			ctx := compiler.GetContext()
			var accountID uint32
			accountID, err = defines.GetAccountId(ctx)
			if err == nil {
				bh := ses.GetBackgroundExec(ctx)
				defer bh.Close()
				defaults, err = readDatabaseDefaultsForRestore(ctx, bh, database, accountID, 0)
			}
		}
		if err != nil {
			return nil, true, err
		}
		if defaults != nil && defaults.Version != 0 {
			return defaults.Collation, true, nil
		}
	}

	v, err := ses.GetSessionSysVar("collation_server")
	if v == nil && err == nil {
		v = "utf8mb4_general_ci"
	}
	return v, true, err
}

// readDatabaseDefaultsForRestore reads source identity and defaults at one
// snapshot. The catalog-table existence query is the proof for old snapshots;
// transport/permission/decoding failures are never treated as legacy absence.
func readDatabaseDefaultsForRestore(ctx context.Context, bh BackgroundExec, dbName string, accountID uint32, ts int64) (*plan.DatabaseDefaults, error) {
	if !plan2.DatabaseDefaultsEnabled(bh.Service()) || plan2.DatabaseDefaultsSystemDatabase(dbName) {
		return nil, nil
	}
	sourceCtx := defines.AttachAccountId(ctx, accountID)
	snapshot := ""
	if ts > 0 {
		snapshot = fmt.Sprintf(" {MO_TS = %d}", ts)
	}
	if ts > 0 {
		rows, err := getStringColsList(sourceCtx, bh, fmt.Sprintf("select relname from mo_catalog.mo_tables%s where account_id = %d and reldatabase = 'mo_catalog' and relname = '%s'", snapshot, accountID, catalog.MODatabaseDefaults), 0)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, nil
		}
	}
	sql := fmt.Sprintf("select dd.character_set, dd.collation_name from mo_catalog.mo_database%s db join mo_catalog.mo_database_defaults%s dd on dd.account_id=db.account_id and dd.database_id=db.dat_id where db.account_id=%d and db.datname=%s", snapshot, snapshot, accountID, quoteSQLStringLiteral(dbName))
	rows, err := getStringColsList(sourceCtx, bh, sql, 0, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 || len(rows[0]) != 2 || rows[0][0] != "utf8mb4" || (rows[0][1] != "utf8mb4_bin" && rows[0][1] != "utf8mb4_general_ci") {
		return nil, moerr.NewInternalError(ctx, "invalid database default metadata")
	}
	return &plan.DatabaseDefaults{CharacterSet: rows[0][0], Collation: rows[0][1], Version: 1}, nil
}

func appendDatabaseDefaultsSQL(sql string, defaults *plan.DatabaseDefaults) string {
	if defaults == nil || defaults.Version == 0 {
		return sql
	}
	return sql + " character set " + defaults.CharacterSet + " collate " + defaults.Collation
}
