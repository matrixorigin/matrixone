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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func databaseDefaultSystemVariable(ses FeSession, name string) (any, bool, error) {
	if name != "character_set_database" && name != "collation_database" || !plan2.DatabaseDefaultsEnabled(ses.GetService()) {
		return nil, false, nil
	}
	compiler := ses.GetTxnCompileCtx()
	database := ses.GetDatabaseName()
	ctx := context.Background()
	if compiler != nil && compiler.execCtx != nil {
		ctx = compiler.GetContext()
		if database != "" && !plan2.DatabaseDefaultsSystemDatabase(database) {
			var defaults *plan.DatabaseDefaults
			var err error
			proc := compiler.GetProcess()
			if proc != nil && proc.GetTxnOperator() != nil {
				defaults, err = plan2.GetDatabaseDefaults(compiler, database, nil)
			} else {
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
				if name == "character_set_database" {
					return defaults.CharacterSet, true, nil
				}
				return defaults.Collation, true, nil
			}
		}
	}
	value, err := ses.GetSessionSysVar("collation_server")
	if err != nil {
		return nil, true, err
	}
	fallback := "utf8mb4_general_ci"
	if value != nil {
		var ok bool
		fallback, ok = value.(string)
		if !ok {
			return nil, true, moerr.NewInternalError(ctx, "collation_server is not a string")
		}
	}
	defaults, err := plan2.NormalizeDatabaseDefaults(ctx, nil, fallback)
	if err != nil {
		return nil, true, err
	}
	if name == "character_set_database" {
		return defaults.CharacterSet, true, nil
	}
	return defaults.Collation, true, nil
}

// Restore reads identity and defaults at the same source snapshot. A historical
// table-existence check is the only fallback for a snapshot predating the feature.
func readDatabaseDefaultsForRestore(ctx context.Context, bh BackgroundExec, dbName string, accountID uint32, ts int64) (*plan.DatabaseDefaults, error) {
	if !plan2.DatabaseDefaultsEnabled(bh.Service()) || plan2.DatabaseDefaultsSystemDatabase(dbName) {
		return nil, nil
	}
	sourceCtx := defines.AttachAccountId(ctx, accountID)
	snapshot := ""
	if ts > 0 {
		snapshot = fmt.Sprintf(" {MO_TS = %d}", ts)
		rows, err := getStringColsList(sourceCtx, bh, fmt.Sprintf("select relname from mo_catalog.mo_tables%s where account_id = %d and reldatabase = 'mo_catalog' and relname = '%s'", snapshot, accountID, catalog.MODatabaseDefaults), 0)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, nil
		}
	}
	sql := fmt.Sprintf("select cast(dd.collation_id as varchar(10)), cast(dd.collation_revision as varchar(10)), cast(dd.version as varchar(20)) from mo_catalog.mo_database%s db join mo_catalog.mo_database_defaults%s dd on dd.account_id=db.account_id and dd.database_id=db.dat_id where db.account_id=%d and db.datname=%s", snapshot, snapshot, accountID, quoteSQLStringLiteral(dbName))
	rows, err := getStringColsList(sourceCtx, bh, sql, 0, 1, 2)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) != 1 || len(rows[0]) != 3 {
		return nil, moerr.NewInternalError(ctx, "invalid database default metadata")
	}
	identity, err := strconv.ParseUint(rows[0][0], 10, 32)
	if err != nil {
		return nil, err
	}
	revision, err := strconv.ParseUint(rows[0][1], 10, 32)
	if err != nil {
		return nil, err
	}
	version, err := strconv.ParseUint(rows[0][2], 10, 64)
	if err != nil {
		return nil, err
	}
	return plan2.DatabaseDefaultsFromIdentity(ctx, uint32(identity), uint32(revision), version)
}

func appendDatabaseDefaultsSQL(sql string, defaults *plan.DatabaseDefaults) string {
	if defaults == nil || defaults.Version == 0 {
		return sql
	}
	return sql + " character set " + defaults.CharacterSet + " collate " + defaults.Collation
}
