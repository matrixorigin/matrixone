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

package frontend

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// A routine writer borrows its caller's transaction. Capture identity before
// reading the definition, then reserve that exact token before persisting it.
type databaseInventoryToken struct {
	accountID         uint32
	databaseID, epoch uint64
	tracked           bool
}

func databaseInventoryReadSQL(accountID uint32, database string) string {
	return fmt.Sprintf("select d.dat_id,i.epoch from mo_catalog.mo_database d left join mo_catalog.mo_database_inventory i on i.account_id=d.account_id and i.database_id=d.dat_id where d.account_id=%d and d.datname=%s",
		accountID, quoteSQLStringLiteral(database))
}

func readDatabaseInventory(ctx context.Context, bh BackgroundExec, database string) (databaseInventoryToken, error) {
	var token databaseInventoryToken
	if database == "" {
		return token, moerr.NewNoDB(ctx)
	}
	if slices.Contains(catalog.SystemDatabases, strings.ToLower(database)) {
		return token, nil
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return token, err
	}
	bh.ClearExecResultSet()
	defer bh.ClearExecResultSet()
	// This is generated catalog SQL, not the routine body. Interpret its quoted
	// identifier value under the same canonical mode that produced the literal.
	if err := bh.ExecWithSQLMode(ctx, databaseInventoryReadSQL(accountID, database), ""); err != nil {
		return token, err
	}
	rows, err := getResultSet(ctx, bh)
	if err != nil {
		return token, err
	}
	if len(rows) == 0 || rows[0].GetRowCount() == 0 {
		return token, moerr.NewBadDB(ctx, database)
	}
	if len(rows) != 1 || rows[0].GetRowCount() != 1 || rows[0].GetColumnCount() != 2 {
		return token, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	for column := uint64(0); column < 2; column++ {
		null, err := rows[0].ColumnIsNull(ctx, 0, column)
		if err != nil {
			return token, err
		}
		if null {
			return token, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
	}
	id, err := rows[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return token, err
	}
	epoch, err := rows[0].GetUint64(ctx, 0, 1)
	if err != nil {
		return token, err
	}
	if id == 0 || epoch == 0 {
		return token, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	if epoch == math.MaxUint64 {
		return token, moerr.NewInternalError(ctx, "database inventory epoch exhausted")
	}
	return databaseInventoryToken{accountID: accountID, databaseID: id, epoch: epoch, tracked: true}, nil
}

func advanceRoutineDatabaseInventory(ctx context.Context, bh BackgroundExec, token databaseInventoryToken) error {
	if !token.tracked {
		return nil
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	if accountID != token.accountID || token.databaseID == 0 || token.epoch == 0 || token.epoch == math.MaxUint64 {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	rowCount, ok := bh.(backgroundExecRowCount)
	if !ok {
		return moerr.NewInternalError(ctx, "database inventory affected-row count unavailable")
	}
	rowCount.SetLastAffectedRows(-1)
	bh.ClearExecResultSet()
	defer bh.ClearExecResultSet()
	query := catalog.DatabaseInventoryIncrementSQL(accountID, token.databaseID) + fmt.Sprintf(" and epoch=%d", token.epoch)
	if err := bh.Exec(ctx, query); err != nil {
		return err
	}
	if rowCount.GetLastAffectedRows() != 1 {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return nil
}
