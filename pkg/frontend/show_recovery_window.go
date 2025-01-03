// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func doShowRecoveryWindow(ctx context.Context, ses *Session, srw *tree.ShowRecoveryWindow) (err error) {

	bh := ses.GetRawBatchBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = bh.Exec(ctx, "rollback;")
		} else {
			err = bh.Exec(ctx, "commit;")
		}
	}()

	// check privilege
	err = checkShowRecoveryWindowPrivilege(ctx, ses, srw)
	if err != nil {
		return err
	}

	// build result columns
	// recovery window level
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("Recovery_Window_Level")

	// account name
	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Account_Name")

	// database name
	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3.SetName("Database_Name")

	// table name
	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetName("Table_Name")

	// recovery windows
	col5 := new(MysqlColumn)
	col5.SetColumnType(defines.MYSQL_TYPE_JSON)
	col5.SetName("Recovery_Windows")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)

	// recovey level
	level := srw.Level
	switch level {
	case tree.RECOVERYWINDOWLEVELACCOUNT:
	case tree.RECOVERYWINDOWLEVELDATABASE:
	case tree.RECOVERYWINDOWLEVELTABLE:
	}

	rows := make([][]interface{}, 0)
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func checkShowRecoveryWindowPrivilege(ctx context.Context, ses *Session, srw *tree.ShowRecoveryWindow) error {
	switch srw.Level {
	case tree.RECOVERYWINDOWLEVELACCOUNT:
		if len(srw.AccountName) > 0 && !ses.GetTenantInfo().IsSysTenant() {
			return moerr.NewInternalError(ctx, "only sys account can show other account's recovery window")
		}
	case tree.RECOVERYWINDOWLEVELDATABASE:
	case tree.RECOVERYWINDOWLEVELTABLE:
	}
	return nil
}
