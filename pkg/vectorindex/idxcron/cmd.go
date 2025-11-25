// Copyright 2022 Matrix Origin
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

package idxcron

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// always replace with the new data
func RegisterUpdate(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tableId uint64,
	dbname string,
	tablename string,
	indexname string,
	action string,
	metadata string,
) (err error) {

	var tenantId uint32
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	status := IndexUpdateStatus{Status: Status_Ok, Msg: "index update registered", Time: time.Now()}
	bytes, err := sonic.Marshal(&status)
	if err != nil {
		return
	}

	sql := fmt.Sprintf("REPLACE INTO mo_catalog.mo_index_update VALUES (%d, %d, '%s', '%s', '%s', '%s', '%s', '%s', now(), now())",
		tenantId,
		tableId,
		dbname,
		tablename,
		indexname,
		action,
		metadata,
		string(bytes))

	res, err := runCmdSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}

// if action == "*", remove all actions
func UnregisterUpdate(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tableId uint64,
	indexname string,
	action string,
) (err error) {

	var tenantId uint32
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	var sql string
	if action == Action_Wildcard {
		sql = fmt.Sprintf("DELETE FROM mo_catalog.mo_index_update WHERE account_id = %d AND table_id = %d AND index_name = '%s'",
			tenantId, tableId, indexname)
	} else {
		sql = fmt.Sprintf("DELETE FROM mo_catalog.mo_index_update WHERE account_id = %d AND table_id = %d AND index_name = '%s' AND action = '%s'",
			tenantId, tableId, indexname, action)
	}

	os.Stderr.WriteString(sql)
	os.Stderr.WriteString("\n")

	res, err := runCmdSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}

func UnregisterUpdateByDbName(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	dbName string) (err error) {

	var tenantId uint32
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := fmt.Sprintf("DELETE FROM mo_catalog.mo_index_update WHERE account_id = %d AND db_name = '%s'", tenantId, dbName)
	res, err := runCmdSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}

func UnregisterUpdateByTableId(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tableId uint64) (err error) {

	var tenantId uint32
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := fmt.Sprintf("DELETE FROM mo_catalog.mo_index_update WHERE account_id = %d AND table_id = %d", tenantId, tableId)
	res, err := runCmdSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}

func RenameSrcTable(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	dbId, tableId uint64,
	oldTableName, newTablename string) (err error) {

	var tenantId uint32
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := fmt.Sprintf("UPDATE mo_catalog.mo_index_update SET table_name = '%s' WHERE account_id = %d AND table_id = %d AND table_name = '%s'",
		newTablename, tenantId, tableId, oldTableName)
	res, err := runCmdSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}
