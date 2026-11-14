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

package disttae

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func inventorySystemDatabase(name string) bool {
	return slices.Contains(catalog.SystemDatabases, strings.ToLower(name))
}

// writeDatabaseInventory borrows the DDL owner. It neither advances the outer
// statement nor starts/commits another transaction. Callers must not hold the
// workspace mutex while invoking the internal executor.
func writeDatabaseInventory(ctx context.Context, op client.TxnOperator, query string) error {
	result, err := executeDatabaseCatalogWrite(ctx, op, query, true)
	if err != nil {
		return err
	}
	affected := result.AffectedRows
	result.Close()
	if affected != 1 {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return nil
}

func executeDatabaseCatalogWrite(ctx context.Context, op client.TxnOperator, query string, requireLock bool) (executor.Result, error) {
	txn, err := txnIsValid(op)
	if err != nil {
		return executor.Result{}, err
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return executor.Result{}, err
	}
	value, ok := moruntime.ServiceRuntime(txn.proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return executor.Result{}, moerr.NewInternalError(ctx, "database inventory executor unavailable")
	}
	statement := executor.StatementOption{}.WithDisableLog()
	if requireLock {
		statement = statement.WithRequireLock()
	}
	opts := executor.Options{}.WithTxn(op).WithDisableIncrStatement().WithAccountID(account).
		WithTimeZone(txn.proc.GetSessionInfo().TimeZone).
		WithStatementOption(statement)
	return value.(executor.SQLExecutor).Exec(ctx, query, opts)
}

func initializeDatabaseInventory(ctx context.Context, op client.TxnOperator, name string, id uint64) error {
	if inventorySystemDatabase(name) {
		return nil
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	return writeDatabaseInventory(ctx, op, fmt.Sprintf(
		"insert into mo_catalog.mo_database_inventory(account_id,database_id,epoch) values(%d,%d,1)", account, id))
}

// The caller has reserved the database inventory in this same transaction.
// Routine metadata must retire before the database identity, not in a later
// response-stage transaction that could target a replacement with this name.
// Imported UDF bodies are retained: neither this transaction nor a client
// response proves that snapshots/clones have released those file references.
func retireDatabaseRoutines(ctx context.Context, op client.TxnOperator, name string) error {
	if inventorySystemDatabase(name) {
		return nil
	}
	for _, table := range []string{"mo_stored_procedure", "mo_user_defined_function"} {
		result, err := executeDatabaseCatalogWrite(ctx, op,
			"delete from mo_catalog."+table+" where db="+sqlquote.String(name), false)
		if err != nil {
			return err
		}
		result.Close()
	}
	return nil
}

// Retire name-keyed compatibility settings before the database identity, in
// the same reserved owner. A later response-stage DELETE could erase settings
// belonging to a committed replacement database with the same name.
func retireDatabaseCompatibility(ctx context.Context, op client.TxnOperator, name string) error {
	if inventorySystemDatabase(name) {
		return nil
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	result, err := executeDatabaseCatalogWrite(ctx, op, fmt.Sprintf(
		"delete from mo_catalog.mo_mysql_compatibility_mode where account_id=%d and dat_name=%s", account, sqlquote.String(name)), false)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

func advanceDatabaseInventory(ctx context.Context, op client.TxnOperator, name string, id uint64) error {
	if inventorySystemDatabase(name) {
		return nil
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	if err := writeDatabaseInventory(ctx, op, catalog.DatabaseInventoryIncrementSQL(account, id)); err != nil {
		return err
	}
	txn, err := txnIsValid(op)
	if err != nil {
		return err
	}
	if txn.tableOps != nil {
		txn.tableOps.fencePrivateInventory(account, id, txn.statementID)
	}
	return nil
}

func retireDatabaseInventory(ctx context.Context, op client.TxnOperator, name string, id uint64) error {
	if inventorySystemDatabase(name) {
		return nil
	}
	if err := advanceDatabaseInventory(ctx, op, name, id); err != nil {
		return err
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	return writeDatabaseInventory(ctx, op, fmt.Sprintf(
		"delete from mo_catalog.mo_database_inventory where account_id=%d and database_id=%d", account, id))
}

// ReserveDatabaseInventory fences the caller's fixed source token with a real
// row write until its existing publication transaction commits or rolls back.
// This typed primitive does not enable the public Rebase publication path.
func (e *Engine) ReserveDatabaseInventory(ctx context.Context, name string, id, epoch uint64, op client.TxnOperator) error {
	if op == nil || op.IsSnapOp() || inventorySystemDatabase(name) || id == 0 || epoch == 0 || epoch == math.MaxUint64 {
		return moerr.NewInvalidInput(ctx, "invalid database inventory reservation")
	}
	meta := op.Txn()
	if meta.Mode != pbtxn.TxnMode_Pessimistic || meta.Isolation != pbtxn.TxnIsolation_RC {
		return moerr.NewNotSupported(ctx, "database inventory reservation requires pessimistic RC")
	}
	db, err := e.Database(ctx, name, op)
	if err != nil {
		return err
	}
	if db.(*txnDatabase).databaseId != id {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	return writeDatabaseInventory(ctx, op, fmt.Sprintf(
		"update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=%d and database_id=%d and epoch=%d", account, id, epoch))
}
