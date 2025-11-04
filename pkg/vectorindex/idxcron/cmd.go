package idxcron

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

func RegisterUpdate(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	accountId uint32,
	tableId uint64,
	dbname string,
	tablename string,
	indexname string,
	action string,
	metadata string,
) (ok bool, err error) {

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := "SELECT"
	res, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}

func UnregisterUpdate(ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tableId uint64,
	dbname string,
	tablename string,
	indexname string,
	action string,
) (err error) {

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := "SELECT"
	res, err := sqlexec.RunSql(sqlproc, sql)
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

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := "SELECT"
	res, err := sqlexec.RunSql(sqlproc, sql)
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

	duration := 5 * time.Minute
	newctx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	newctx, cancel := context.WithTimeout(newctx, duration)
	defer cancel()
	sqlctx := sqlexec.NewSqlContext(newctx, cnUUID, txn, catalog.System_Account, nil)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)

	sql := "SELECT"
	res, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer res.Close()

	return
}
