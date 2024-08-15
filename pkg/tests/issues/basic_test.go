package issues

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func createTestDatabase(
	t *testing.T,
	name string,
	cn embed.ServiceOperator,
) {
	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := sql.Exec(
		ctx,
		fmt.Sprintf("create database %s", name),
		executor.Options{},
	)
	require.NoError(t, err)
	res.Close()

	waitDatabaseCreated(t, name, cn)
}

func execDDL(
	t *testing.T,
	db string,
	ddl string,
	cn embed.ServiceOperator,
) {
	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := sql.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(db)
			res, err := txn.Exec(ddl, executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.WithDatabase(db),
	)

	require.NoError(t, err)
}

func waitDatabaseCreated(
	t *testing.T,
	name string,
	cn embed.ServiceOperator,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()

	for {
		res, err := sql.Exec(
			ctx,
			"show databases",
			executor.Options{},
		)
		require.NoError(t, err)

		if hasName(name, res) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func waitTableCreated(
	t *testing.T,
	db string,
	name string,
	cn embed.ServiceOperator,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 1000000000*time.Second)
	defer cancel()

	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()

	for {
		res, err := sql.Exec(
			ctx,
			"show tables",
			executor.Options{}.WithDatabase(db),
		)
		require.NoError(t, err)

		if hasName(name, res) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func hasName(
	name string,
	res executor.Result,
) bool {
	defer res.Close()

	has := false
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			values := executor.GetStringRows(cols[0])
			for _, v := range values {
				if strings.EqualFold(name, v) {
					has = true
					return false
				}
			}
			return true
		},
	)
	return has
}

func getDatabaseName(
	t *testing.T,
) string {
	return fmt.Sprintf(
		"db_%s_%d",
		t.Name(),
		time.Now().Nanosecond(),
	)
}

func getFullTableName(
	db string,
	table string,
) string {
	return db + "." + table
}
