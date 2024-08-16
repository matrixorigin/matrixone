// Copyright 2021 - 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func createTableAndWaitCNApplied(
	t *testing.T,
	db string,
	tableName string,
	tableSQL string,
	createOnCN embed.ServiceOperator,
	waitOnCNs ...embed.ServiceOperator,
) {
	createTestDatabase(t, db, createOnCN)
	for _, cn := range waitOnCNs {
		waitDatabaseCreated(t, db, cn)
	}

	execSQL(
		t,
		db,
		tableSQL,
		createOnCN,
	)

	for _, cn := range waitOnCNs {
		waitTableCreated(t, db, tableName, cn)
	}
}

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

func execSQL(
	t *testing.T,
	db string,
	sql string,
	cn embed.ServiceOperator,
) timestamp.Timestamp {
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var txnOp client.TxnOperator
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txnOp = txn.Txn()
			res, err := txn.Exec(sql, executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.WithDatabase(db),
	)

	require.NoError(t, err)
	return txnOp.Txn().CommitTS
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

func getSQLExecutor(
	cn embed.ServiceOperator,
) executor.SQLExecutor {
	return cn.RawService().(cnservice.Service).GetSQLExecutor()
}
