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

package testutils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func CreateTableAndWaitCNApplied(
	t *testing.T,
	db string,
	tableName string,
	tableSQL string,
	createOnCN embed.ServiceOperator,
	waitOnCNs ...embed.ServiceOperator,
) timestamp.Timestamp {
	CreateTestDatabase(t, db, createOnCN)
	for _, cn := range waitOnCNs {
		WaitDatabaseCreated(t, db, cn)
	}

	committedAt := ExecSQL(
		t,
		db,
		createOnCN,
		tableSQL,
	)

	for _, cn := range waitOnCNs {
		WaitTableCreated(t, db, tableName, cn)
	}
	return committedAt
}

func CreateTestDatabase(
	t *testing.T,
	name string,
	cn embed.ServiceOperator,
) {
	CreateTestDatabaseWithAccount(
		t,
		0,
		name,
		cn,
	)
}

func CreateTestDatabaseWithAccount(
	t *testing.T,
	account int32,
	name string,
	cn embed.ServiceOperator,
) {
	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseCreateTestDatabase)
	defer cancel()

	ctx = defines.AttachAccountId(ctx, uint32(account))
	res, err := sql.Exec(
		ctx,
		fmt.Sprintf("create database %s", name),
		executor.Options{}.WithAccountID(uint32(account)),
	)
	require.NoError(t, moerr.AttachCause(ctx, err))
	res.Close()

	WaitDatabaseCreatedWithAccount(t, account, name, cn)
}

func WaitTableCreated(
	t *testing.T,
	db string,
	name string,
	cn embed.ServiceOperator,
) {
	WaitTableCreatedWithAccount(
		t,
		0,
		db,
		name,
		cn,
	)
}

func WaitTableCreatedWithAccount(
	t *testing.T,
	account int32,
	db string,
	name string,
	cn embed.ServiceOperator,
) {
	for {
		if TableExistsWithAccount(t, account, db, name, cn) {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func WaitLogtailApplied(
	t *testing.T,
	min timestamp.Timestamp,
	cn embed.ServiceOperator,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()
	txn := cn.RawService().(cnservice.Service).GetTxnClient()
	_, err := txn.WaitLogTailAppliedAt(ctx, min)
	require.NoError(t, err)
}

func WaitDatabaseCreated(
	t *testing.T,
	name string,
	cn embed.ServiceOperator,
) {
	WaitDatabaseCreatedWithAccount(
		t,
		0,
		name,
		cn,
	)
}

func WaitDatabaseCreatedWithAccount(
	t *testing.T,
	account int32,
	name string,
	cn embed.ServiceOperator,
) {
	for {
		if DBExistsWithAccount(t, account, name, cn) {
			return

		}
		time.Sleep(time.Millisecond * 100)
	}
}

func ExecSQL(
	t *testing.T,
	db string,
	cn embed.ServiceOperator,
	sql ...string,
) timestamp.Timestamp {
	return ExecSQLWithReadResult(
		t,
		db,
		cn,
		nil,
		sql...,
	)
}

func ExecSQLWithAccount(
	t *testing.T,
	account int32,
	db string,
	cn embed.ServiceOperator,
	sql ...string,
) timestamp.Timestamp {
	return ExecSQLWithReadResultAndAccount(
		t,
		account,
		db,
		cn,
		nil,
		sql...,
	)
}

func ExecSQLWithReadResult(
	t *testing.T,
	db string,
	cn embed.ServiceOperator,
	reader func(int, string, executor.Result),
	sql ...string,
) timestamp.Timestamp {
	return ExecSQLWithReadResultAndAccount(
		t,
		0,
		db,
		cn,
		reader,
		sql...,
	)
}

func ExecSQLWithReadResultAndAccount(
	t *testing.T,
	account int32,
	db string,
	cn embed.ServiceOperator,
	reader func(int, string, executor.Result),
	sql ...string,
) timestamp.Timestamp {
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeoutCause(
		defines.AttachAccountId(context.Background(), uint32(account)),
		time.Second*60,
		moerr.CauseExecSQL,
	)
	defer cancel()

	var txnOp client.TxnOperator
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txnOp = txn.Txn()
			for idx, s := range sql {
				res, err := txn.Exec(s, executor.StatementOption{})
				if err != nil {
					return err
				}
				if reader != nil {
					reader(idx, s, res)
				}
				res.Close()
			}
			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithAccountID(uint32(account)).
			WithResolveVariableFunc(
				func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
					if varName == "sql_mode" {
						return "", nil
					}
					return nil, moerr.NewInternalErrorf(ctx, "variable %s not supported", varName)
				},
			),
	)

	require.NoError(t, moerr.AttachCause(ctx, err), sql)
	WaitLogtailApplied(t, txnOp.Txn().CommitTS, cn)
	return txnOp.Txn().CommitTS
}

func ExecSQLWithMinCommittedTS(
	t *testing.T,
	db string,
	cn embed.ServiceOperator,
	min timestamp.Timestamp,
	sql ...string,
) timestamp.Timestamp {
	return ExecSQLWithMinCommittedTSAndAccount(
		t,
		0,
		db,
		cn,
		min,
		sql...,
	)
}

func ExecSQLWithMinCommittedTSAndAccount(
	t *testing.T,
	account int32,
	db string,
	cn embed.ServiceOperator,
	min timestamp.Timestamp,
	sql ...string,
) timestamp.Timestamp {
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeoutCause(
		defines.AttachAccountId(context.Background(), uint32(account)),
		time.Second*60,
		moerr.CauseExecSQLWithMinCommittedTS,
	)
	defer cancel()

	var txnOp client.TxnOperator
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txnOp = txn.Txn()
			for _, s := range sql {
				res, err := txn.Exec(s, executor.StatementOption{})
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithAccountID(uint32(account)).
			WithMinCommittedTS(min),
	)

	require.NoError(t, moerr.AttachCause(ctx, err), sql)
	return txnOp.Txn().CommitTS
}

func HasName(
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

func GetDatabaseName(
	t *testing.T,
) string {
	return fmt.Sprintf(
		"db_%s_%d",
		t.Name(),
		time.Now().Nanosecond(),
	)
}

func GetSQLExecutor(
	cn embed.ServiceOperator,
) executor.SQLExecutor {
	return cn.RawService().(cnservice.Service).GetSQLExecutor()
}

func DBExists(
	t *testing.T,
	name string,
	cn embed.ServiceOperator,
) bool {
	return DBExistsWithAccount(t, 0, name, cn)
}

func DBExistsWithAccount(
	t *testing.T,
	account int32,
	name string,
	cn embed.ServiceOperator,
) bool {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseDBExists)
	defer cancel()

	ctx = defines.AttachAccountId(ctx, uint32(account))

	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(
		ctx,
		"show databases",
		executor.Options{}.WithAccountID(uint32(account)),
	)
	require.NoError(t, moerr.AttachCause(ctx, err))

	return HasName(name, res)
}

func TableExists(
	t *testing.T,
	db string,
	name string,
	cn embed.ServiceOperator,
) bool {
	return TableExistsWithAccount(
		t,
		0,
		db,
		name,
		cn,
	)
}

func TableExistsWithAccount(
	t *testing.T,
	account int32,
	db string,
	name string,
	cn embed.ServiceOperator,
) bool {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseTableExists)
	defer cancel()

	ctx = defines.AttachAccountId(ctx, uint32(account))

	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(
		ctx,
		"show tables",
		executor.Options{}.WithDatabase(db).WithAccountID(uint32(account)),
	)
	require.NoError(t, moerr.AttachCause(ctx, err))

	return HasName(name, res)
}

func WaitClusterAppliedTo(
	t *testing.T,
	c embed.Cluster,
	ts timestamp.Timestamp,
) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseWaitClusterAppliedTo)
	defer cancel()

	c.ForeachServices(
		func(s embed.ServiceOperator) bool {
			if s.ServiceType() == metadata.ServiceType_CN {
				_, err := s.RawService().(cnservice.Service).GetTimestampWaiter().GetTimestamp(
					ctx,
					ts,
				)
				require.NoError(t, moerr.AttachCause(ctx, err))
			}
			return true
		},
	)
}

func GetTableID(
	t *testing.T,
	db string,
	table string,
	txn executor.TxnExecutor,
) uint64 {
	txn.Use(catalog.MO_CATALOG)
	res, err := txn.Exec(
		fmt.Sprintf("select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s'",
			strings.ToLower(table),
			strings.ToLower(db),
		),
		executor.StatementOption{},
	)
	require.NoError(t, err)
	defer res.Close()

	id := uint64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			id = executor.GetFixedRows[uint64](cols[0])[0]
			return false
		},
	)

	return id
}

func MustParseMOCtlResult(t *testing.T, result string) string {
	var r ctlResult
	require.NoError(t, json.Unmarshal([]byte(result), &r))
	return r.Result
}

func ReadCount(
	res executor.Result,
) int {
	n := int64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			n += executor.GetFixedRows[int64](cols[0])[0]
			return true
		},
	)
	return int(n)
}

type ctlResult struct {
	Result string `json:"result"`
}

func CreateAccount(
	t *testing.T,
	c embed.Cluster,
	accountName string,
	password string,
) int32 {
	accountName = strings.ToLower(accountName)

	cn0, err := c.GetCNService(0)
	require.NoError(t, err)

	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
		cn0.GetServiceConfig().CN.Frontend.Port,
	)

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(
		fmt.Sprintf(
			"create account %s ADMIN_NAME 'root' IDENTIFIED BY '%s'",
			accountName,
			password,
		))
	require.NoError(t, err)

	accountID := int32(-1)
	ExecSQLWithReadResult(
		t,
		"mo_catalog",
		cn0,
		func(i int, s string, r executor.Result) {
			r.ReadRows(
				func(rows int, cols []*vector.Vector) bool {
					accountID = executor.GetFixedRows[int32](cols[0])[0]
					return true
				},
			)
		},
		"select account_id from mo_account where account_name = '"+accountName+"' and admin_name = 'root'",
	)
	require.NotEqual(t, int32(-1), accountID)
	return accountID
}
