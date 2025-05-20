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
	sql := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseCreateTestDatabase)
	defer cancel()
	res, err := sql.Exec(
		ctx,
		fmt.Sprintf("create database %s", name),
		executor.Options{},
	)
	require.NoError(t, moerr.AttachCause(ctx, err))
	res.Close()

	WaitDatabaseCreated(t, name, cn)
}

func WaitTableCreated(
	t *testing.T,
	db string,
	name string,
	cn embed.ServiceOperator,
) {
	for {
		if TableExists(t, db, name, cn) {
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
	for {
		if DBExists(t, name, cn) {
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

func ExecSQLWithReadResult(
	t *testing.T,
	db string,
	cn embed.ServiceOperator,
	reader func(int, string, executor.Result),
	sql ...string,
) timestamp.Timestamp {
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeoutCause(
		defines.AttachAccountId(context.Background(), 0),
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
		executor.Options{}.WithDatabase(db),
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
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeoutCause(
		defines.AttachAccountId(context.Background(), 0),
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
		executor.Options{}.WithDatabase(db).WithMinCommittedTS(min),
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
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseDBExists)
	defer cancel()

	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(
		ctx,
		"show databases",
		executor.Options{},
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
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseTableExists)
	defer cancel()

	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	res, err := exec.Exec(
		ctx,
		"show tables",
		executor.Options{}.WithDatabase(db),
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

type ctlResult struct {
	Result string `json:"result"`
}
