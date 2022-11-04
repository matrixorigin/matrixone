// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var (
	createDB  = `create database if not exists kv_test`
	useDB     = `use kv_test;`
	createSql = `create table if not exists txn_test_kv (kv_key varchar(20) primary key, kv_value varchar(10))`
)

// sqlClient use sql client to connect to CN node and use a table to simulate rr test KV operations
type sqlClient struct {
	cn service.CNService
}

func newSQLClient(logger *zap.Logger, env service.Cluster) (Client, error) {
	cn, err := env.GetCNServiceIndexed(0)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/", cn.SQLAddress()))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createDB)
	if err != nil {
		return nil, multierr.Append(err, db.Close())
	}

	_, err = db.Exec(useDB)
	if err != nil {
		return nil, multierr.Append(err, db.Close())
	}

	_, err = db.Exec(createSql)
	if err != nil {
		return nil, multierr.Append(err, db.Close())
	}

	return &sqlClient{
		cn: cn,
	}, multierr.Append(err, db.Close())
}

func (c *sqlClient) NewTxn(options ...client.TxnOption) (Txn, error) {
	return newSQLTxn(c.cn)
}

type sqlTxn struct {
	db  *sql.DB
	txn *sql.Tx

	mu struct {
		sync.Mutex
		closed bool
	}
}

func newSQLTxn(cn service.CNService) (Txn, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/kv_test", cn.SQLAddress()))
	if err != nil {
		return nil, err
	}

	txn, err := db.Begin()
	if err != nil {
		return nil, multierr.Append(err, db.Close())
	}
	return &sqlTxn{
		db:  db,
		txn: txn,
	}, nil
}

func (kop *sqlTxn) Commit() error {
	kop.mu.Lock()
	defer kop.mu.Unlock()
	if kop.mu.closed {
		return moerr.NewTxnClosed()
	}

	kop.mu.closed = true
	err := kop.txn.Commit()
	if err != nil {
		return multierr.Append(err, kop.db.Close())
	}
	return kop.db.Close()
}

func (kop *sqlTxn) Rollback() error {
	kop.mu.Lock()
	defer kop.mu.Unlock()
	if kop.mu.closed {
		return nil
	}

	err := kop.txn.Rollback()
	if err != nil {
		return multierr.Append(err, kop.db.Close())
	}
	return kop.db.Close()
}

func (kop *sqlTxn) Read(key string) (string, error) {
	rows, err := kop.txn.Query(fmt.Sprintf("select kv_value from txn_test_kv where kv_key = '%s'", key))
	if err != nil {
		return "", err
	}

	if !rows.Next() {
		return "", rows.Close()
	}
	v := ""
	if err := rows.Scan(&v); err != nil {
		return "", multierr.Append(err, rows.Close())
	}
	return v, multierr.Append(err, rows.Close())
}

func (kop *sqlTxn) Write(key, value string) error {
	v, err := kop.Read(key)
	if err != nil {
		return err
	}

	if v == "" {
		return kop.insert(key, value)
	}
	return kop.update(key, value)
}

func (kop *sqlTxn) ExecSQL(sql string) (sql.Result, error) {
	return kop.txn.Exec(sql)
}

func (kop *sqlTxn) ExecSQLQuery(sql string) (*sql.Rows, error) {
	return kop.txn.Query(sql)
}

func (kop *sqlTxn) insert(key, value string) error {
	res, err := kop.txn.Exec(fmt.Sprintf("insert into txn_test_kv(kv_key, kv_value) values('%s', '%s')", key, value))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	if n != 1 {
		panic(n)
	}
	return err
}

func (kop *sqlTxn) update(key, value string) error {
	_, err := kop.txn.Exec(fmt.Sprintf("update txn_test_kv set kv_value = '%s' where kv_key = '%s'", value, key))
	return err
}
