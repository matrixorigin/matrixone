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
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/multierr"
)

var (
	createSql = `create table if not exists txn_test_kv (
		key          varchar(10) primary key,
		value        varchar(10))`
)

// sqlClient use sql client to connect to CN node and use a table to simulate rr test KV operations
type sqlClient struct {
	env service.Cluster
}

func newSQLClient(env service.Cluster) (Client, error) {
	return &sqlClient{
		env: env,
	}, nil
}

func (c *sqlClient) NewTxn(options ...client.TxnOption) (Txn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	c.env.WaitCNStoreReportedIndexed(ctx, 0)
	store, err := c.env.GetCNStoreInfoIndexed(ctx, 0)
	if err != nil {
		return nil, err
	}
	return newSQLTxn(store)
}

type sqlTxn struct {
	db  *sql.DB
	txn *sql.Tx

	mu struct {
		sync.Mutex
		closed bool
	}
}

func newSQLTxn(store logpb.CNStoreInfo) (Txn, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/system", store.ServiceAddress))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createSql)
	if err != nil {
		return nil, multierr.Append(err, db.Close())
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
	rows, err := kop.txn.Query("select key, value from txn_test_kv where key = ?", key)
	if err != nil {
		return "", err
	}

	if !rows.Next() {
		return "", rows.Close()
	}
	v := ""
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

func (kop *sqlTxn) insert(key, value string) error {
	_, err := kop.txn.Exec("insert into txn_test_kv(key, value) values(?, ?)", key, value)
	return err
}

func (kop *sqlTxn) update(key, value string) error {
	_, err := kop.txn.Exec("update txn_test_kv set value = ? where key = ?", key, value)
	return err
}
