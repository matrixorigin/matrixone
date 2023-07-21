// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const SQLUserName = "proxy_user"

// SQLRouter routes connections with SQL query. It executes some queries to
// get information about tenant, connection ID or others and returns the
// property CN servers.
type SQLRouter interface {
	// GetCNServersByTenant gets all connection information by tenant name.
	// It connects to CN backend server to query data.
	GetCNServersByTenant(string) ([]*CNServer, error)
}

// SQLWorker is the interface which contains methods to init the SQL worker.
type SQLWorker interface {
	// SetSQLUser sets up the auth information to connect to backend servers.
	// The information comes from TaskTableUser which is in HAKeeper.
	SetSQLUser(username, password string)
	// SetAddressFn sets up the function which returns the backend server address.
	// The information comes from TaskTableUser which is in HAKeeper.
	SetAddressFn(f sqlAddressFn)
}

// sqlUser defines the structure which contains username and password
// that is used to connect to backend server.
type sqlUser struct {
	username string
	password string
}

// sqlAddressFn is the function that would return the backend server address.
type sqlAddressFn func(context.Context, bool) (string, error)

// sqlWorker is the type of SQL worker, which do some SQL query jobs.
type sqlWorker struct {
	mu struct {
		sync.RWMutex
		sqlUser       sqlUser
		addressGetter sqlAddressFn
	}
}

// newSQLWorker creates a new sqlWorker instance.
func newSQLWorker() *sqlWorker {
	return &sqlWorker{}
}

// readyRLocked checks if the worker is ready. It checks its username
// and address getter function, if they are both not empty, the worker
// is ready. It requires the read lock.
func (w *sqlWorker) readyRLocked() bool {
	return w.mu.sqlUser.username != "" && w.mu.addressGetter != nil
}

// SetSQLUser implements the SQLWorker interface.
func (w *sqlWorker) SetSQLUser(username, password string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.sqlUser.username = username
	w.mu.sqlUser.password = password
}

// SetAddressFn implements the SQLWorker interface.
func (w *sqlWorker) SetAddressFn(f sqlAddressFn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mu.addressGetter = f
}

// initDB uses user info and address getter function to open the connection to
// backend server and returns the sql.DB instance.
func (w *sqlWorker) initDB() (*sql.DB, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if !w.readyRLocked() {
		return nil, moerr.NewInternalErrorNoCtx("SQL worker is not ready yet")
	}
	dbAddress, err := w.mu.addressGetter(context.Background(), true)
	if err != nil {
		return nil, err
	}
	sock := "tcp"
	if strings.Contains(dbAddress, "sock") {
		sock = "unix"
	}
	dsn := fmt.Sprintf("%s:%s@%s(%s)/?timeout=5s",
		w.mu.sqlUser.username, w.mu.sqlUser.password, sock, dbAddress)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// GetCNServersByTenant implements the SQLWorker interface.
func (w *sqlWorker) GetCNServersByTenant(tenant string) ([]*CNServer, error) {
	db, err := w.initDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// The special user has the highest priority to fetch all tenants' information.
	// So we filter with tenant name.
	rows, err := db.Query(
		fmt.Sprintf("select node_id, conn_id, host from processlist() a where account='%s'", tenant))
	if err != nil {
		return nil, err
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	defer rows.Close()
	var cns []*CNServer
	for rows.Next() {
		var (
			nodeID string
			connID uint32
			host   string
		)
		err := rows.Scan(&nodeID, &connID, &host)
		if err != nil {
			return nil, err
		} else {
			cns = append(cns, &CNServer{
				backendConnID: connID,
				uuid:          nodeID,
				addr:          host,
			})
		}
	}
	return cns, nil
}
