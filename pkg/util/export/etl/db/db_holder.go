// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db_holder

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")
)

// sqlWriterDBUser holds the db user for logger
var (
	sqlWriterDBUser atomic.Value
	dbAddressFunc   atomic.Value

	db            atomic.Value
	dbRefreshTime time.Time

	dbMux sync.Mutex

	DBConnErrCount atomic.Uint32
)

const MOLoggerUser = "mo_logger"
const MaxConnectionNumber = 1

const DBConnRetryThreshold = 8

const DBRefreshTime = time.Hour

type DBUser struct {
	UserName string
	Password string
}

func SetSQLWriterDBUser(userName string, password string) {
	user := &DBUser{
		UserName: userName,
		Password: password,
	}
	sqlWriterDBUser.Store(user)
}
func GetSQLWriterDBUser() (*DBUser, error) {
	dbUser := sqlWriterDBUser.Load()
	if dbUser == nil {
		return nil, errNotReady
	} else {
		return sqlWriterDBUser.Load().(*DBUser), nil

	}
}

func SetSQLWriterDBAddressFunc(f func(context.Context, bool) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context, bool) (string, error) {
	return dbAddressFunc.Load().(func(context.Context, bool) (string, error))
}

func SetDBConn(conn *sql.DB) {
	db.Store(conn)
	dbRefreshTime = time.Now().Add(DBRefreshTime)
}

func CloseDBConn() {
	dbVal := db.Load()
	if dbVal == nil {
		return
	}
	dbConn := dbVal.(*sql.DB)
	if dbConn != nil {
		dbConn.Close()
	}
}

func GetOrInitDBConn(forceNewConn bool, randomCN bool) (*sql.DB, error) {
	dbMux.Lock()
	defer dbMux.Unlock()
	initFunc := func() error {
		CloseDBConn()
		dbUser, _ := GetSQLWriterDBUser()
		if dbUser == nil {
			return errNotReady
		}

		// TODO: trigger with new selected-CN, converge all connections
		addressFunc := GetSQLWriterDBAddressFunc()
		if addressFunc == nil {
			return errNotReady
		}
		dbAddress, err := addressFunc(context.Background(), randomCN)
		if err != nil {
			return err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=10s&writeTimeout=15s&timeout=15s&maxAllowedPacket=0",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		newDBConn, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}

		//45s suggest by xzxiong
		newDBConn.SetConnMaxLifetime(45 * time.Second)
		newDBConn.SetMaxOpenConns(MaxConnectionNumber)
		newDBConn.SetMaxIdleConns(MaxConnectionNumber)
		SetDBConn(newDBConn)
		return nil
	}

	if forceNewConn || db.Load() == nil {
		err := initFunc()
		if err != nil {
			return nil, err
		}
	} else if time.Now().After(dbRefreshTime) {
		err := initFunc()
		if err != nil {
			return nil, err
		}
	}

	dbConn := db.Load().(*sql.DB)
	return dbConn, nil
}

func WriteRowRecords(records [][]string, tbl *table.Table, timeout time.Duration) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var err error

	var dbConn *sql.DB

	if DBConnErrCount.Load() > DBConnRetryThreshold {
		dbConn, err = GetOrInitDBConn(true, true)
		DBConnErrCount.Store(0)
	} else {
		dbConn, err = GetOrInitDBConn(false, false)
	}
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = bulkInsert(ctx, dbConn, records, tbl)
	if err != nil {
		DBConnErrCount.Add(1)
		return 0, err
	}

	return len(records), nil
}

const initedSize = 4 * mpool.MB

var bufPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, initedSize))
}}

func getBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func putBuffer(buf *bytes.Buffer) {
	if buf != nil {
		buf.Reset()
		bufPool.Put(buf)
	}
}

type CSVWriter struct {
	ctx       context.Context
	formatter *csv.Writer
	buf       *bytes.Buffer
}

func NewCSVWriter(ctx context.Context) *CSVWriter {
	buf := getBuffer()
	buf.Reset()
	writer := csv.NewWriter(buf)

	w := &CSVWriter{
		ctx:       ctx,
		buf:       buf,
		formatter: writer,
	}
	return w
}

func (w *CSVWriter) WriteStrings(record []string) error {
	if err := w.formatter.Write(record); err != nil {
		return err
	}
	return nil
}

func (w *CSVWriter) GetContent() string {
	w.formatter.Flush() // Ensure all data is written to buffer
	return w.buf.String()
}

func (w *CSVWriter) Release() {
	if w.buf != nil {
		w.buf.Reset()
		w.buf = nil
		w.formatter = nil
	}
	putBuffer(w.buf)
}

func bulkInsert(ctx context.Context, sqlDb *sql.DB, records [][]string, tbl *table.Table) error {
	if len(records) == 0 {
		return nil
	}

	csvWriter := NewCSVWriter(ctx)
	defer csvWriter.Release() // Ensures that the buffer is returned to the pool

	// Write each record of the chunk to the CSVWriter
	for _, record := range records {
		for i, col := range record {
			record[i] = strings.ReplaceAll(strings.ReplaceAll(col, "\\", "\\\\"), "'", "''")
		}
		if err := csvWriter.WriteStrings(record); err != nil {
			return err
		}
	}

	csvData := csvWriter.GetContent()

	loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s.%s", csvData, tbl.Database, tbl.Table)

	// Use the transaction to execute the SQL command

	_, execErr := sqlDb.Exec(loadSQL)

	return execErr

}

type DBConnProvider func(forceNewConn bool, randomCN bool) (*sql.DB, error)

func IsRecordExisted(ctx context.Context, record []string, tbl *table.Table, getDBConn DBConnProvider) (bool, error) {
	dbConn, err := getDBConn(false, false)
	if err != nil {
		return false, err
	}

	if tbl.Table == "statement_info" {
		const stmtIDIndex = 0           // Replace with actual index for statement ID if different
		const statusIndex = 15          // Replace with actual index for status
		const requestAtIndex = 12       // Replace with actual index for request_at
		if len(record) <= statusIndex { // Use the largest index you will access
			return false, nil
		}
		return isStatementExisted(ctx, dbConn, record[stmtIDIndex], record[statusIndex], record[requestAtIndex])
	}

	return false, nil
}

func isStatementExisted(ctx context.Context, db *sql.DB, stmtId string, status string, request_at string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM `system`.statement_info WHERE statement_id = ? AND status = ? AND request_at = ?)"
	err := db.QueryRowContext(ctx, query, stmtId, status, request_at).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

var gLabels map[string]string = nil

func SetLabelSelector(labels map[string]string) {
	labels["account"] = "sys"
	gLabels = labels
}

// GetLabelSelector
// Tips: more details in route.RouteForSuperTenant function. It mainly depends on S1.
// Tips: gLabels better contain {"account":"sys"}.
// - Because clusterservice.Selector using clusterservice.globbing do regex-match in route.RouteForSuperTenant
// - If you use labels{"account":"sys", "role":"ob"}, the Selector can match those pods, which have labels{"account":"*", "role":"ob"}
func GetLabelSelector() map[string]string {
	return gLabels
}
