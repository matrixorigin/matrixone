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

	db atomic.Value

	dbMux sync.Mutex

	DBConnErrCount atomic.Uint32
)

const MOLoggerUser = "mo_logger"
const MaxConnectionNumber = 1

const DBConnRetryThreshold = 8

const MaxInsertLen = 200
const MiddleInsertLen = 10

var maxRowBufPool sync.Pool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

var middleRowBufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}
var oneRowBufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

type prepareSQLs struct {
	maxRowNum int
	maxRows   string

	middleRowNum int
	middleRows   string

	oneRow  string
	columns int
}

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
}

func InitOrRefreshDBConn(forceNewConn bool, randomCN bool) (*sql.DB, error) {
	initFunc := func() error {
		dbMux.Lock()
		defer dbMux.Unlock()
		dbUser, _ := GetSQLWriterDBUser()
		if dbUser == nil {
			return errNotReady
		}

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
		if dbConn != nil {
			dbConn.Close()
		}
		dbConn, err = InitOrRefreshDBConn(true, true)
		DBConnErrCount.Store(0)
	} else {
		dbConn, err = InitOrRefreshDBConn(false, false)
	}
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = bulkInsert(ctx, dbConn, records, tbl, MaxInsertLen, MiddleInsertLen)
	if err != nil {
		fmt.Printf("gavinyue bulkInsert failed, err: %v\n", err)
		DBConnErrCount.Add(1)
		return 0, err
	}

	return len(records), nil
}

func getPrepareSQL(tbl *table.Table, columns int, batchLen int, middleBatchLen int) *prepareSQLs {
	columnNames := make([]string, len(tbl.Columns))

	for i, column := range tbl.Columns {
		columnNames[i] = fmt.Sprintf("`%s`", column.Name)
	}

	columnStr := strings.Join(columnNames, ",")

	prefix := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES ", tbl.Database, tbl.Table, columnStr)

	oneRowBuf := oneRowBufPool.Get().(*bytes.Buffer)
	for i := 0; i < columns; i++ {
		if i == 0 {
			oneRowBuf.WriteByte('?')
		} else {
			oneRowBuf.WriteString(",?")
		}
	}
	oneRow := fmt.Sprintf("%s (%s)", prefix, oneRowBuf.String())

	maxRowBuf := maxRowBufPool.Get().(*bytes.Buffer)
	maxRowBuf.WriteString(prefix)
	for i := 0; i < batchLen; i++ {
		if i == 0 {
			maxRowBuf.WriteByte('(')
		} else {
			maxRowBuf.WriteString(",(")
		}
		maxRowBuf.Write(oneRowBuf.Bytes())
		maxRowBuf.WriteByte(')')
	}

	middleRowBuf := middleRowBufPool.Get().(*bytes.Buffer)
	middleRowBuf.WriteString(prefix)
	for i := 0; i < middleBatchLen; i++ {
		if i == 0 {
			middleRowBuf.WriteByte('(')
		} else {
			middleRowBuf.WriteString(",(")
		}
		middleRowBuf.Write(oneRowBuf.Bytes())
		middleRowBuf.WriteByte(')')
	}

	prepareSQLS := &prepareSQLs{
		maxRowNum: batchLen,
		maxRows:   maxRowBuf.String(),

		middleRowNum: middleBatchLen,
		middleRows:   middleRowBuf.String(),

		oneRow:  oneRow,
		columns: columns,
	}
	oneRowBuf.Reset()
	maxRowBuf.Reset()
	middleRowBuf.Reset()
	oneRowBufPool.Put(oneRowBuf)
	maxRowBufPool.Put(maxRowBuf)
	middleRowBufPool.Put(middleRowBuf)
	return prepareSQLS
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
	writer.UseCRLF = true // Use \r\n as the line terminator

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

func bulkInsert(ctx context.Context, sqlDb *sql.DB, records [][]string, tbl *table.Table, batchLen int, middleBatchLen int) error {
	if len(records) == 0 {
		return nil
	}

	csvWriter := NewCSVWriter(ctx)
	defer csvWriter.Release() // Ensures that the buffer is returned to the pool

	// Write each record to the CSVWriter
	for _, record := range records {
		if err := csvWriter.WriteStrings(record); err != nil {
			return err
		}
	}

	csvData := csvWriter.GetContent()
	escapedCSVData := strings.ReplaceAll(csvData, "'", "''")
	loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s.%s", escapedCSVData, tbl.Database, tbl.Table)

	// Begin a new transaction
	tx, err := sqlDb.Begin()
	if err != nil {
		return err
	}

	// Use the transaction to execute the SQL command
	_, execErr := tx.Exec(loadSQL)

	// If there's an error, rollback the transaction
	if execErr != nil {
		fmt.Printf("gavinyue1 bulkInsert failed, err: %v, sql: %v \n", execErr, loadSQL)
		tx.Rollback()
		return execErr
	}

	// If no errors, commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
