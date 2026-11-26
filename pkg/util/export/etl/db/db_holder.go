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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

var (
	errNotReady = moerr.NewInvalidStateNoCtx("SQL writer's DB conn not ready")

	// ErrIncompatibleStatementInfoRecord identifies a statement_info CSV record
	// written with a schema that cannot be loaded into the current table.
	ErrIncompatibleStatementInfoRecord = moerr.NewInternalErrorNoCtx("incompatible statement_info CSV record")
)

// sqlWriterDBUser holds the db user for logger
var (
	sqlWriterDBUser atomic.Value
	dbAddressFunc   atomic.Value

	db                 atomic.Pointer[sql.DB]
	dbRefreshTime      time.Time
	metricDBConnFailed bool

	dbMux         sync.Mutex
	dbRebuildSlot = make(chan struct{}, 1)
	openDBConn    = sql.Open

	DBConnErrCount = NewReConnectionBackOff(time.Minute, DBConnRetryThreshold)
)

const MOLoggerUser = "mo_logger"
const MaxConnectionNumber = 1

const DBConnRetryThreshold = 8

const DBRefreshTime = time.Hour
const staleDBConnCloseDelay = 30 * time.Second

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
		return dbUser.(*DBUser), nil

	}
}

func SetSQLWriterDBAddressFunc(f func(context.Context, bool) (string, error)) {
	dbAddressFunc.Store(f)
}

func GetSQLWriterDBAddressFunc() func(context.Context, bool) (string, error) {
	if f := dbAddressFunc.Load(); f == nil {
		return nil
	} else {
		return f.(func(context.Context, bool) (string, error))
	}
}

func setDBConnLocked(conn *sql.DB) *sql.DB {
	old := db.Swap(conn)
	metricDBConnFailed = false
	if conn == nil {
		dbRefreshTime = time.Time{}
	} else {
		dbRefreshTime = time.Now().Add(DBRefreshTime)
	}
	if old == conn {
		return nil
	}
	return old
}

func SetDBConn(conn *sql.DB) {
	dbRebuildSlot <- struct{}{}
	defer func() { <-dbRebuildSlot }()

	dbMux.Lock()
	defer dbMux.Unlock()
	setDBConnLocked(conn)
}

func CloseDBConn() {
	dbRebuildSlot <- struct{}{}
	defer func() { <-dbRebuildSlot }()

	dbMux.Lock()
	dbConn := setDBConnLocked(nil)
	dbMux.Unlock()
	closeDBConn(dbConn)
}

func closeDBConn(dbConn *sql.DB) {
	if dbConn != nil {
		_ = dbConn.Close()
	}
}

func closeStaleDBConn(dbConn *sql.DB) {
	if dbConn == nil {
		return
	}
	// DB.Close prevents new operations. Delay it so goroutines that already loaded
	// the old pointer just before a swap do not fail immediately with a closed DB.
	time.AfterFunc(staleDBConnCloseDelay, func() {
		closeDBConn(dbConn)
	})
}

func getDBConnSnapshot() (*sql.DB, bool, bool) {
	dbMux.Lock()
	defer dbMux.Unlock()

	dbConn := db.Load()
	return dbConn, dbConn != nil && time.Now().After(dbRefreshTime), metricDBConnFailed
}

func buildDBConn(ctx context.Context, randomCN bool) (*sql.DB, error) {
	dbUser, _ := GetSQLWriterDBUser()
	if dbUser == nil {
		return nil, errNotReady
	}

	// TODO: trigger with new selected-CN, converge all connections
	addressFunc := GetSQLWriterDBAddressFunc()
	if addressFunc == nil {
		return nil, errNotReady
	}
	dbAddress, err := addressFunc(ctx, randomCN)
	if err != nil {
		return nil, err
	}
	dsn :=
		fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=10s&writeTimeout=15s&timeout=15s&maxAllowedPacket=0",
			dbUser.UserName,
			dbUser.Password,
			dbAddress)
	newDBConn, err := openDBConn("mysql", dsn)
	if err != nil {
		return nil, err
	}

	//45s suggest by xzxiong
	newDBConn.SetConnMaxLifetime(45 * time.Second)
	newDBConn.SetMaxOpenConns(MaxConnectionNumber)
	newDBConn.SetMaxIdleConns(MaxConnectionNumber)
	if err = newDBConn.PingContext(ctx); err != nil {
		closeDBConn(newDBConn)
		return nil, err
	}
	return newDBConn, nil
}

// GetOrInitDBConn retains the connection policy used by the older exporters.
var GetOrInitDBConn = func(forceNewConn, randomCN bool) (*sql.DB, error) {
	return getOrInitDBConn(context.Background(), forceNewConn, randomCN, false)
}

func GetOrInitMetricDBConn(ctx context.Context) (*sql.DB, error) {
	// A cold start can also fail against a bad CN, before there is a
	// pool to mark. Rediscover a random eligible CN on the next attempt.
	return getOrInitDBConn(ctx, false, true, true)
}

// MarkMetricDBConnFailed requests a new CN on the next metric attempt, even
// when the next ETL run falls outside the shared error counter's window.
func MarkMetricDBConnFailed(conn *sql.DB) {
	dbMux.Lock()
	defer dbMux.Unlock()
	if conn != nil && db.Load() == conn {
		metricDBConnFailed = true
	}
}

func getOrInitDBConn(ctx context.Context, forceNewConn, randomCN, metric bool) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	current, refreshDue, failed := getDBConnSnapshot()
	force := forceNewConn || (metric && failed)
	switch {
	case current != nil && !force && !refreshDue:
		return current, nil
	case current != nil && !force:
		// A scheduled refresh must not stall callers that can use the pool.
		select {
		case dbRebuildSlot <- struct{}{}:
		default:
			return current, nil
		}
	default:
		select {
		case dbRebuildSlot <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	defer func() { <-dbRebuildSlot }()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A concurrent rebuild may have already replaced the failed or old pool.
	current, refreshDue, failed = getDBConnSnapshot()
	force = (forceNewConn && !DBConnErrCount.Check()) || (metric && failed)
	if current != nil && !force && !refreshDue {
		return current, nil
	}
	newDBConn, err := buildDBConn(ctx, randomCN || force)
	if err != nil {
		if current != nil && !force && ctx.Err() == nil {
			return current, nil
		}
		return nil, err
	}

	dbMux.Lock()
	oldDBConn := setDBConnLocked(newDBConn)
	dbMux.Unlock()
	closeStaleDBConn(oldDBConn)
	if force {
		DBConnErrCount.Reset()
	}
	return newDBConn, nil
}

func WriteRowRecords(records [][]string, tbl *table.Table, timeout time.Duration) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var err error

	var dbConn *sql.DB

	if !DBConnErrCount.Check() {
		dbConn, err = GetOrInitDBConn(true, true)
	} else {
		dbConn, err = GetOrInitDBConn(false, false)
	}
	if err != nil {
		v2.TraceMOLoggerErrorConnDBCounter.Inc()
		_ = DBConnErrCount.Count()
		return 0, err
	}
	if err = dbConn.Ping(); err != nil {
		v2.TraceMOLoggerErrorPingDBCounter.Inc()
		_ = DBConnErrCount.Count()
		return 0, err
	}

	ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseWriteRowRecords)
	defer cancel()

	err = bulkInsert(ctx, dbConn, records, tbl)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		_ = DBConnErrCount.Count()
		return 0, err
	}

	DBConnErrCount.Reset()
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

var _ table.RowWriter = (*CSVWriter)(nil)

type CSVWriter struct {
	formatter *csv.Writer
	buf       *bytes.Buffer
	release   func(buffer *bytes.Buffer)
}

func NewCSVWriter(_ context.Context) *CSVWriter {
	buf := getBuffer()
	buf.Reset()
	writer := csv.NewWriter(buf)

	w := &CSVWriter{
		buf:       buf,
		formatter: writer,
		release:   putBuffer,
	}
	return w
}

func NewCSVWriterWithBuffer(_ context.Context, buf *bytes.Buffer) *CSVWriter {
	writer := csv.NewWriter(buf)

	w := &CSVWriter{
		buf:       buf,
		formatter: writer,
		release:   nil,
	}
	return w
}
func (w *CSVWriter) WriteRow(row *table.Row) error { return w.WriteStrings(row.ToStrings()) }
func (w *CSVWriter) GetContentLength() int         { w.formatter.Flush(); return w.buf.Len() }

// FlushAndClose implements RowWriter, but NO Close action.
func (w *CSVWriter) FlushAndClose() (int, error) {
	w.formatter.Flush()
	return w.GetContentLength(), nil
}

func (w *CSVWriter) ResetBuffer(buf *bytes.Buffer) {
	w.buf = buf
	w.formatter = csv.NewWriter(buf)
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

func (w *CSVWriter) Flush() { w.formatter.Flush() }

func (w *CSVWriter) Release() {
	if w.buf != nil {
		if w.release != nil {
			// fix: need to release the !nil buffer
			defer w.release(w.buf)
		}
		w.buf = nil
		w.formatter = nil
	}
	w.release = nil
}

func bulkInsert(ctx context.Context, sqlDb *sql.DB, records [][]string, tbl *table.Table) error {
	if len(records) == 0 {
		return nil
	}

	diagnosticColumns, err := statementInfoDiagnosticColumns(tbl)
	if err != nil {
		return err
	}

	csvWriter := NewCSVWriter(ctx)
	defer csvWriter.Release() // Ensures that the buffer is returned to the pool

	// Write each record of the chunk to the CSVWriter
	for _, record := range records {
		if diagnosticColumns[0] >= 0 {
			if len(record) != len(tbl.Columns) {
				return moerr.NewInternalErrorNoCtx("invalid statement_info record width")
			}
			for _, idx := range diagnosticColumns[:2] {
				record[idx] = CapStatementInfoText(record[idx], StatementInfoTextLimit)
			}
			idx := diagnosticColumns[2]
			if len(record[idx]) > StatementInfoTextLimit {
				record[idx] = string(StatementInfoPlanSummary(len(record[idx])))
			}
		}
		for i, col := range record {
			record[i] = strings.ReplaceAll(strings.ReplaceAll(col, "\\", "\\\\"), "'", "''")
		}
		if err := csvWriter.WriteStrings(record); err != nil {
			return err
		}
	}

	csvData := csvWriter.GetContent()

	loadSQL := fmt.Sprintf("LOAD DATA INLINE FORMAT='csv', DATA='%s' INTO TABLE %s.%s FIELDS TERMINATED BY ','", csvData, tbl.Database, tbl.Table)
	v2.TraceMOLoggerExportSqlHistogram.Observe(float64(len(loadSQL)))

	// Use the transaction to execute the SQL command

	_, execErr := sqlDb.Exec(loadSQL)

	return execErr

}

type DBConnProvider func(forceNewConn bool, randomCN bool) (*sql.DB, error)

func IsRecordExisted(ctx context.Context, record []string, tbl *table.Table, getDBConn DBConnProvider) (bool, error) {
	if tbl.Table == "statement_info" {
		const stmtIDIndex = 0
		const statusIndex = 16    // Replace with actual index for status (shifted by 1 due to account_id)
		const requestAtIndex = 13 // Replace with actual index for request_at (shifted by 1 due to account_id)
		accountIdIndex := 4
		statusFieldIndex := statusIndex
		requestAtFieldIndex := requestAtIndex
		if len(tbl.Columns) > 0 {
			if len(record) != len(tbl.Columns) {
				return false, ErrIncompatibleStatementInfoRecord
			}
			for i, column := range tbl.Columns {
				switch column.Name {
				case "account_id":
					accountIdIndex = i
				case "status":
					statusFieldIndex = i
				case "request_at":
					requestAtFieldIndex = i
				}
			}
		}
		if accountIdIndex >= len(record) || statusFieldIndex >= len(record) || requestAtFieldIndex >= len(record) {
			return false, nil
		}
		accountIdStr := record[accountIdIndex]
		accountId, err := strconv.ParseUint(accountIdStr, 10, 32)
		if err != nil {
			return false, ErrIncompatibleStatementInfoRecord
		}
		dbConn, err := getDBConn(false, false)
		if err != nil {
			return false, err
		}
		return isStatementExisted(ctx, dbConn, record[stmtIDIndex], record[statusFieldIndex], record[requestAtFieldIndex], uint32(accountId))
	}

	return false, nil
}

func isStatementExisted(ctx context.Context, db *sql.DB, stmtId string, status string, request_at string, accountId uint32) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM `system`.statement_info WHERE statement_id = ? AND status = ? AND request_at = ? AND account_id = ?)"
	err := db.QueryRowContext(ctx, query, stmtId, status, request_at, accountId).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

var gLabels map[string]string = nil

func SetLabelSelector(labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	gLabels = make(map[string]string, len(labels)+1)
	gLabels["account"] = "sys"
	for k, v := range labels {
		gLabels[k] = v
	}
}

// GetLabelSelector
// Tips: more details in route.RouteForSuperTenant function. It mainly depends on S1.
// Tips: gLabels better contain {"account":"sys"}.
// - Because clusterservice.Selector using clusterservice.globbing do regex-match in route.RouteForSuperTenant
// - If you use labels{"account":"sys", "role":"ob"}, the Selector can match those pods, which have labels{"account":"*", "role":"ob"}
func GetLabelSelector() map[string]string {
	return gLabels
}

var _ table.BackOff = (*ReConnectionBackOff)(nil)

type ReConnectionBackOff struct {
	lock sync.Mutex
	// setting
	window    time.Duration
	threshold int
	// status
	last  time.Time
	count int
}

func NewReConnectionBackOff(window time.Duration, threshold int) *ReConnectionBackOff {
	return &ReConnectionBackOff{
		window:    window,
		threshold: threshold,
		last:      time.Now(),
		count:     0,
	}
}

// Count implement table.BackOff
// return true, means not in backoff cycle. You can run your code.
func (b *ReConnectionBackOff) Count() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	if time.Since(b.last) > b.window {
		b.count = 1
		b.last = time.Now()
		return true
	}

	b.count++
	b.last = time.Now()
	return b.count <= b.threshold
}

func (b *ReConnectionBackOff) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.count = 0
	b.last = time.Now()
}

// Check return same as Count, but without changed.
func (b *ReConnectionBackOff) Check() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.count <= b.threshold ||
		(time.Now().Before(b.last) || time.Since(b.last) > b.window)
}
