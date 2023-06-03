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

package etl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"go.uber.org/zap"
)

// MAX_CHUNK_SIZE is the maximum size of a chunk of records to be inserted in a single insert.
const MAX_CHUNK_SIZE = 1024 * 1024 * 4

const MAX_INSERT_TIME = 3 * time.Second

var _ SqlWriter = (*DefaultSqlWriter)(nil)

// DefaultSqlWriter SqlWriter is a writer that writes data to a SQL database.
type DefaultSqlWriter struct {
	ctx       context.Context
	csvWriter *CSVWriter
	tbl       *table.Table
	buffer    [][]string
	mux       sync.Mutex
}

func NewSqlWriter(ctx context.Context, tbl *table.Table, csv *CSVWriter) *DefaultSqlWriter {
	return &DefaultSqlWriter{
		ctx:       ctx,
		csvWriter: csv,
		tbl:       tbl,
	}
}

type SqlWriter interface {
	table.RowWriter
	WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error)
}

func (sw *DefaultSqlWriter) GetContent() string {
	return ""
}

func (sw *DefaultSqlWriter) WriteStrings(record []string) error {
	return nil
}

func (sw *DefaultSqlWriter) WriteRow(row *table.Row) error {
	sw.buffer = append(sw.buffer, row.ToStrings())
	return nil
}

func (sw *DefaultSqlWriter) flushBuffer(force bool) (int, error) {
	now := time.Now()
	sw.mux.Lock()
	defer sw.mux.Unlock()

	var err error
	var cnt int

	cnt, err = sw.WriteRowRecords(sw.buffer, sw.tbl, false)

	if err != nil {
		sw.dumpBufferToCSV()
	}
	_, err = sw.csvWriter.FlushAndClose()
	logutil.Debug("sqlWriter flushBuffer finished", zap.Int("cnt", cnt), zap.Error(err), zap.Duration("time", time.Since(now)))
	return cnt, err
}

func (sw *DefaultSqlWriter) dumpBufferToCSV() error {
	if len(sw.buffer) == 0 {
		return nil
	}
	// write sw.buffer to csvWriter
	for _, row := range sw.buffer {
		sw.csvWriter.WriteStrings(row)
	}
	return nil
}

func (sw *DefaultSqlWriter) FlushAndClose() (int, error) {
	if sw.buffer != nil && len(sw.buffer) == 0 {
		return 0, nil
	}
	cnt, err := sw.flushBuffer(true)
	sw.buffer = nil
	sw.tbl = nil
	sw.csvWriter = nil
	return cnt, err
}

func bulkInsert(ctx context.Context, done chan error, sqlDb *sql.DB, records [][]string, tbl *table.Table, maxLen int) {
	if len(records) == 0 {
		done <- nil
		return
	}

	baseStr := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", tbl.Database, tbl.Table)

	sb := strings.Builder{}
	defer sb.Reset()

	tx, err := sqlDb.Begin()
	if err != nil {
		done <- err
		return
	}

	for idx, row := range records {
		if len(row) == 0 {
			continue
		}

		sb.WriteString("(")
		for i, field := range row {
			if i != 0 {
				sb.WriteString(",")
			}
			if tbl.Columns[i].ColType == table.TJson {
				var js interface{}
				escapedJSON, _ := json.Marshal(js)
				_ = json.Unmarshal([]byte(field), &js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		sb.WriteString(")")

		if sb.Len() >= maxLen || idx == len(records)-1 {
			stmt := baseStr + sb.String() + ";"
			_, err := tx.ExecContext(ctx, stmt)
			if err != nil {
				tx.Rollback()
				sb.Reset()
				done <- err
				return
			}
			select {
			case <-ctx.Done():
				// If context deadline is exceeded, rollback the transaction
				tx.Rollback()
				done <- errors.New("execution cancelled: context deadline exceeded")
				return
			default:
			}
			sb.Reset()
		} else {
			sb.WriteString(",")
		}
	}
	if err := tx.Commit(); err != nil {
		done <- err
		return
	}
	done <- nil
}

func (sw *DefaultSqlWriter) WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var err error
	dbConn, err := db_holder.InitOrRefreshDBConn(false)
	if err != nil {
		logutil.Error("sqlWriter db init failed", zap.Error(err))
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), MAX_INSERT_TIME)
	defer cancel()

	done := make(chan error)
	go bulkInsert(ctx, done, dbConn, records, tbl, MAX_CHUNK_SIZE)

	select {
	case err := <-done:
		if err != nil {
			return 0, err
		} else {
			logutil.Debug("sqlWriter WriteRowRecords finished", zap.Int("cnt", len(records)))
			return len(records), nil
		}
	case <-ctx.Done():
		logutil.Warn("sqlWriter WriteRowRecords cancelled", zap.Error(ctx.Err()))
		return 0, ctx.Err()
	}
}
