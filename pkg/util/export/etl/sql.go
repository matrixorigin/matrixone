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
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"go.uber.org/zap"
)

// MAX_CHUNK_SIZE is the maximum size of a chunk of records to be inserted in a single insert.
const MAX_CHUNK_SIZE = 1024 * 1024 * 4
const BUFFER_FLUSH_LIMIT = 512 // Adjust this value according to your requirements

const PACKET_TOO_LARGE = "packet for query is too large"

var _ SqlWriter = (*DefaultSqlWriter)(nil)

// DefaultSqlWriter SqlWriter is a writer that writes data to a SQL database.
type DefaultSqlWriter struct {
	ctx       context.Context
	semaphore chan struct{}
	csvWriter *CSVWriter
	tbl       *table.Table
	buffer    [][]string
	mux       sync.Mutex
}

func NewSqlWriter(ctx context.Context, tbl *table.Table, csv *CSVWriter) *DefaultSqlWriter {
	return &DefaultSqlWriter{
		ctx:       ctx,
		semaphore: make(chan struct{}, 3),
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

	if len(sw.buffer) >= BUFFER_FLUSH_LIMIT*3 {
		if _, err := sw.flushBuffer(); err != nil {
			return err
		}
	}
	return nil
}

func (sw *DefaultSqlWriter) flushBuffer() (int, error) {
	sw.mux.Lock()
	defer sw.mux.Unlock()
	if len(sw.buffer) == 0 {
		return 0, nil
	}
	cnt, err := sw.WriteRowRecords(sw.buffer, sw.tbl, false)
	if err != nil {
		if err := sw.dumpBufferToCSV(); err != nil {
			return 0, err
		}
		return cnt, nil
	}
	sw.buffer = sw.buffer[:0] // Clear the buffer
	return cnt, nil
}

func (sw *DefaultSqlWriter) dumpBufferToCSV() error {
	if len(sw.buffer) == 0 {
		return nil
	}
	// write sw.buffer to csvWriter
	for _, row := range sw.buffer {
		sw.csvWriter.WriteStrings(row)
	}
	sw.buffer = sw.buffer[:0] // Clear the buffer
	return nil
}

func (sw *DefaultSqlWriter) FlushAndClose() (int, error) {
	sw.flushBuffer()
	cnt, err := sw.csvWriter.FlushAndClose()
	if err != nil {
		return 0, err
	}
	sw.buffer = nil
	sw.tbl = nil
	return cnt, err
}

//
//func generateInsertStatement(records [][]string, tbl *table.Table) (string, int, error) {
//
//	sb := strings.Builder{}
//	defer sb.Reset()
//	sb.WriteString("INSERT INTO")
//	sb.WriteString(" `" + tbl.Database + "`." + tbl.Table + " ")
//	sb.WriteString("VALUES ")
//
//	// write values
//	for idx, row := range records {
//		if len(row) == 0 {
//			continue
//		}
//		sb.WriteString("(")
//		for i, field := range row {
//			if i != 0 {
//				sb.WriteString(",")
//			}
//			if tbl.Columns[i].ColType == table.TJson {
//				var js interface{}
//				err := json.Unmarshal([]byte(field), &js)
//				if err != nil {
//					return "", 0, err
//				}
//				escapedJSON, _ := json.Marshal(js)
//				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
//			} else {
//				// escape single quote abd backslash
//				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
//				// truncate string if it's too long caused by escape for varchar
//				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
//					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
//				} else {
//					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
//				}
//			}
//		}
//		if idx == len(records)-1 {
//			sb.WriteString(");")
//		} else {
//			sb.WriteString("),")
//		}
//	}
//	return sb.String(), len(records), nil
//}

func bulkInsert(db *sql.DB, records [][]string, tbl *table.Table, maxLen int) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	baseStr := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES ", tbl.Database, tbl.Table)

	sb := strings.Builder{}
	defer sb.Reset()

	// Start a new transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, err
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
				_ = json.Unmarshal([]byte(field), &js)
				escapedJSON, _ := json.Marshal(js)
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
			_, err := db.Exec(stmt)
			if err != nil {
				tx.Rollback() // Rollback the transaction on error
				return 0, err
			}
			sb.Reset()
		} else {
			sb.WriteString(",")
		}
	}

	err = tx.Commit() // Commit the transaction
	if err != nil {
		return 0, err
	}

	return len(records), nil
}

func (sw *DefaultSqlWriter) WriteRowRecords(records [][]string, tbl *table.Table, is_merge bool) (int, error) {

	var err error
	var cnt int
	dbConn, err := db.InitOrRefreshDBConn(false)
	if err != nil {
		logutil.Error("sqlWriter db init failed", zap.Error(err))
		return 0, err
	}

	cnt, err = bulkInsert(dbConn, records, tbl, MAX_CHUNK_SIZE)
	if err != nil {
		logutil.Error("sqlWriter bulk insert failed", zap.Error(err))
		return 0, err
	}
	return cnt, nil
}
