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
	"fmt"
	"sync"
	"time"

	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"

	_ "github.com/go-sql-driver/mysql"
)

const MAX_INSERT_TIME = 3 * time.Second

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
	sw.mux.Lock()
	defer sw.mux.Unlock()

	var err error
	var cnt int

	cnt, err = db_holder.WriteRowRecords(sw.buffer, sw.tbl, MAX_INSERT_TIME)

	if err != nil {
		if sw.tbl.Table == "rawlog" {
			logEntry := []string{
				"log_error",                              // raw_item
				"",                                       // node_uuid (this should probably be dynamically generated)
				"ALL",                                    // node_type
				"",                                       // span_id (should be dynamically set if needed)
				"",                                       // trace_id (should be dynamically generated)
				"mo_logger_sql",                          // logger_name
				time.Now().Format("2006-01-02 15:04:05"), // timestamp
				"error",                                  // level
				"etl/sql.go",                             // caller
				fmt.Sprintf("error: %s", err.Error()),    // message
				"{}",                                     // extra
				"0",                                      // err_code
				err.Error(),                              // error
				"",                                       // stack
				"",                                       // span_name
				"0",                                      // parent_span_id
				time.Now().Format("2006-01-02 15:04:05"), // start_time
				time.Now().Format("2006-01-02 15:04:05"), // end_time
				"0",                                      // duration
				"{}",                                     // resource
				"internal",                               // span_kind
				"",                                       // statement_id (should be dynamically set if needed)
				"",                                       // session_id (should be dynamically set if needed)
			}
			// Append the new log entry to your records
			sw.buffer = append(sw.buffer, logEntry)
		}
		sw.dumpBufferToCSV()
	}
	_, err = sw.csvWriter.FlushAndClose()
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
