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

package sqlWriter

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"go.uber.org/zap"
)

const PACKET_LARGE_ERROR = "packet for query is too large"

// MAX_CHUNK_SIZE is the maximum size of a chunk of records to be inserted in a single query.
// Default packet size limit for MySQL is 16MB, but we set it to 15MB to be safe.
const MAX_CHUNK_SIZE = 1024 * 1024 * 15

var _ SqlWriter = (*BaseSqlWriter)(nil)

// BaseSqlWriter SqlWriter is a writer that writes data to a SQL database.
type BaseSqlWriter struct {
	db      *sql.DB
	address string
	ctx     context.Context
	dbMux   sync.Mutex
}

type SqlWriter interface {
	table.RowWriter
	WriteRows(rows string, tbl *table.Table) (int, error)
}

func (sw *BaseSqlWriter) GetContent() string {
	return ""
}
func (sw *BaseSqlWriter) WriteRow(row *table.Row) error {
	return nil
}

func generateInsertStatement(records [][]string, tbl *table.Table) (string, int, error) {

	sb := strings.Builder{}
	sb.WriteString("INSERT INTO")
	sb.WriteString(" `" + tbl.Database + "`." + tbl.Table + " ")

	// write columns
	sb.WriteString("(")
	for i, col := range tbl.Columns {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("`" + col.Name + "`")
	}
	sb.WriteString(") ")

	// write values
	sb.WriteString("VALUES ")
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
				err := json.Unmarshal([]byte(field), &js)
				if err != nil {
					return "", 0, err
				}
				escapedJSON, _ := json.Marshal(js)
				sb.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(strings.ReplaceAll(string(escapedJSON), "\\", "\\\\"), "'", "\\'")))
			} else {
				// escape single quote abd backslash
				escapedStr := strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\'"), "'", "\\'")
				// truncate string if it's too long caused by escape for varchar
				if tbl.Columns[i].ColType == table.TVarchar && tbl.Columns[i].Scale < len(escapedStr) {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr[:tbl.Columns[i].Scale-1]))
				} else {
					sb.WriteString(fmt.Sprintf("'%s'", escapedStr))
				}
			}
		}
		if idx == len(records)-1 {
			sb.WriteString(");")
		} else {
			sb.WriteString("),")
		}
	}
	return sb.String(), len(records), nil
}

// chunkRecords splits records into chunks of size <= maxChunkSize
func chunkRecords(records [][]string, maxChunkSize int) [][][]string {
	var chunks [][][]string
	var chunk [][]string
	chunkSize := 0

	for _, record := range records {
		recordSize := 0
		for _, field := range record {
			recordSize += len(field)
		}

		// If adding the current record would exceed the maxChunkSize, create a new chunk
		if chunkSize+recordSize > maxChunkSize {
			chunks = append(chunks, chunk)
			chunk = nil
			chunkSize = 0
		}

		chunk = append(chunk, record)
		chunkSize += recordSize
	}

	// Append the last chunk if it's not empty
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}

	return chunks
}

func bulkInsert(db *sql.DB, records [][]string, tbl *table.Table, maxLen int) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	chunkSize := maxLen
	chunks := chunkRecords(records, chunkSize)

	totalInserted := 0
	var err error
	for _, chunk := range chunks {
		stmt, cnt, _ := generateInsertStatement(chunk, tbl)
		_, err = db.Exec(stmt)
		if err != nil {
			// _, err = db.Exec(stmt)
			logutil.Info("sqlWriter db exec failed", zap.String("size", strconv.Itoa(len(chunk))), zap.Error(err))
		}
		if err != nil {
			return totalInserted, err
		}
		totalInserted += cnt
	}

	return totalInserted, err
}

func (sw *BaseSqlWriter) WriteRows(rows string, tbl *table.Table) (int, error) {
	r := csv.NewReader(strings.NewReader(rows))
	records, err := r.ReadAll()
	if err != nil {
		return 0, err
	}
	db, dbErr := sw.initOrRefreshDBConn(false)
	if dbErr != nil {
		logutil.Error("sqlWriter db init failed", zap.String("address", sw.address), zap.Error(dbErr))
		return 0, dbErr
	}
	stmt, cnt, _ := generateInsertStatement(records, tbl)
	_, err = db.Exec(stmt)
	if err != nil {
		if strings.Contains(err.Error(), PACKET_LARGE_ERROR) {
			bulkCnt, bulkErr := bulkInsert(db, records, tbl, MAX_CHUNK_SIZE)
			if bulkErr != nil {
				// logutil.Error("sqlWriter db insert bulk insert failed", zap.String("address", sw.address), zap.String("records lens", strconv.Itoa(len(records))), zap.Error(bulkErr))
				return 0, err
			}
			return bulkCnt, bulkErr
		} else {
			// logutil.Error("sqlWriter db insert failed", zap.String("address", sw.address), zap.Error(err))
			return 0, err
			// if table not exist return, no need to retry
			// todo: create table if not exist
			//if strings.Contains(err.Error(), "no such table") {
			//	return 0, err
			//}
			// refresh connection if invalid connection
			//if strings.Contains(err.Error(), "invalid connection") {
			//	newDb, newDbErr := sw.initOrRefreshDBConn(true)
			//	if newDbErr != nil {
			//		logutil.Error("sqlWriter db init failed", zap.String("address", sw.address), zap.Error(newDbErr))
			//		return 0, newDbErr
			//	} else {
			//		db = newDb
			//	}
			//}
			// _, err = db.Exec(stmt)
		}
	}
	return cnt, nil
}

func (sw *BaseSqlWriter) FlushAndClose() (int, error) {
	sw.dbMux.Lock()
	defer sw.dbMux.Unlock()
	var err error
	if sw.db == nil {
		err = sw.db.Close()
	}
	sw.db = nil
	return 0, err
}

func (sw *BaseSqlWriter) initOrRefreshDBConn(forceNewConn bool) (*sql.DB, error) {
	sw.dbMux.Lock()
	defer sw.dbMux.Unlock()

	initFunc := func() error {
		dbUser, _ := GetSQLWriterDBUser()
		if dbUser == nil {
			sw.db = nil
			return errNotReady
		}

		addressFunc := GetSQLWriterDBAddressFunc()
		if addressFunc == nil {
			sw.db = nil
			return errNotReady
		}
		dbAddress, err := addressFunc(context.Background())
		if err != nil {
			return err
		}
		dsn :=
			fmt.Sprintf("%s:%s@tcp(%s)/?readTimeout=300s&writeTimeout=30m&timeout=3000s",
				dbUser.UserName,
				dbUser.Password,
				dbAddress)
		db, err := sql.Open("mysql", dsn)
		logutil.Info("sqlWriter db initialized", zap.String("address", dbAddress))
		if err != nil {
			logutil.Info("sqlWriter db initialized failed", zap.String("address", dbAddress), zap.Error(err))
			return err
		}
		if sw.db != nil {
			sw.db.Close()
		}
		sw.db = db
		sw.address = dbAddress
		return nil
	}

	if forceNewConn || sw.db == nil {
		if sw.db != nil {
			err := sw.db.Ping()
			if err == nil {
				return sw.db, nil
			}
		}
		logutil.Info("sqlWriter db init", zap.Bool("forceNewConn", forceNewConn))
		err := initFunc()
		if err != nil {
			return nil, err
		}
	}

	return sw.db, nil
}
